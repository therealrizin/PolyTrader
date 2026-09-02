```rust
use std::env;
use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use polymarket_client_sdk_v2::{
    clob::{
        types::{
            Amount, OrderType as PolyOrderType, Side as PolySide,
        },
        Client, Config,
    },
    types::{Decimal, U256},
    POLYGON,
};
use rust_decimal::Decimal as RustDecimal;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    client: Client,
    signer: LocalSigner,
    executor_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRequest {
    client_order_id: Option<String>,
    market_slug: Option<String>,
    token_id: String,
    side: String,
    price: String,
    size: String,
    amount_usdc: Option<String>,
    order_type: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderResponse {
    success: bool,
    order_id: Option<String>,
    status: Option<String>,
    making_amount: Option<String>,
    taking_amount: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    success: bool,
    service: &'static str,
    address: String,
    chain_id: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();

    let port: u16 = env::var("EXECUTOR_PORT")
        .unwrap_or_else(|_| "8090".to_string())
        .parse()?;

    let host = env::var("POLYMARKET_CLOB_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());

    let private_key =
        env::var("POLYMARKET_PRIVATE_KEY")
            .expect("POLYMARKET_PRIVATE_KEY is not configured");

    let api_key =
        env::var("POLYMARKET_API_KEY")
            .expect("POLYMARKET_API_KEY is not configured");

    let api_secret =
        env::var("POLYMARKET_API_SECRET")
            .expect("POLYMARKET_API_SECRET is not configured");

    let api_passphrase =
        env::var("POLYMARKET_API_PASSPHRASE")
            .expect("POLYMARKET_API_PASSPHRASE is not configured");

    let executor_token =
        env::var("EXECUTOR_TOKEN")
            .expect("EXECUTOR_TOKEN is not configured");

    let deposit_wallet =
        env::var("POLYMARKET_DEPOSIT_WALLET")
            .expect("POLYMARKET_DEPOSIT_WALLET is not configured");

    let signer =
        LocalSigner::from_str(&private_key)?
            .with_chain_id(Some(POLYGON));

    info!("EOA signer: {}", signer.address());
    info!("Deposit wallet: {}", deposit_wallet);
    info!("CLOB: {}", host);

    let creds =
        polymarket_client_sdk_v2::clob::types::ApiCreds {
            api_key,
            api_secret,
            api_passphrase,
        };

    /*
     * IMPORTANT:
     *
     * The EOA is the account that signs.
     *
     * The DEPOSIT WALLET is the maker/funder.
     *
     * This is the V2 Poly1271 flow.
     */
    let client = Client::new(
        &host,
        Config::builder()
            .use_server_time(true)
            .build(),
    )?
    .authentication_builder(&signer)
    .funder(deposit_wallet.parse()?)
    .signature_type(
        polymarket_client_sdk_v2::clob::types::SignatureType::Poly1271
    )
    .credentials(creds)
    .authenticate()
    .await?;

    let state = AppState {
        client,
        signer,
        executor_token,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/order", post(order))
        .with_state(state);

    let bind_address = format!("0.0.0.0:{port}");

    info!("Polymarket executor listening on {}", bind_address);

    let listener = TcpListener::bind(&bind_address).await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn health(
    State(state): State<AppState>,
) -> Json<HealthResponse> {
    Json(HealthResponse {
        success: true,
        service: "polymarket-executor",
        address: state.signer.address().to_string(),
        chain_id: POLYGON,
    })
}

async fn order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> (StatusCode, Json<OrderResponse>) {
    let supplied_token =
        headers
            .get("X-Executor-Token")
            .and_then(|v| v.to_str().ok());

    if supplied_token != Some(state.executor_token.as_str()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(OrderResponse {
                success: false,
                order_id: None,
                status: None,
                making_amount: None,
                taking_amount: None,
                error: Some("Unauthorized".to_string()),
            }),
        );
    }

    if req.token_id.is_empty() {
        return bad_request("tokenId is required");
    }

    if req.side != "UP" && req.side != "DOWN" {
        return bad_request("side must be UP or DOWN");
    }

    let token_id =
        match U256::from_str(&req.token_id) {
            Ok(v) => v,
            Err(e) => {
                return bad_request(
                    &format!("Invalid tokenId: {e}")
                )
            }
        };

    let price =
        match RustDecimal::from_str(&req.price) {
            Ok(v) => v,
            Err(e) => {
                return bad_request(
                    &format!("Invalid price: {e}")
                )
            }
        };

    let size =
        match RustDecimal::from_str(&req.size) {
            Ok(v) => v,
            Err(e) => {
                return bad_request(
                    &format!("Invalid size: {e}")
                )
            }
        };

    /*
     * Both UP and DOWN are BUY.
     *
     * Java already selects the actual conditional token.
     */
    let side = PolySide::Buy;

    /*
     * Keep the Java executor contract:
     *
     * orderType currently arrives as FOK.
     */
    let order_type =
        match req
            .order_type
            .as_deref()
            .unwrap_or("FOK")
            .to_uppercase()
            .as_str()
        {
            "FOK" => PolyOrderType::Fok,
            "FAK" => PolyOrderType::Fak,
            "GTC" => PolyOrderType::Gtc,
            "GTD" => PolyOrderType::Gtd,
            other => {
                return bad_request(
                    &format!("Unsupported orderType: {other}")
                )
            }
        };

    info!(
        client_order_id = ?req.client_order_id,
        market_slug = ?req.market_slug,
        token_id = %req.token_id,
        side = %req.side,
        price = %req.price,
        size = %req.size,
        "Submitting Polymarket order"
    );

    /*
     * Build the order through the Rust V2 SDK.
     *
     * For FOK/FAK, `amount` represents USDC.
     * For GTC/GTD, size is the number of outcome tokens.
     */
    let result = match order_type {
        PolyOrderType::Fok | PolyOrderType::Fak => {
            let amount_usdc =
                match req.amount_usdc.as_deref() {
                    Some(value) => {
                        match RustDecimal::from_str(value) {
                            Ok(v) => v,
                            Err(e) => {
                                return bad_request(
                                    &format!(
                                        "Invalid amountUsdc: {e}"
                                    )
                                )
                            }
                        }
                    }

                    None => {
                        return bad_request(
                            "amountUsdc is required for FOK/FAK"
                        )
                    }
                };

            let amount =
                match Amount::usdc(
                    Decimal::try_from(amount_usdc)
                        .map_err(|e| anyhow::anyhow!("{e}"))
                        .unwrap_or(Decimal::ZERO),
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        return internal_error(
                            &format!(
                                "Failed to create USDC amount: {e}"
                            )
                        )
                    }
                };

            state
                .client
                .market_order()
                .token_id(token_id)
                .amount(amount)
                .side(side)
                .build()
                .await
        }

        PolyOrderType::Gtc | PolyOrderType::Gtd => {
            let mut builder =
                state
                    .client
                    .limit_order()
                    .token_id(token_id)
                    .price(
                        Decimal::try_from(price)
                            .map_err(|e| anyhow::anyhow!("{e}"))
                            .unwrap_or(Decimal::ZERO),
                    )
                    .size(
                        Decimal::try_from(size)
                            .map_err(|e| anyhow::anyhow!("{e}"))
                            .unwrap_or(Decimal::ZERO),
                    )
                    .side(side)
                    .order_type(order_type);

            if matches!(order_type, PolyOrderType::Gtd) {
                builder = builder
                    .expiration(
                        chrono::Utc::now()
                            + chrono::TimeDelta::minutes(5)
                    );
            }

            builder.build().await
        }
    };

    let order = match result {
        Ok(order) => order,

        Err(e) => {
            error!("Failed to build order: {}", e);

            return internal_error(
                &format!("Failed to build order: {e}")
            );
        }
    };

    let signed_order =
        match state.client.sign(&state.signer, order).await {
            Ok(order) => order,

            Err(e) => {
                error!("Failed to sign order: {}", e);

                return internal_error(
                    &format!("Failed to sign order: {e}")
                );
            }
        };

    let response =
        match state.client.post_order(signed_order).await {
            Ok(response) => response,

            Err(e) => {
                error!("Polymarket order failed: {}", e);

                return (
                    StatusCode::BAD_GATEWAY,
                    Json(OrderResponse {
                        success: false,
                        order_id: None,
                        status: None,
                        making_amount: None,
                        taking_amount: None,
                        error: Some(e.to_string()),
                    }),
                );
            }
        };

    info!(
        order_id = %response.order_id,
        success = response.success,
        "ORDER ACCEPTED"
    );

    (
        StatusCode::OK,
        Json(OrderResponse {
            success: response.success,
            order_id: Some(response.order_id.to_string()),
            status: Some(format!("{:?}", response.status)),
            making_amount: None,
            taking_amount: None,
            error: None,
        }),
    )
}

fn bad_request(
    error: &str,
) -> (StatusCode, Json<OrderResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(OrderResponse {
            success: false,
            order_id: None,
            status: None,
            making_amount: None,
            taking_amount: None,
            error: Some(error.to_string()),
        }),
    )
}

fn internal_error(
    error: &str,
) -> (StatusCode, Json<OrderResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(OrderResponse {
            success: false,
            order_id: None,
            status: None,
            making_amount: None,
            taking_amount: None,
            error: Some(error.to_string()),
        }),
    )
}
```

