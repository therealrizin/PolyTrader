use std::env;
use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::{TimeDelta, Utc};
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Normal},
    clob::{
        types::{
            Amount, OrderType as PolyOrderType, Side as PolySide, SignatureType,
        },
        Client,
        Config,
    },
    types::{Decimal, U256},
    POLYGON,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

type AuthClient = Client<Authenticated<Normal>>;

#[derive(Clone)]
struct AppState {
    client: AuthClient,
    signer: Arc<PrivateKeySigner>,
    executor_token: String,
}

#[derive(Debug, Deserialize)]
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
struct OrderResponse {
    success: bool,
    order_id: Option<String>,
    status: Option<String>,
    making_amount: Option<String>,
    taking_amount: Option<String>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let host = env::var("POLYMARKET_CLOB_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());

    let private_key = env::var("POLYMARKET_PRIVATE_KEY")
        .expect("POLYMARKET_PRIVATE_KEY is not configured");

    let deposit_wallet = env::var("POLYMARKET_DEPOSIT_WALLET")
        .expect("POLYMARKET_DEPOSIT_WALLET is not configured");

    let executor_token = env::var("EXECUTOR_TOKEN")
        .expect("EXECUTOR_TOKEN is not configured");

    let port: u16 = env::var("EXECUTOR_PORT")
        .unwrap_or_else(|_| "8090".to_string())
        .parse()
        .expect("EXECUTOR_PORT must be a valid port");

    let signer = Arc::new(
        PrivateKeySigner::from_str(&private_key)?
            .with_chain_id(Some(POLYGON)),
    );

    info!("Signer address: {}", signer.address());
    info!("Polymarket CLOB host: {}", host);
    info!("Deposit wallet: {}", deposit_wallet);

    let client = Client::new(
        &host,
        Config::builder()
            .use_server_time(true)
            .build(),
    )?
    .authentication_builder(&*signer)
    .funder(deposit_wallet.parse()?)
    .signature_type(SignatureType::Poly1271)
    .authenticate()
    .await?;

    info!("Successfully authenticated with Polymarket");

    let state = AppState {
        client,
        signer,
        executor_token,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/order", post(place_order))
        .with_state(state);

    let address = format!("0.0.0.0:{port}");

    info!("Starting executor on {}", address);

    let listener = TcpListener::bind(&address).await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn place_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<OrderRequest>,
) -> (StatusCode, Json<OrderResponse>) {
    let provided_token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    let expected = format!("Bearer {}", state.executor_token);

    if provided_token != expected {
        return bad_request("Unauthorized");
    }

    info!(
        "Received order request: client_order_id={:?}, market_slug={:?}, token_id={}, side={}, price={}, size={}, amount_usdc={:?}, order_type={:?}",
        req.client_order_id,
        req.market_slug,
        req.token_id,
        req.side,
        req.price,
        req.size,
        req.amount_usdc,
        req.order_type
    );

    let token_id = match U256::from_str(&req.token_id) {
        Ok(token_id) => token_id,
        Err(_) => {
            return bad_request("Invalid token_id");
        }
    };

    let side = match req.side.to_uppercase().as_str() {
        "BUY" => PolySide::Buy,
        "SELL" => PolySide::Sell,
        _ => {
            return bad_request("side must be BUY or SELL");
        }
    };

    let order_type = match req
        .order_type
        .as_deref()
        .unwrap_or("FOK")
        .to_uppercase()
        .as_str()
    {
        "FOK" => PolyOrderType::FOK,
        "FAK" => PolyOrderType::FAK,
        "GTC" => PolyOrderType::GTC,
        "GTD" => PolyOrderType::GTD,
        _ => {
            return bad_request("Unsupported order_type");
        }
    };

    let is_gtd = matches!(order_type, PolyOrderType::GTD);

    let order = match order_type {
        PolyOrderType::FOK | PolyOrderType::FAK => {
            let amount_usdc = match req.amount_usdc.as_deref() {
                Some(value) => value,
                None => {
                    return bad_request(
                        "amount_usdc is required for FOK/FAK orders",
                    );
                }
            };

            let amount_decimal = match Decimal::try_from(amount_usdc) {
                Ok(value) => value,
                Err(e) => {
                    return bad_request(&format!(
                        "Invalid amount_usdc: {e}"
                    ));
                }
            };

            let amount = match Amount::usdc(amount_decimal) {
                Ok(amount) => amount,
                Err(e) => {
                    return bad_request(&format!(
                        "Invalid USDC amount: {e}"
                    ));
                }
            };

            state
                .client
                .market_order()
                .token_id(token_id)
                .amount(amount)
                .side(side)
                .order_type(order_type.clone())
                .build()
                .await
        }

        PolyOrderType::GTC | PolyOrderType::GTD => {
            let price_decimal = match Decimal::try_from(req.price.as_str()) {
                Ok(value) => value,
                Err(e) => {
                    return bad_request(&format!("Invalid price: {e}"));
                }
            };

            let size_decimal = match Decimal::try_from(req.size.as_str()) {
                Ok(value) => value,
                Err(e) => {
                    return bad_request(&format!("Invalid size: {e}"));
                }
            };

            let mut builder = state
                .client
                .limit_order()
                .token_id(token_id)
                .price(price_decimal)
                .size(size_decimal)
                .side(side)
                .order_type(order_type.clone());

            if is_gtd {
                builder =
                    builder.expiration(Utc::now() + TimeDelta::minutes(5));
            }

            builder.build().await
        }

        _ => {
            return bad_request("Unsupported order_type");
        }
    };

    let order = match order {
        Ok(order) => order,
        Err(e) => {
            error!("Failed to build order: {:?}", e);

            return internal_error(&format!(
                "Failed to build order: {e}"
            ));
        }
    };

    info!(
        "Signing order: token_id={}, side={:?}, order_type={:?}",
        req.token_id, side, order_type
    );

    let signed_order =
        match state.client.sign(&*state.signer, order).await {
            Ok(order) => order,
            Err(e) => {
                error!("Failed to sign order: {:?}", e);

                return internal_error(&format!(
                    "Failed to sign order: {e}"
                ));
            }
        };

    info!(
        "Posting order: client_order_id={:?}, market_slug={:?}",
        req.client_order_id, req.market_slug
    );

    let response = match state.client.post_order(signed_order).await {
        Ok(response) => response,
        Err(e) => {
            error!("Polymarket order failed: {:?}", e);

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
        "Polymarket order submitted successfully: order_id={}, status={:?}, success={}",
        response.order_id,
        response.status,
        response.success
    );

    (
        StatusCode::OK,
        Json(OrderResponse {
            success: response.success,
            order_id: Some(response.order_id),
            status: Some(response.status.to_string()),
            making_amount: Some(response.making_amount.to_string()),
            taking_amount: Some(response.taking_amount.to_string()),
            error: response.error_msg,
        }),
    )
}

fn bad_request(error: &str) -> (StatusCode, Json<OrderResponse>) {
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

fn internal_error(error: &str) -> (StatusCode, Json<OrderResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
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