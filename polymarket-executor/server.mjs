import "dotenv/config";

import express from "express";
import {
    ClobClient,
    OrderType,
    Side,
    Chain
} from "@polymarket/clob-client-v2";

import {
    createWalletClient,
    http
} from "viem";

import {
    privateKeyToAccount
} from "viem/accounts";


const PORT =
    Number(process.env.EXECUTOR_PORT || 8090);

const HOST =
    process.env.POLYMARKET_CLOB_URL ||
    "https://clob.polymarket.com";

const PRIVATE_KEY =
    process.env.POLYMARKET_PRIVATE_KEY;

const API_KEY =
    process.env.POLYMARKET_API_KEY;

const API_SECRET =
    process.env.POLYMARKET_API_SECRET;

const API_PASSPHRASE =
    process.env.POLYMARKET_API_PASSPHRASE;

const CHAIN_ID =
    Number(process.env.POLYMARKET_CHAIN_ID || 137);

const EXECUTOR_TOKEN =
    process.env.EXECUTOR_TOKEN;


if (!PRIVATE_KEY) {
    throw new Error(
        "POLYMARKET_PRIVATE_KEY is not configured"
    );
}

if (!API_KEY) {
    throw new Error(
        "POLYMARKET_API_KEY is not configured"
    );
}

if (!API_SECRET) {
    throw new Error(
        "POLYMARKET_API_SECRET is not configured"
    );
}

if (!API_PASSPHRASE) {
    throw new Error(
        "POLYMARKET_API_PASSPHRASE is not configured"
    );
}

if (!EXECUTOR_TOKEN) {
    throw new Error(
        "EXECUTOR_TOKEN is not configured"
    );
}


const account =
    privateKeyToAccount(PRIVATE_KEY);


const walletClient =
    createWalletClient({
        account,
        chain: {
            id: CHAIN_ID,
            name: "Polygon",
            nativeCurrency: {
                name: "POL",
                symbol: "POL",
                decimals: 18
            },
            rpcUrls: {
                default: {
                    http: [
                        process.env.POLYGON_RPC_URL ||
                        "https://polygon-rpc.com"
                    ]
                }
            }
        },
        transport: http(
            process.env.POLYGON_RPC_URL ||
            "https://polygon-rpc.com"
        )
    });


const credentials = {
    key: API_KEY,
    secret: API_SECRET,
    passphrase: API_PASSPHRASE
};


const client =
    new ClobClient({
        host: HOST,
        chain: CHAIN_ID,
        signer: walletClient,
        creds: credentials,
        throwOnError: true
    });


const app =
    express();

app.use(express.json());


function authenticate(req, res, next) {

    const supplied =
        req.header("X-Executor-Token");

    if (!supplied
        || supplied !== EXECUTOR_TOKEN) {

        return res
            .status(401)
            .json({
                success: false,
                error: "Unauthorized"
            });
    }

    next();
}


app.get("/health", (req, res) => {

    res.json({
        success: true,
        service: "polymarket-executor",
        address: account.address,
        chainId: CHAIN_ID
    });
});


app.post(
    "/order",
    authenticate,
    async (req, res) => {

        try {

            const {
                clientOrderId,
                marketSlug,
                tokenId,
                side,
                price,
                size,
                amountUsdc,
                orderType
            } = req.body;


            if (!tokenId) {
                return res
                    .status(400)
                    .json({
                        success: false,
                        error: "tokenId is required"
                    });
            }


            if (!price) {
                return res
                    .status(400)
                    .json({
                        success: false,
                        error: "price is required"
                    });
            }


            if (!size) {
                return res
                    .status(400)
                    .json({
                        success: false,
                        error: "size is required"
                    });
            }


            if (side !== "UP"
                && side !== "DOWN") {

                return res
                    .status(400)
                    .json({
                        success: false,
                        error: "side must be UP or DOWN"
                    });
            }


            const sdkSide =
                side === "UP"
                    ? Side.BUY
                    : Side.BUY;


            /*
             * We use a LIMIT/FOK order instead of an unrestricted
             * market order.
             *
             * Java calculates:
             *
             *     size = USDC amount / best ask
             *
             * and supplies that exact price.
             *
             * Therefore Polymarket cannot execute above that price.
             */
            const response =
                await client.createAndPostOrder(
                    {
                        tokenID: tokenId,

                        price: Number(price),

                        size: Number(size),

                        side: sdkSide
                    },
                    {
                        tickSize:
                            process.env.POLYMARKET_TICK_SIZE ||
                            "0.01",

                        negRisk: false
                    },
                    OrderType.FOK
                );


            console.log(
                JSON.stringify({
                    event: "ORDER_ACCEPTED",
                    clientOrderId,
                    marketSlug,
                    tokenId,
                    side,
                    price,
                    size,
                    amountUsdc,
                    response
                })
            );


            return res.json({
                success: true,

                orderId:
                    response?.orderID ??
                    response?.orderId ??
                    null,

                status:
                    response?.status ??
                    "UNKNOWN",

                makingAmount:
                    response?.makingAmount ??
                    null,

                takingAmount:
                    response?.takingAmount ??
                    null,

                error: null
            });


        } catch (error) {

            console.error(
                "Polymarket order failed:",
                error
            );


            return res
                .status(502)
                .json({
                    success: false,
                    orderId: null,
                    status: null,
                    error:
                        error?.message ??
                        String(error)
                });
        }
    }
);


app.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `Polymarket executor listening on :${PORT}`
        );

        console.log(
            `Signer: ${account.address}`
        );

        console.log(
            `CLOB: ${HOST}`
        );

        console.log(
            `Chain: ${CHAIN_ID}`
        );
    }
);