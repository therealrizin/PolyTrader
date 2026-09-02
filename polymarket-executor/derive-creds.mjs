// derive-creds.mjs — run once, then delete
import { ClobClient } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";

const pk = process.env.POLYMARKET_PRIVATE_KEY; // paste temporarily, don't commit
const account = privateKeyToAccount(pk);
const walletClient = createWalletClient({ account, transport: http("https://polygon-rpc.com") });

const client = new ClobClient({ host: "https://clob.polymarket.com", chain: 137, signer: walletClient });
const creds = await client.createOrDeriveApiKey();
console.log(creds); // { apiKey, secret, passphrase }