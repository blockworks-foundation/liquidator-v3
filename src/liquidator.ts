import * as os from 'os';
import * as fs from 'fs';
import {
  AssetType,
  getMultipleAccounts,
  MangoAccount,
  MangoGroup,
  PerpMarket,
  RootBank,
  zeroKey,
  ZERO_BN,
  AdvancedOrdersLayout,
  MangoAccountLayout,
  MangoCache,
  QUOTE_INDEX,
  Cluster,
  Config,
  I80F48,
  IDS,
  ONE_I80F48,
  MangoClient,
  sleep,
  ZERO_I80F48,
  MarketMode,
  makeLiquidateDelistingTokenInstruction,
  makeForceSettlePerpPositionInstruction,
  TokenAccountLayout,
  TokenAccount,
} from '@blockworks-foundation/mango-client';
import {
  Commitment,
  Connection,
  Keypair,
  PublicKey,
  Transaction,
} from '@solana/web3.js';
import { Market, OpenOrders } from '@project-serum/serum';
import BN from 'bn.js';
import { Orderbook } from '@project-serum/serum/lib/market';
import axios from 'axios';
import * as Env from 'dotenv';
import envExpand from 'dotenv-expand';
import { Client as RpcWebSocketClient } from 'rpc-websockets';
import { AsyncBlockingQueue } from './AsyncBlockingQueue';
import {
  ASSOCIATED_TOKEN_PROGRAM_ID,
  Token,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';

envExpand(Env.config());

const interval = parseInt(process.env.INTERVAL || '3500');
const refreshAccountsInterval = parseInt(
  process.env.INTERVAL_ACCOUNTS || '600000',
);
const refreshWebsocketInterval = parseInt(
  process.env.INTERVAL_WEBSOCKET || '300000',
);
const liquidatableFeedWebsocketAddress =
  process.env.LIQUIDATABLE_FEED_WEBSOCKET_ADDRESS;
const rebalanceInterval = parseInt(process.env.INTERVAL_REBALANCE || '10000');
const checkTriggers = process.env.CHECK_TRIGGERS
  ? process.env.CHECK_TRIGGERS === 'true'
  : true;
const checkDelisting = process.env.CHECK_DELISTING
  ? process.env.CHECK_DELISTING === 'true'
  : false;
const liabLimit = I80F48.fromNumber(
  Math.min(parseFloat(process.env.LIAB_LIMIT || '0.9'), 1),
);
let lastRebalance = Date.now();

const config = new Config(IDS);

const cluster = (process.env.CLUSTER || 'mainnet') as Cluster;
const groupName = process.env.GROUP || 'mainnet.1';
const groupIds =
  config.getGroup(cluster, groupName) ??
  (() => {
    throw new Error(`Group ${groupName} not found`);
  })();

// The Triton team recommends using the commitment level `confirmed` in order
// to avoid BlockhashNotFound errors. Adding a parameter.
const commitmentLevel = process.env.COMMITMENT_LEVEL || 'processed';

// Target values to keep in spot, ordered the same as in mango client's ids.json
// Example:
//
//         MNGO BTC ETH SOL USDT SRM RAY COPE FTT MSOL
// TARGETS=0    0   0   1   0    0   0   0    0   0
const TARGETS = process.env.TARGETS?.replace(/\s+/g, ' ')
  .trim()
  .split(' ')
  .map((s) => parseFloat(s)) ?? [0, 0, 0, 0, 0, 0, 0, 0, 0];

// Do not liquidate accounts that have less than this much in value
const minEquity = parseInt(process.env.MIN_EQUITY || '0');
if (minEquity > 0) {
  console.log(`Minimum equity required to liquidate: ${minEquity}`);
}

const mangoProgramId = groupIds.mangoProgramId;
const mangoGroupKey = groupIds.publicKey;

const payer = Keypair.fromSecretKey(
  Uint8Array.from(
    JSON.parse(
      process.env.PRIVATE_KEY ||
        fs.readFileSync(
          process.env.KEYPAIR || os.homedir() + '/.config/solana/id.json',
          'utf-8',
        ),
    ),
  ),
);

console.log(`Commitment level: ${commitmentLevel}`);

console.log(`Payer: ${payer.publicKey.toBase58()}`);
const rpcEndpoint = process.env.ENDPOINT_URL || config.cluster_urls[cluster];
const connection = new Connection(rpcEndpoint, commitmentLevel as Commitment);
const client = new MangoClient(connection, mangoProgramId, {
  timeout: parseInt(process.env.CONFIRMATION_TIMEOUT || '30000'),
});

let mangoSubscriptionId = -1;
let dexSubscriptionId = -1;

let mangoGroup: MangoGroup;
let cache: MangoCache;
let liqorMangoAccount: MangoAccount;
let spotMarkets: Market[];
let perpMarkets: PerpMarket[];
let rootBanks: (RootBank | undefined)[];
let mangoAccounts: MangoAccount[] = [];

async function main() {
  console.log(`Starting liquidator for ${groupName}...`);
  console.log(`RPC Endpoint: ${rpcEndpoint}`);

  mangoGroup = await client.getMangoGroup(mangoGroupKey);
  cache = await mangoGroup.loadCache(connection);

  try {
    if (process.env.LIQOR_PK) {
      liqorMangoAccount = await client.getMangoAccount(
        new PublicKey(process.env.LIQOR_PK),
        mangoGroup.dexProgramId,
      );
      if (!liqorMangoAccount.owner.equals(payer.publicKey)) {
        throw new Error('Account not owned by Keypair');
      }
    } else {
      const accounts = await client.getMangoAccountsForOwner(
        mangoGroup,
        payer.publicKey,
        true,
      );
      if (accounts.length) {
        accounts.sort((a, b) =>
          b
            .computeValue(mangoGroup, cache)
            .sub(a.computeValue(mangoGroup, cache))
            .toNumber(),
        );
        liqorMangoAccount = accounts[0];
      } else {
        throw new Error('No Mango Account found for this Keypair');
      }
    }
  } catch (err: any) {
    console.error(`Error loading liqor Mango Account: ${err}`);
    return;
  }
  console.log(`Liqor Public Key: ${liqorMangoAccount.publicKey.toBase58()}`);

  perpMarkets = await Promise.all(
    groupIds.perpMarkets.map((perpMarket) => {
      return mangoGroup.loadPerpMarket(
        connection,
        perpMarket.marketIndex,
        perpMarket.baseDecimals,
        perpMarket.quoteDecimals,
      );
    }),
  );
  spotMarkets = await Promise.all(
    groupIds.spotMarkets.map((spotMarket) => {
      return Market.load(
        connection,
        spotMarket.publicKey,
        undefined,
        groupIds.serumProgramId,
      );
    }),
  );
  rootBanks = await mangoGroup.loadRootBanks(connection);
  notify(`V3 Liquidator launched for group ${groupName}`);

  if (liquidatableFeedWebsocketAddress) {
    await liquidatableFromLiquidatableFeed();
  } else {
    await liquidatableFromSolanaRpc();
  }
}

// never returns
async function checkMangoGroup() {
  console.log('reloading group');

  mangoGroup = await client.getMangoGroup(mangoGroupKey);
  await mangoGroup.loadRootBanks(connection);
  while (true) {
    try {
      for (let i = 0; i < mangoGroup.tokens.length; i++) {
        const tokenInfo = mangoGroup.tokens[i];

        if (tokenInfo.spotMarketMode == MarketMode.ForceCloseOnly) {
          console.log('force closing spot market', i);
          const market = await Market.load(
            connection,
            mangoGroup.spotMarkets[i].spotMarket,
            undefined,
            mangoGroup.dexProgramId,
          );

          const [dustAccountPk] = await PublicKey.findProgramAddress(
            [
              mangoGroup.publicKey.toBytes(),
              Buffer.from('DustAccount', 'utf-8'),
            ],
            groupIds.mangoProgramId,
          );

          // Get liqor ATA
          const liqorLiabTokenAccountPk = await Token.getAssociatedTokenAddress(
            ASSOCIATED_TOKEN_PROGRAM_ID,
            TOKEN_PROGRAM_ID,
            tokenInfo.mint,
            liqorMangoAccount.owner,
          );
          let liqorLiabTokenAccountInfo = await connection.getAccountInfo(
            liqorLiabTokenAccountPk,
          );
          if (!liqorLiabTokenAccountInfo) {
            console.log('creating ata for liqor');
            const createAccountTx = new Transaction().add(
              Token.createAssociatedTokenAccountInstruction(
                ASSOCIATED_TOKEN_PROGRAM_ID,
                TOKEN_PROGRAM_ID,
                tokenInfo.mint,
                liqorLiabTokenAccountPk,
                liqorMangoAccount.owner,
                payer.publicKey,
              ),
            );
            await client.sendTransaction(createAccountTx, payer, []);
            liqorLiabTokenAccountInfo = (await connection.getAccountInfo(
              liqorLiabTokenAccountPk,
            ))!;
          }

          const liqorLiabTokenAccount = new TokenAccount(
            liqorLiabTokenAccountPk,
            TokenAccountLayout.decode(liqorLiabTokenAccountInfo.data),
          );

          for (let mangoAccount of mangoAccounts) {
            try {
              // can't liquidate dust account or the liqor itself
              if (
                mangoAccount.publicKey.equals(dustAccountPk) ||
                mangoAccount.publicKey.equals(liqorMangoAccount.publicKey)
              ) {
                continue;
              }

              if (
                mangoAccount.inMarginBasket[i] ||
                !mangoAccount.spotOpenOrders[i].equals(PublicKey.default) ||
                !mangoAccount.getNet(cache.rootBankCache[i], i).eq(ZERO_I80F48)
              ) {
                console.log(
                  'delisting for account',
                  mangoAccount.publicKey.toBase58(),
                );

                if (mangoAccount.beingLiquidated && perpMarkets[0]) {
                  console.log(
                    'resetBeingLiquidated',
                    mangoAccount.publicKey.toBase58(),
                  );
                  //reset beingLiquidated flag
                  await client.forceCancelAllPerpOrdersInMarket(
                    mangoGroup,
                    mangoAccount,
                    perpMarkets[0],
                    payer,
                    10,
                  );
                }

                // Cancel all open orders for this market
                if (mangoAccount.inMarginBasket[i]) {
                  console.log(
                    'cancelAllSpotOrders',
                    mangoAccount.publicKey.toBase58(),
                  );
                  await client.cancelAllSpotOrders(
                    mangoGroup,
                    mangoAccount,
                    market,
                    payer,
                    20,
                  );
                  mangoAccount = await mangoAccount.reload(
                    connection,
                    mangoGroup.dexProgramId,
                  );
                }

                if (!mangoAccount.spotOpenOrders[i].equals(zeroKey)) {
                  console.log(
                    'closeSpotOpenOrders',
                    mangoAccount.publicKey.toBase58(),
                  );
                  await client.closeSpotOpenOrders(
                    mangoGroup,
                    mangoAccount,
                    payer,
                    i,
                    false,
                  );
                  await mangoAccount.reload(
                    connection,
                    mangoGroup.dexProgramId,
                  );
                }

                const liqeeNet = mangoAccount.getNet(cache.rootBankCache[i], i);

                if (!liqeeNet.isZero()) {
                  const liabRootBank = rootBanks[i]!;
                  const liabNodeBank = rootBanks[i]!.nodeBankAccounts[0];
                  const liquidateTx = new Transaction();

                  const maxAmount = liqeeNet.gt(ZERO_I80F48)
                    ? liqorMangoAccount
                        .getNativeDeposit(rootBanks[QUOTE_INDEX]!, QUOTE_INDEX)
                        .div(mangoGroup.getPriceNative(i, cache))
                        .toNumber()
                    : liqorLiabTokenAccount.amount;

                  // Find or create liqee ATA
                  const liqeeLiabTokenAccountPk =
                    await Token.getAssociatedTokenAddress(
                      ASSOCIATED_TOKEN_PROGRAM_ID,
                      TOKEN_PROGRAM_ID,
                      tokenInfo.mint,
                      mangoAccount.owner,
                      true,
                    );
                  const liqeeLiabTokenAccountInfo =
                    await connection.getAccountInfo(liqeeLiabTokenAccountPk);
                  if (!liqeeLiabTokenAccountInfo) {
                    console.log('creating ata for liqee');
                    liquidateTx.add(
                      Token.createAssociatedTokenAccountInstruction(
                        ASSOCIATED_TOKEN_PROGRAM_ID,
                        TOKEN_PROGRAM_ID,
                        tokenInfo.mint,
                        liqeeLiabTokenAccountPk,
                        mangoAccount.owner,
                        payer.publicKey,
                      ),
                    );
                  }

                  const liquidateIx = makeLiquidateDelistingTokenInstruction(
                    groupIds.mangoProgramId,
                    mangoGroup.publicKey,
                    mangoGroup.mangoCache,
                    dustAccountPk,
                    mangoAccount.publicKey,
                    liqorMangoAccount.publicKey,
                    payer.publicKey,
                    rootBanks[QUOTE_INDEX]!.publicKey,
                    rootBanks[QUOTE_INDEX]!.nodeBanks[0],
                    liabRootBank.publicKey,
                    liabNodeBank.publicKey,
                    liabNodeBank.vault,
                    liqeeLiabTokenAccountPk,
                    liqorLiabTokenAccountPk,
                    mangoGroup.signerKey,
                    mangoAccount.spotOpenOrders.filter(
                      (_, i) => mangoAccount.inMarginBasket[i],
                    ),
                    liqorMangoAccount.spotOpenOrders.filter(
                      (_, i) => liqorMangoAccount.inMarginBasket[i],
                    ),
                    new BN(maxAmount),
                  );

                  liquidateTx.add(liquidateIx);
                  console.log(
                    'liquidateDelistingToken',
                    mangoAccount.publicKey.toBase58(),
                  );
                  await client.sendTransaction(liquidateTx, payer, []);
                }
              }
            } catch (err) {
              console.error(
                'Error liquidating delisting token',
                mangoAccount.publicKey.toBase58(),
                err,
              );
            }
          }
        }

        if (tokenInfo.spotMarketMode == MarketMode.SwappingSpotMarket) {
          console.log('preparing spot market for swapping', i);
          for (let mangoAccount of mangoAccounts) {
            try {
              if (mangoAccount.inMarginBasket[i]) {
                const market = await Market.load(
                  connection,
                  mangoGroup.spotMarkets[i].spotMarket,
                  undefined,
                  mangoGroup.dexProgramId,
                );

                console.log(
                  'cancelAllSpotOrders',
                  mangoAccount.publicKey.toBase58(),
                );
                await client.cancelAllSpotOrders(
                  mangoGroup,
                  mangoAccount,
                  market,
                  payer,
                  20,
                );

                console.log('settleFunds', mangoAccount.publicKey.toBase58());
                await client.settleFunds(
                  mangoGroup,
                  mangoAccount,
                  payer,
                  market,
                );

                await client.closeSpotOpenOrders(
                  mangoGroup,
                  mangoAccount,
                  payer,
                  i,
                );
              }
            } catch (err) {
              console.error(
                'Error closing openorders',
                mangoAccount.publicKey.toBase58(),
                err,
              );
            }
          }
        }

        if (tokenInfo.perpMarketMode == MarketMode.ForceCloseOnly) {
          console.log('force closing perp market', i);
          const perpMarket = perpMarkets.find((x) =>
            x.publicKey.equals(mangoGroup.perpMarkets[i].perpMarket),
          )!;
          const mngoVault = new TokenAccount(
            perpMarket.mngoVault,
            await connection.getAccountInfo(perpMarket.mngoVault),
          );
          const rawPrice = cache.priceCache[i].price;

          for (let mangoAccount of mangoAccounts) {
            try {
              const perpAccount = mangoAccount.perpAccounts[i];
              // Cancel all open orders for this market
              if (
                perpAccount.bidsQuantity.gt(ZERO_BN) ||
                perpAccount.asksQuantity.gt(ZERO_BN)
              ) {
                console.log(
                  'cancelAllPerpOrders',
                  mangoAccount.publicKey.toBase58(),
                );
                try {
                  await client.cancelAllPerpOrders(
                    mangoGroup,
                    [perpMarket],
                    mangoAccount,
                    payer,
                    false,
                  );
                } catch (err) {
                  console.log(
                    'Error cancelling perp orders',
                    mangoAccount.publicKey.toBase58(),
                    err,
                  );
                }
              }

              // Redeem all MNGO
              if (perpAccount.mngoAccrued.gt(ZERO_BN) && mngoVault.amount > 0) {
                console.log('redeemMngo', mangoAccount.publicKey.toBase58());
                const MNGO_INDEX = groupIds!.oracles.findIndex(
                  (t) => t.symbol === 'MNGO',
                );
                const mngoRootBank = rootBanks[MNGO_INDEX]!;
                const mngoNodeBank = rootBanks[MNGO_INDEX]!.nodeBankAccounts[0];
                await client.redeemMngo(
                  mangoGroup,
                  mangoAccount,
                  perpMarket,
                  payer,
                  mngoRootBank.publicKey,
                  mngoNodeBank.publicKey,
                  mngoNodeBank.vault,
                );
              }

              // Find an account with opposite base position and force settle against it
              let basePosition = mangoAccount.perpAccounts[i].basePosition;
              const quotePosition = mangoAccount.perpAccounts[
                i
              ].getQuotePosition(cache.perpMarketCache[i]);

              if (basePosition.isZero() && quotePosition.isZero()) {
                continue;
              }

              const sign = basePosition.gt(ZERO_BN) ? 1 : -1;

              // get all accounts with an opposite base position
              const oppositeSignAccounts = mangoAccounts.filter((x) =>
                sign < 0
                  ? x.perpAccounts[i].basePosition.gt(ZERO_BN)
                  : x.perpAccounts[i].basePosition.lt(ZERO_BN),
              );
              const settleTx = new Transaction();

              for (const account of oppositeSignAccounts) {
                if (account.publicKey.equals(mangoAccount.publicKey)) {
                  continue;
                }
                const otherBasePosition = account.perpAccounts[i].basePosition;
                // TODO test limit of 10, will it exceed max length?
                if (settleTx.instructions.length < 10) {
                  console.log(
                    'found account to settle against',
                    basePosition.toNumber(),
                    otherBasePosition.toNumber(),
                  );
                  settleTx.add(
                    makeForceSettlePerpPositionInstruction(
                      groupIds.mangoProgramId,
                      mangoGroup.publicKey,
                      mangoAccount.publicKey,
                      account.publicKey,
                      mangoGroup.mangoCache,
                      perpMarket.publicKey,
                    ),
                  );
                  basePosition = basePosition.add(otherBasePosition);
                  const postSign = basePosition.gt(ZERO_BN) ? 1 : -1;
                  if (postSign !== sign) {
                    break;
                  }
                }
              }

              if (settleTx.instructions.length > 0) {
                console.log(
                  'force settling for',
                  mangoAccount.publicKey.toBase58(),
                );
                await client.sendTransaction(settleTx, payer, []);
              } else {
                console.log('no accounts found to settle against');
              }

              await client.settlePnl(
                mangoGroup,
                cache,
                mangoAccount,
                perpMarket,
                rootBanks[QUOTE_INDEX]!,
                rawPrice,
                payer,
                mangoAccounts,
              );
            } catch (err) {
              console.error(
                'Error force closing perps',
                mangoAccount.publicKey,
                err,
              );
            }
          }
        }
      }
    } catch (err) {
      console.error('Error checking for delisting markets', err);
    }
    await sleep(60000);
  }
}

// never returns
async function liquidatableFromSolanaRpc() {
  await refreshAccounts(mangoGroup, mangoAccounts);
  watchAccounts(groupIds.mangoProgramId, mangoGroup, mangoAccounts);

  if (checkDelisting) {
    checkMangoGroup();
  }

  // eslint-disable-next-line
  while (true) {
    try {
      if (checkTriggers) {
        // load all the advancedOrders accounts
        const mangoAccountsWithAOs = mangoAccounts.filter(
          (ma) => ma.advancedOrdersKey && !ma.advancedOrdersKey.equals(zeroKey),
        );
        const allAOs = mangoAccountsWithAOs.map((ma) => ma.advancedOrdersKey);

        const advancedOrders = await getMultipleAccounts(connection, allAOs);
        [cache, liqorMangoAccount] = await Promise.all([
          mangoGroup.loadCache(connection),
          liqorMangoAccount.reload(connection, mangoGroup.dexProgramId),
        ]);

        mangoAccountsWithAOs.forEach((ma, i) => {
          const decoded = AdvancedOrdersLayout.decode(
            advancedOrders[i].accountInfo.data,
          );
          ma.advancedOrders = decoded.orders;
        });
      } else {
        [cache, liqorMangoAccount] = await Promise.all([
          mangoGroup.loadCache(connection),
          liqorMangoAccount.reload(connection, mangoGroup.dexProgramId),
        ]);
      }

      for (const mangoAccount of mangoAccounts) {
        const mangoAccountKeyString = mangoAccount.publicKey.toBase58();

        // Handle trigger orders for this mango account
        if (checkTriggers && mangoAccount.advancedOrders) {
          try {
            await processTriggerOrders(
              mangoGroup,
              cache,
              perpMarkets,
              mangoAccount,
            );
          } catch (err: any) {
            if (err.message.includes('MangoErrorCode::InvalidParam')) {
              console.error(
                'Failed to execute trigger order, order already executed',
              );
            } else if (
              err.message.includes('MangoErrorCode::TriggerConditionFalse')
            ) {
              console.error(
                'Failed to execute trigger order, trigger condition was false',
              );
            } else {
              console.error(
                `Failed to execute trigger order for ${mangoAccountKeyString}: ${err}`,
              );
            }
          }
        }

        // If not liquidatable continue to next mango account
        if (!mangoAccount.isLiquidatable(mangoGroup, cache)) {
          continue;
        }

        // Reload mango account to make sure still liquidatable
        await mangoAccount.reload(connection, mangoGroup.dexProgramId);

        const liquidated = await maybeLiquidateAccount(mangoAccount);
        if (liquidated) {
          await balanceAccount(
            mangoGroup,
            liqorMangoAccount,
            cache,
            spotMarkets,
            perpMarkets,
          );
        }
      }

      cache = await mangoGroup.loadCache(connection);
      await liqorMangoAccount.reload(connection, mangoGroup.dexProgramId);

      // Check need to rebalance again after checking accounts
      await balanceAccount(
        mangoGroup,
        liqorMangoAccount,
        cache,
        spotMarkets,
        perpMarkets,
      );
      await sleep(interval);
    } catch (err) {
      console.error('Error checking accounts:', err);
    }
  }
}

async function maybeLiquidateAccount(
  mangoAccount: MangoAccount,
): Promise<boolean> {
  const mangoAccountKeyString = mangoAccount.publicKey.toBase58();

  if (!mangoAccount.isLiquidatable(mangoGroup, cache)) {
    console.log(`Account ${mangoAccountKeyString} no longer liquidatable`);
    return false;
  }

  const equity = mangoAccount.computeValue(mangoGroup, cache).toNumber();
  if (equity < minEquity && minEquity > 0) {
    // console.log(`Account ${mangoAccountKeyString} only has ${equity}, PASS`);
    return false;
  }

  const health = mangoAccount.getHealthRatio(mangoGroup, cache, 'Maint');
  const accountInfoString = mangoAccount.toPrettyString(
    groupIds,
    mangoGroup,
    cache,
  );
  console.log(
    `Sick account ${mangoAccountKeyString} health ratio: ${health.toString()}\n${accountInfoString}`,
  );
  notify(`Sick account\n${accountInfoString}`);
  try {
    await liquidateAccount(
      mangoGroup,
      cache,
      spotMarkets,
      rootBanks,
      perpMarkets,
      mangoAccount,
      liqorMangoAccount,
    );

    console.log('Liquidated account', mangoAccountKeyString);
    notify(`Liquidated account ${mangoAccountKeyString}`);
  } catch (err: any) {
    console.error(`Failed to liquidate account ${mangoAccountKeyString}:`, err);
    notify(`Failed to liquidate account ${mangoAccountKeyString}: ${err}`);
  }

  return true;
}

async function newAccountOnLiquidatableFeed(account) {
  console.log(`Checking health of Account ${account}...`);
  try {
    const mangoAccountKey = new PublicKey(account);
    const mangoAccount = new MangoAccount(mangoAccountKey, null);

    [cache, liqorMangoAccount] = await Promise.all([
      mangoGroup.loadCache(connection),
      liqorMangoAccount.reload(connection, mangoGroup.dexProgramId),
      mangoAccount.reload(connection, mangoGroup.dexProgramId),
    ]);

    const liquidated = await maybeLiquidateAccount(mangoAccount);
    if (liquidated) {
      cache = await mangoGroup.loadCache(connection);
      await liqorMangoAccount.reload(connection, mangoGroup.dexProgramId);

      // Check need to rebalance again after checking accounts
      await balanceAccount(
        mangoGroup,
        liqorMangoAccount,
        cache,
        spotMarkets,
        perpMarkets,
      );
    }
  } catch (err) {
    console.error('Error liquidating account:', err);
  }
}

// never returns
async function liquidatableFromLiquidatableFeed() {
  let candidates = new AsyncBlockingQueue<string>();
  let candidatesSet = new Set<string>();
  const ws = new RpcWebSocketClient(liquidatableFeedWebsocketAddress, {
    max_reconnects: Infinity,
  });
  ws.on('open', (x) => console.log('opened liquidatable feed'));
  ws.on('error', (status) => console.log('error on liquidatable feed', status));
  ws.on('close', (err) => console.log('closed liquidatable feed', err));
  ws.on('candidate', (params) => {
    const account = params.account;
    if (!candidatesSet.has(account)) {
      candidatesSet.add(account);
      candidates.enqueue(account);
    }
  });

  while (true) {
    const account = await candidates.dequeue();
    candidatesSet.delete(account);
    await newAccountOnLiquidatableFeed(account);
  }
}

function watchAccounts(
  mangoProgramId: PublicKey,
  mangoGroup: MangoGroup,
  mangoAccounts: MangoAccount[],
) {
  try {
    console.log('Watching accounts...');
    const openOrdersAccountSpan = OpenOrders.getLayout(
      mangoGroup.dexProgramId,
    ).span;
    const openOrdersAccountOwnerOffset = OpenOrders.getLayout(
      mangoGroup.dexProgramId,
    ).offsetOf('owner');

    if (mangoSubscriptionId != -1) {
      connection.removeProgramAccountChangeListener(mangoSubscriptionId);
    }
    if (dexSubscriptionId != -1) {
      connection.removeProgramAccountChangeListener(dexSubscriptionId);
    }

    mangoSubscriptionId = connection.onProgramAccountChange(
      mangoProgramId,
      async ({ accountId, accountInfo }) => {
        try {
          const index = mangoAccounts.findIndex((account) =>
            account.publicKey.equals(accountId),
          );

          const mangoAccount = new MangoAccount(
            accountId,
            MangoAccountLayout.decode(accountInfo.data),
          );
          if (index == -1) {
            mangoAccounts.push(mangoAccount);
          } else {
            const spotOpenOrdersAccounts =
              mangoAccounts[index].spotOpenOrdersAccounts;
            mangoAccount.spotOpenOrdersAccounts = spotOpenOrdersAccounts;
            mangoAccounts[index] = mangoAccount;
            await mangoAccount.loadOpenOrders(
              connection,
              mangoGroup.dexProgramId,
            );
          }
        } catch (err) {
          console.error(`could not update mango account ${err}`);
        }
      },
      'processed',
      [
        { dataSize: MangoAccountLayout.span },
        {
          memcmp: {
            offset: MangoAccountLayout.offsetOf('mangoGroup'),
            bytes: mangoGroup.publicKey.toBase58(),
          },
        },
      ],
    );

    dexSubscriptionId = connection.onProgramAccountChange(
      mangoGroup.dexProgramId,
      ({ accountId, accountInfo }) => {
        const ownerIndex = mangoAccounts.findIndex((account) =>
          account.spotOpenOrders.some((key) => key.equals(accountId)),
        );

        if (ownerIndex > -1) {
          const openOrdersIndex = mangoAccounts[
            ownerIndex
          ].spotOpenOrders.findIndex((key) => key.equals(accountId));
          const openOrders = OpenOrders.fromAccountInfo(
            accountId,
            accountInfo,
            mangoGroup.dexProgramId,
          );
          mangoAccounts[ownerIndex].spotOpenOrdersAccounts[openOrdersIndex] =
            openOrders;
        } else {
          console.error('Could not match OpenOrdersAccount to MangoAccount');
        }
      },
      'processed',
      [
        { dataSize: openOrdersAccountSpan },
        {
          memcmp: {
            offset: openOrdersAccountOwnerOffset,
            bytes: mangoGroup.signerKey.toBase58(),
          },
        },
      ],
    );
  } catch (err) {
    console.error('Error watching accounts', err);
  } finally {
    setTimeout(
      watchAccounts,
      refreshWebsocketInterval,
      mangoProgramId,
      mangoGroup,
      mangoAccounts,
    );
  }
}

async function refreshAccounts(
  mangoGroup: MangoGroup,
  mangoAccounts: MangoAccount[],
) {
  try {
    console.log('Refreshing accounts...');
    console.time('getAllMangoAccounts');

    mangoAccounts.splice(
      0,
      mangoAccounts.length,
      ...(await client.getAllMangoAccounts(mangoGroup, undefined, true)),
    );
    shuffleArray(mangoAccounts);

    console.timeEnd('getAllMangoAccounts');
    console.log(`Fetched ${mangoAccounts.length} accounts`);
  } catch (err: any) {
    console.error(`Error reloading accounts: ${err}`);
  } finally {
    setTimeout(
      refreshAccounts,
      refreshAccountsInterval,
      mangoGroup,
      mangoAccounts,
    );
  }
}

/**
 * Process trigger orders for one mango account
 */
async function processTriggerOrders(
  mangoGroup: MangoGroup,
  cache: MangoCache,
  perpMarkets: PerpMarket[],
  mangoAccount: MangoAccount,
) {
  for (let i = 0; i < mangoAccount.advancedOrders.length; i++) {
    const order = mangoAccount.advancedOrders[i];
    if (!(order.perpTrigger && order.perpTrigger.isActive)) {
      continue;
    }

    const trigger = order.perpTrigger;
    const currentPrice = cache.priceCache[trigger.marketIndex].price;
    const configMarketIndex = groupIds.perpMarkets.findIndex(
      (pm) => pm.marketIndex === trigger.marketIndex,
    );

    // Remove advanced orders if the market is in force close
    if (
      mangoGroup.tokens[trigger.marketIndex].perpMarketMode ==
      MarketMode.ForceCloseOnly
    ) {
      console.log('removeAdvancedOrder', mangoAccount.publicKey.toBase58(), i);
      return client.removeAdvancedOrder(mangoGroup, mangoAccount, payer, i);
    }

    if (
      (trigger.triggerCondition == 'above' &&
        currentPrice.gt(trigger.triggerPrice)) ||
      (trigger.triggerCondition == 'below' &&
        currentPrice.lt(trigger.triggerPrice))
    ) {
      console.log(
        `Executing order for account ${mangoAccount.publicKey.toBase58()}`,
      );
      return client.executePerpTriggerOrder(
        mangoGroup,
        mangoAccount,
        cache,
        perpMarkets[configMarketIndex],
        payer,
        i,
      );
    }
  }
}

async function liquidateAccount(
  mangoGroup: MangoGroup,
  cache: MangoCache,
  spotMarkets: Market[],
  rootBanks: (RootBank | undefined)[],
  perpMarkets: PerpMarket[],
  liqee: MangoAccount,
  liqor: MangoAccount,
) {
  const hasPerpOpenOrders = liqee.perpAccounts.some(
    (pa) => pa.bidsQuantity.gt(ZERO_BN) || pa.asksQuantity.gt(ZERO_BN),
  );

  if (hasPerpOpenOrders) {
    console.log('forceCancelPerpOrders');
    await Promise.all(
      perpMarkets.map((perpMarket) => {
        return client.forceCancelAllPerpOrdersInMarket(
          mangoGroup,
          liqee,
          perpMarket,
          payer,
          10,
        );
      }),
    );
    await liqee.reload(connection, mangoGroup.dexProgramId);
    if (!liqee.isLiquidatable(mangoGroup, cache)) {
      throw new Error('Account no longer liquidatable');
    }
  }

  for (let r = 0; r < 5 && liqee.hasAnySpotOrders(); r++) {
    for (let i = 0; i < groupIds.spotMarkets.length; i++) {
      if (liqee.inMarginBasket[i]) {
        const spotMarketConfig = groupIds.spotMarkets[i];
        const spotMarket = spotMarkets[spotMarketConfig.marketIndex];
        const baseRootBank = rootBanks[spotMarketConfig.marketIndex];
        const quoteRootBank = rootBanks[QUOTE_INDEX];

        if (baseRootBank && quoteRootBank) {
          console.log('forceCancelOrders ', i);
          await client.forceCancelSpotOrders(
            mangoGroup,
            liqee,
            spotMarket,
            baseRootBank,
            quoteRootBank,
            payer,
            new BN(5),
          );
        }
      }
    }

    await liqee.reload(connection, mangoGroup.dexProgramId);
    if (!liqee.isLiquidatable(mangoGroup, cache)) {
      throw new Error('Account no longer liquidatable');
    }
  }

  const healthComponents = liqee.getHealthComponents(mangoGroup, cache);
  const maintHealths = liqee.getHealthsFromComponents(
    mangoGroup,
    cache,
    healthComponents.spot,
    healthComponents.perps,
    healthComponents.quote,
    'Maint',
  );

  let shouldLiquidateSpot = false;
  for (let i = 0; i < mangoGroup.tokens.length; i++) {
    if (liqee.getNet(cache.rootBankCache[i], i).isNeg()) {
      shouldLiquidateSpot = true;
      break;
    }
  }

  if (shouldLiquidateSpot) {
    await liquidateSpot(
      mangoGroup,
      cache,
      perpMarkets,
      rootBanks,
      liqee,
      liqor,
    );
    await liqee.reload(connection, mangoGroup.dexProgramId);
    if (!liqee.isLiquidatable(mangoGroup, cache)) {
      return;
    }
  }

  await liquidatePerps(mangoGroup, cache, perpMarkets, rootBanks, liqee, liqor);

  if (
    !shouldLiquidateSpot &&
    !maintHealths.perp.isNeg() &&
    liqee.beingLiquidated
  ) {
    // Send a ForceCancelPerp to reset the being_liquidated flag
    console.log('forceCancelAllPerpOrdersInMarket');
    await client.forceCancelAllPerpOrdersInMarket(
      mangoGroup,
      liqee,
      perpMarkets[0],
      payer,
      10,
    );
  }
}

async function liquidateSpot(
  mangoGroup: MangoGroup,
  cache: MangoCache,
  perpMarkets: PerpMarket[],
  rootBanks: (RootBank | undefined)[],
  liqee: MangoAccount,
  liqor: MangoAccount,
) {
  console.log('liquidateSpot');

  let minNet = ZERO_I80F48;
  let minNetIndex = -1;
  let maxNet = ZERO_I80F48;
  let maxNetIndex = -1;

  for (let i = 0; i < mangoGroup.tokens.length; i++) {
    const price = cache.priceCache[i] ? cache.priceCache[i].price : ONE_I80F48;
    const netDeposit = liqee
      .getNativeDeposit(cache.rootBankCache[i], i)
      .sub(liqee.getNativeBorrow(cache.rootBankCache[i], i))
      .mul(price);

    if (netDeposit.lt(minNet)) {
      minNet = netDeposit;
      minNetIndex = i;
    } else if (netDeposit.gt(maxNet)) {
      maxNet = netDeposit;
      maxNetIndex = i;
    }
  }
  if (minNetIndex == -1) {
    throw new Error('min net index neg 1');
  }

  if (minNetIndex == maxNetIndex) {
    maxNetIndex = QUOTE_INDEX;
  }

  const liabRootBank = rootBanks[minNetIndex];
  const assetRootBank = rootBanks[maxNetIndex];

  if (assetRootBank && liabRootBank) {
    const liqorInitHealth = liqor.getHealth(mangoGroup, cache, 'Init');
    const liabInitLiabWeight = mangoGroup.spotMarkets[minNetIndex]
      ? mangoGroup.spotMarkets[minNetIndex].initLiabWeight
      : ONE_I80F48;
    const assetInitAssetWeight = mangoGroup.spotMarkets[maxNetIndex]
      ? mangoGroup.spotMarkets[maxNetIndex].initAssetWeight
      : ONE_I80F48;

    const maxLiabTransfer = liqorInitHealth
      .div(
        mangoGroup
          .getPriceNative(minNetIndex, cache)
          .mul(liabInitLiabWeight.sub(assetInitAssetWeight).abs()),
      )
      .mul(liabLimit);

    if (liqee.isBankrupt) {
      console.log('Bankrupt account', liqee.publicKey.toBase58());
      const quoteRootBank = rootBanks[QUOTE_INDEX];
      if (quoteRootBank) {
        await client.resolveTokenBankruptcy(
          mangoGroup,
          liqee,
          liqor,
          quoteRootBank,
          liabRootBank,
          payer,
          maxLiabTransfer,
        );
        await liqee.reload(connection, mangoGroup.dexProgramId);
      }
    } else {
      if (maxNet.lt(ZERO_I80F48) || maxNetIndex == -1) {
        const highestHealthMarket = perpMarkets
          .map((perpMarket, i) => {
            const marketIndex = mangoGroup.getPerpMarketIndex(
              perpMarket.publicKey,
            );
            const perpMarketInfo = mangoGroup.perpMarkets[marketIndex];
            const perpAccount = liqee.perpAccounts[marketIndex];
            const perpMarketCache = cache.perpMarketCache[marketIndex];
            const price = mangoGroup.getPriceNative(marketIndex, cache);
            const perpHealth = perpAccount.getHealth(
              perpMarketInfo,
              price,
              perpMarketInfo.maintAssetWeight,
              perpMarketInfo.maintLiabWeight,
              perpMarketCache.longFunding,
              perpMarketCache.shortFunding,
            );
            return { perpHealth: perpHealth, marketIndex: marketIndex, i };
          })
          .sort((a, b) => {
            return b.perpHealth.sub(a.perpHealth).toNumber();
          })[0];

        let maxLiabTransfer = liqorInitHealth.mul(liabLimit);
        if (maxNetIndex !== QUOTE_INDEX) {
          maxLiabTransfer = liqorInitHealth
            .div(ONE_I80F48.sub(assetInitAssetWeight))
            .mul(liabLimit);
        }

        console.log('liquidateTokenAndPerp', highestHealthMarket.marketIndex);
        await client.liquidateTokenAndPerp(
          mangoGroup,
          liqee,
          liqor,
          liabRootBank,
          payer,
          AssetType.Perp,
          highestHealthMarket.marketIndex,
          AssetType.Token,
          minNetIndex,
          maxLiabTransfer,
        );
      } else {
        console.log('liquidateTokenAndToken', maxNetIndex, minNetIndex);
        await client.liquidateTokenAndToken(
          mangoGroup,
          liqee,
          liqor,
          assetRootBank,
          liabRootBank,
          payer,
          maxLiabTransfer,
        );
      }

      await liqee.reload(connection, mangoGroup.dexProgramId);
      if (liqee.isBankrupt) {
        console.log('Bankrupt account', liqee.publicKey.toBase58());
        const quoteRootBank = rootBanks[QUOTE_INDEX];
        if (quoteRootBank) {
          await client.resolveTokenBankruptcy(
            mangoGroup,
            liqee,
            liqor,
            quoteRootBank,
            liabRootBank,
            payer,
            maxLiabTransfer,
          );
          await liqee.reload(connection, mangoGroup.dexProgramId);
        }
      }
    }
  }
}

async function liquidatePerps(
  mangoGroup: MangoGroup,
  cache: MangoCache,
  perpMarkets: PerpMarket[],
  rootBanks: (RootBank | undefined)[],
  liqee: MangoAccount,
  liqor: MangoAccount,
) {
  console.log('liquidatePerps');
  const lowestHealthMarket = perpMarkets
    .map((perpMarket, i) => {
      const marketIndex = mangoGroup.getPerpMarketIndex(perpMarket.publicKey);
      const perpMarketInfo = mangoGroup.perpMarkets[marketIndex];
      const perpAccount = liqee.perpAccounts[marketIndex];
      const perpMarketCache = cache.perpMarketCache[marketIndex];
      const price = mangoGroup.getPriceNative(marketIndex, cache);
      const perpHealth = perpAccount.getHealth(
        perpMarketInfo,
        price,
        perpMarketInfo.maintAssetWeight,
        perpMarketInfo.maintLiabWeight,
        perpMarketCache.longFunding,
        perpMarketCache.shortFunding,
      );
      return { perpHealth: perpHealth, marketIndex: marketIndex, i };
    })
    .sort((a, b) => {
      return a.perpHealth.sub(b.perpHealth).toNumber();
    })[0];

  if (!lowestHealthMarket) {
    throw new Error('Couldnt find a perp market to liquidate');
  }

  const marketIndex = lowestHealthMarket.marketIndex;
  const perpAccount = liqee.perpAccounts[marketIndex];
  const perpMarket = perpMarkets[lowestHealthMarket.i];

  if (!perpMarket) {
    throw new Error(`Perp market not found for ${marketIndex}`);
  }

  const liqorInitHealth = liqor.getHealth(mangoGroup, cache, 'Init');
  let maxLiabTransfer = liqorInitHealth.mul(liabLimit);
  const quoteRootBank = rootBanks[QUOTE_INDEX];
  if (liqee.isBankrupt) {
    if (quoteRootBank) {
      // don't do anything it if quote position is zero
      console.log('resolvePerpBankruptcy', maxLiabTransfer.toString());
      await client.resolvePerpBankruptcy(
        mangoGroup,
        liqee,
        liqor,
        perpMarket,
        quoteRootBank,
        payer,
        marketIndex,
        maxLiabTransfer,
      );
      await liqee.reload(connection, mangoGroup.dexProgramId);
    }
  } else {
    let maxNet = ZERO_I80F48;
    let maxNetIndex = mangoGroup.tokens.length - 1;

    // We need to settle the positive PnLs to make sure that the account has funds
    // to actually compensate for a liquidation. Otherwise we may end up with a
    // token liquidation of 0.
    //
    // https://discord.com/channels/791995070613159966/826034521261604874/934629112734167060

    if (quoteRootBank) {
      await client.settlePosPnl(
        mangoGroup,
        cache,
        liqee,
        perpMarkets,
        quoteRootBank,
        payer,
        mangoAccounts,
      );
    }

    for (let i = 0; i < mangoGroup.tokens.length; i++) {
      const price = cache.priceCache[i]
        ? cache.priceCache[i].price
        : ONE_I80F48;

      const netDeposit = liqee.getNet(cache.rootBankCache[i], i).mul(price);
      if (netDeposit.gt(maxNet)) {
        maxNet = netDeposit;
        maxNetIndex = i;
      }
    }

    const assetRootBank = rootBanks[maxNetIndex];
    const liqorInitHealth = liqor.getHealth(mangoGroup, cache, 'Init');
    if (perpAccount.basePosition.isZero() && maxNet.gt(ZERO_I80F48)) {
      if (assetRootBank) {
        // we know that since sum of perp healths is negative, lowest perp market must be negative
        console.log('liquidateTokenAndPerp', marketIndex);
        if (maxNetIndex !== QUOTE_INDEX) {
          maxLiabTransfer = liqorInitHealth
            .div(
              ONE_I80F48.sub(
                mangoGroup.spotMarkets[maxNetIndex].initAssetWeight,
              ),
            )
            .mul(liabLimit);
        }
        await client.liquidateTokenAndPerp(
          mangoGroup,
          liqee,
          liqor,
          assetRootBank,
          payer,
          AssetType.Token,
          maxNetIndex,
          AssetType.Perp,
          marketIndex,
          maxLiabTransfer,
        );
      }
    } else {
      console.log('liquidatePerpMarket', marketIndex);

      // technically can be higher because of liquidation fee, but
      // let's just give ourselves extra room
      const perpMarketInfo = mangoGroup.perpMarkets[marketIndex];
      const initAssetWeight = perpMarketInfo.initAssetWeight;
      const initLiabWeight = perpMarketInfo.initLiabWeight;
      let baseTransferRequest;
      if (perpAccount.basePosition.gt(ZERO_BN)) {
        // TODO adjust for existing base position on liqor
        baseTransferRequest = new BN(
          liqorInitHealth
            .div(ONE_I80F48.sub(initAssetWeight))
            .div(mangoGroup.getPriceNative(marketIndex, cache))
            .div(I80F48.fromI64(perpMarketInfo.baseLotSize))
            .floor()
            .mul(liabLimit)
            .toNumber(),
        );
      } else {
        baseTransferRequest = new BN(
          liqorInitHealth
            .div(initLiabWeight.sub(ONE_I80F48))
            .div(mangoGroup.getPriceNative(marketIndex, cache))
            .div(I80F48.fromI64(perpMarketInfo.baseLotSize))
            .floor()
            .mul(liabLimit)
            .toNumber(),
        ).neg();
      }

      await client.liquidatePerpMarket(
        mangoGroup,
        liqee,
        liqor,
        perpMarket,
        payer,
        baseTransferRequest,
      );
    }

    await liqee.reload(connection, mangoGroup.dexProgramId);
    if (liqee.isBankrupt) {
      const maxLiabTransfer = liqorInitHealth.mul(liabLimit);
      const quoteRootBank = rootBanks[QUOTE_INDEX];
      if (quoteRootBank) {
        console.log('resolvePerpBankruptcy', maxLiabTransfer.toString());
        await client.resolvePerpBankruptcy(
          mangoGroup,
          liqee,
          liqor,
          perpMarket,
          quoteRootBank,
          payer,
          marketIndex,
          maxLiabTransfer,
        );
      }
      await liqee.reload(connection, mangoGroup.dexProgramId);
    }
  }
}

function getDiffsAndNet(
  mangoGroup: MangoGroup,
  mangoAccount: MangoAccount,
  cache: MangoCache,
) {
  const diffs: I80F48[] = [];
  const netValues: [number, I80F48, number][] = [];
  // Go to each base currency and see if it's above or below target

  for (let i = 0; i < groupIds!.spotMarkets.length; i++) {
    const target = TARGETS[i] !== undefined ? TARGETS[i] : 0;
    const marketIndex = groupIds!.spotMarkets[i].marketIndex;
    const diff = mangoAccount
      .getUiDeposit(cache.rootBankCache[marketIndex], mangoGroup, marketIndex)
      .sub(
        mangoAccount.getUiBorrow(
          cache.rootBankCache[marketIndex],
          mangoGroup,
          marketIndex,
        ),
      )
      .sub(I80F48.fromNumber(target));
    diffs.push(diff);
    netValues.push([i, diff.mul(cache.priceCache[i].price), marketIndex]);
  }

  return { diffs, netValues };
}

async function balanceAccount(
  mangoGroup: MangoGroup,
  mangoAccount: MangoAccount,
  mangoCache: MangoCache,
  spotMarkets: Market[],
  perpMarkets: PerpMarket[],
) {
  if (Date.now() < lastRebalance + rebalanceInterval) {
    return;
  }

  const { diffs, netValues } = getDiffsAndNet(
    mangoGroup,
    mangoAccount,
    mangoCache,
  );
  const tokensUnbalanced = netValues.some(
    (nv) => Math.abs(diffs[nv[0]].toNumber()) > spotMarkets[nv[0]].minOrderSize,
  );
  const positionsUnbalanced = perpMarkets.some((pm) => {
    const index = mangoGroup.getPerpMarketIndex(pm.publicKey);
    const perpAccount = mangoAccount.perpAccounts[index];
    const basePositionSize = Math.abs(
      pm.baseLotsToNumber(perpAccount.basePosition),
    );

    return basePositionSize != 0 || perpAccount.quotePosition.gt(ZERO_I80F48);
  });

  if (tokensUnbalanced) {
    await balanceTokens(mangoGroup, mangoAccount, spotMarkets);
  }

  if (positionsUnbalanced) {
    await closePositions(mangoGroup, mangoAccount, perpMarkets);
  }

  lastRebalance = Date.now();
}

async function balanceTokens(
  mangoGroup: MangoGroup,
  mangoAccount: MangoAccount,
  markets: Market[],
) {
  try {
    console.log('balanceTokens');
    await mangoAccount.reload(connection, mangoGroup.dexProgramId);
    const cache = await mangoGroup.loadCache(connection);
    const cancelOrdersPromises: Promise<string | undefined>[] = [];
    const bidsInfo = await getMultipleAccounts(
      connection,
      markets.map((m) => m.bidsAddress),
    );
    const bids = bidsInfo
      ? bidsInfo.map((o, i) => Orderbook.decode(markets[i], o.accountInfo.data))
      : [];
    const asksInfo = await getMultipleAccounts(
      connection,
      markets.map((m) => m.asksAddress),
    );
    const asks = asksInfo
      ? asksInfo.map((o, i) => Orderbook.decode(markets[i], o.accountInfo.data))
      : [];

    for (let i = 0; i < markets.length; i++) {
      const marketIndex = mangoGroup.getSpotMarketIndex(markets[i].publicKey);
      const orders = [...bids[i], ...asks[i]].filter((o) =>
        o.openOrdersAddress.equals(mangoAccount.spotOpenOrders[marketIndex]),
      );

      for (const order of orders) {
        cancelOrdersPromises.push(
          client.cancelSpotOrder(
            mangoGroup,
            mangoAccount,
            payer,
            markets[i],
            order,
          ),
        );
      }
    }
    console.log(`Cancelling ${cancelOrdersPromises.length} orders`);
    await Promise.all(cancelOrdersPromises);

    const openOrders = await mangoAccount.loadOpenOrders(
      connection,
      mangoGroup.dexProgramId,
    );
    const settlePromises: Promise<string | undefined>[] = [];
    for (let i = 0; i < markets.length; i++) {
      const marketIndex = mangoGroup.getSpotMarketIndex(markets[i].publicKey);
      const oo = openOrders[marketIndex];
      if (
        oo &&
        (oo.quoteTokenTotal.add(oo['referrerRebatesAccrued']).gt(new BN(0)) ||
          oo.baseTokenTotal.gt(new BN(0)))
      ) {
        settlePromises.push(
          client.settleFunds(mangoGroup, mangoAccount, payer, markets[i]),
        );
      }
    }
    console.log(`Settling on ${settlePromises.length} markets`);
    await Promise.all(settlePromises);

    const { diffs, netValues } = getDiffsAndNet(
      mangoGroup,
      mangoAccount,
      cache,
    );

    netValues.sort((a, b) => b[1].sub(a[1]).toNumber());
    for (let i = 0; i < groupIds!.spotMarkets.length; i++) {
      const marketIndex = netValues[i][2];
      const netIndex = netValues[i][0];
      const marketConfig = groupIds!.spotMarkets.find(
        (m) => m.marketIndex == marketIndex,
      )!;
      const market = markets.find((m) =>
        m.publicKey.equals(mangoGroup.spotMarkets[marketIndex].spotMarket),
      )!;
      const liquidationFee = mangoGroup.spotMarkets[marketIndex].liquidationFee;
      if (Math.abs(diffs[netIndex].toNumber()) > market!.minOrderSize) {
        const side = netValues[i][1].gt(ZERO_I80F48) ? 'sell' : 'buy';
        const price = mangoGroup
          .getPrice(marketIndex, cache)
          .mul(
            side == 'buy'
              ? ONE_I80F48.add(liquidationFee)
              : ONE_I80F48.sub(liquidationFee),
          )
          .toNumber();
        const quantity = Math.abs(diffs[netIndex].toNumber());

        console.log(
          `${side}ing ${quantity} of ${marketConfig.baseSymbol} for $${price}`,
        );
        await client.placeSpotOrder(
          mangoGroup,
          mangoAccount,
          mangoGroup.mangoCache,
          market,
          payer,
          side,
          price,
          Math.abs(diffs[netIndex].toNumber()),
          'limit',
        );
        await client.settleFunds(
          mangoGroup,
          mangoAccount,
          payer,
          markets[marketIndex],
        );
      }
    }
  } catch (err) {
    console.error('Error rebalancing tokens', err);
  }
}

async function closePositions(
  mangoGroup: MangoGroup,
  mangoAccount: MangoAccount,
  perpMarkets: PerpMarket[],
) {
  try {
    console.log('closePositions');
    await mangoAccount.reload(connection, mangoGroup.dexProgramId);
    const cache = await mangoGroup.loadCache(connection);

    for (let i = 0; i < perpMarkets.length; i++) {
      const perpMarket = perpMarkets[i];
      const index = mangoGroup.getPerpMarketIndex(perpMarket.publicKey);
      const perpAccount = mangoAccount.perpAccounts[index];

      if (perpMarket && perpAccount) {
        const openOrders = await perpMarket.loadOrdersForAccount(
          connection,
          mangoAccount,
        );

        for (const oo of openOrders) {
          await client.cancelPerpOrder(
            mangoGroup,
            mangoAccount,
            payer,
            perpMarket,
            oo,
          );
        }

        const basePositionSize = Math.abs(
          perpMarket.baseLotsToNumber(perpAccount.basePosition),
        );
        const price = mangoGroup.getPrice(index, cache);

        if (basePositionSize != 0) {
          const side = perpAccount.basePosition.gt(ZERO_BN) ? 'sell' : 'buy';
          const liquidationFee = mangoGroup.perpMarkets[index].liquidationFee;
          const orderPrice =
            side == 'sell'
              ? price.mul(ONE_I80F48.sub(liquidationFee)).toNumber()
              : price.mul(ONE_I80F48.add(liquidationFee)).toNumber();
          const bookSideInfo =
            side == 'sell'
              ? await connection.getAccountInfo(perpMarket.bids)
              : await connection.getAccountInfo(perpMarket.asks);

          console.log(
            `${side}ing ${basePositionSize} of ${groupIds?.perpMarkets[i].baseSymbol}-PERP for $${orderPrice}`,
          );

          await client.placePerpOrder(
            mangoGroup,
            mangoAccount,
            cache.publicKey,
            perpMarket,
            payer,
            side,
            orderPrice,
            basePositionSize,
            'ioc',
            0,
            bookSideInfo ? bookSideInfo : undefined,
            true,
          );
        }

        await mangoAccount.reload(connection, mangoGroup.dexProgramId);

        if (perpAccount.quotePosition.gt(ZERO_I80F48)) {
          const quoteRootBank = mangoGroup.rootBankAccounts[QUOTE_INDEX];
          if (quoteRootBank) {
            console.log('settlePnl');
            await client.settlePnl(
              mangoGroup,
              cache,
              mangoAccount,
              perpMarket,
              quoteRootBank,
              cache.priceCache[index].price,
              payer,
            );
          }
        }
      }
    }
  } catch (err) {
    console.error('Error closing positions', err);
  }
}

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

function notify(content: string) {
  if (content && process.env.WEBHOOK_URL) {
    try {
      axios.post(process.env.WEBHOOK_URL, { content });
    } catch (err) {
      console.error('Error posting to notify webhook:', err);
    }
  }
}

process.on('unhandledRejection', (err: any, p: any) => {
  console.error(`Unhandled rejection: ${err} promise: ${p})`, err);
});

main();
