// This tests the liquidator by creating new liqor & liqee account on devnet.3, opening a BTC long, then crashing the oracle price leading to bankruptcy.
// The test then runs the liquidator for 60s allowing you to observe the output.
// Running this test requires:
// - mango-client-v3 is cloned into the same directory as liquidator-v3 to run the keeper and crank
// - the shared devnet keypair (Cwg...) is present at ~/.config/solana/devnet.json'
// Note that the liquidator may fail to rebalance after running due to no liquidity on the orderbook.
import fs from 'fs';
import os from 'os';
import {
  Cluster,
  Config,
  MangoClient,
  sleep,
  QUOTE_INDEX,
  IDS,
} from '@blockworks-foundation/mango-client';
import {
  Account,
  Commitment,
  Connection,
} from '@solana/web3.js';
import { Market } from '@project-serum/serum';
import { Token, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import { spawn } from 'child_process';
import * as path from 'path';

async function testPerpLiquidationAndBankruptcy() {
  const cluster = (process.env.CLUSTER || 'devnet') as Cluster;
  const config = new Config(IDS);
  const keypairPath = os.homedir() + '/.config/solana/devnet.json';
  const payer = new Account(
    JSON.parse(
      process.env.KEYPAIR ||
        fs.readFileSync(keypairPath, 'utf-8'),
    ),
  );
  
  const connection = new Connection(
    config.cluster_urls[cluster],
    'processed' as Commitment,
  );

  const groupIds = config.getGroup(cluster, 'devnet.3');
  if (!groupIds) {
    throw new Error(`Group not found`);
  }

  const client = new MangoClient(connection, groupIds.mangoProgramId);
  const mangoGroup = await client.getMangoGroup(groupIds.publicKey);
  console.log(mangoGroup.admin.toBase58());
  const perpMarkets = await Promise.all(
    groupIds.perpMarkets.map((perpMarket) => {
      return mangoGroup.loadPerpMarket(
        connection,
        perpMarket.marketIndex,
        perpMarket.baseDecimals,
        perpMarket.quoteDecimals,
      );
    }),
  );
  const spotMarkets = await Promise.all(
    groupIds.spotMarkets.map((spotMarket) => {
      return Market.load(
        connection,
        spotMarket.publicKey,
        undefined,
        groupIds.serumProgramId,
      );
    }),
  );

  // Run keeper
  const keeper = spawn('yarn', ['keeper'], {
    cwd: path.resolve(__dirname, '../../mango-client-v3/'),
    env: {
      CLUSTER: 'devnet',
      GROUP: 'devnet.3',
      PATH: process.env.PATH
    },
  });
  // keeper.stdout.on('data', (data) => {
  //   console.log(`keeper stdout: ${data}`);
  // });
  keeper.stderr.on('data', (data) => {
    console.error(`keeper stderr: ${data}`);
  });

  keeper.on('close', (code) => {
    console.log(`keeper exited with code ${code}`);
  });

  await sleep(10000);

  // Run crank
  const crank = spawn('yarn', ['crank'], {
    cwd: path.resolve(__dirname, '../../mango-client-v3/'),
    env: {
      CLUSTER: 'devnet',
      GROUP: 'devnet.3',
      PATH: process.env.PATH
    },
  });

  crank.stderr.on('data', (data) => {
    console.error(`crank stderr: ${data}`);
  });

  crank.on('close', (code) => {
    console.log(`crank exited with code ${code}`);
  });

  await sleep(10000);

  let cache = await mangoGroup.loadCache(connection);
  const rootBanks = await mangoGroup.loadRootBanks(connection);
  const quoteRootBank = rootBanks[QUOTE_INDEX];
  if (!quoteRootBank) {
    throw new Error('Quote Rootbank Not Found');
  }
  const quoteNodeBanks = await quoteRootBank.loadNodeBanks(connection);
  const quoteTokenInfo = mangoGroup.tokens[QUOTE_INDEX];
  const quoteToken = new Token(
    connection,
    quoteTokenInfo.mint,
    TOKEN_PROGRAM_ID,
    payer,
  );
  const quoteWallet = await quoteToken.getOrCreateAssociatedAccountInfo(
    payer.publicKey,
  );

  const btcToken = new Token(
    connection,
    mangoGroup.tokens[1].mint,
    TOKEN_PROGRAM_ID,
    payer,
  );
  const btcWallet = await btcToken.getOrCreateAssociatedAccountInfo(
    payer.publicKey,
  );

  const liqorPk = await client.initMangoAccount(mangoGroup, payer);
  const liqorAccount = await client.getMangoAccount(
    liqorPk,
    mangoGroup.dexProgramId,
  );
  console.log('Created Liqor:', liqorPk.toBase58());

  const liqeePk = await client.initMangoAccount(mangoGroup, payer);
  const liqeeAccount = await client.getMangoAccount(
    liqeePk,
    mangoGroup.dexProgramId,
  );
  console.log('Created Liqee:', liqeePk.toBase58());

  const makerPk = await client.initMangoAccount(mangoGroup, payer);
  const makerAccount = await client.getMangoAccount(
    makerPk,
    mangoGroup.dexProgramId,
  );
  console.log('Created Maker:', liqorPk.toBase58());

  await client.setStubOracle(
    mangoGroup.publicKey,
    mangoGroup.oracles[1],
    payer,
    60000,
  );

  // await runKeeper();
  console.log('Depositing for liqor');
  await client.deposit(
    mangoGroup,
    liqorAccount,
    payer,
    quoteRootBank.publicKey,
    quoteNodeBanks[0].publicKey,
    quoteNodeBanks[0].vault,
    quoteWallet.address,
    100000,
  );
  console.log('Depositing for liqee');
  await client.deposit(
    mangoGroup,
    liqeeAccount,
    payer,
    rootBanks[1]!.publicKey,
    rootBanks[1]!.nodeBanks[0],
    rootBanks[1]!.nodeBankAccounts[0].vault,
    btcWallet.address,
    1,
  );
  console.log('Depositing for maker');
  await client.deposit(
    mangoGroup,
    makerAccount,
    payer,
    quoteRootBank.publicKey,
    quoteNodeBanks[0].publicKey,
    quoteNodeBanks[0].vault,
    quoteWallet.address,
    100000,
  );

  // await runKeeper();

  console.log('Placing maker orders');
  await client.placePerpOrder(
    mangoGroup,
    makerAccount,
    mangoGroup.mangoCache,
    perpMarkets[0],
    payer,
    'sell',
    60000,
    0.0111,
    'limit',
  );

  await client.placePerpOrder(
    mangoGroup,
    makerAccount,
    mangoGroup.mangoCache,
    perpMarkets[0],
    payer,
    'buy',
    1000,
    10,
    'limit',
  );

  await client.placeSpotOrder2(
    mangoGroup,
    makerAccount,
    spotMarkets[1],
    payer,
    'buy',
    100,
    3,
    'postOnly',
  );

  console.log('Placing taker order');
  await client.placePerpOrder(
    mangoGroup,
    liqeeAccount,
    mangoGroup.mangoCache,
    perpMarkets[0],
    payer,
    'buy',
    60000,
    1,
    'market',
  );

  // await runKeeper();
  await liqeeAccount.reload(connection);
  await liqorAccount.reload(connection);

  console.log(
    'Liqor base',
    liqorAccount.perpAccounts[1].basePosition.toString(),
  );
  console.log(
    'Liqor quote',
    liqorAccount.perpAccounts[1].quotePosition.toString(),
  );
  console.log(
    'Liqee base',
    liqeeAccount.perpAccounts[1].basePosition.toString(),
  );
  console.log(
    'Liqee quote',
    liqeeAccount.perpAccounts[1].quotePosition.toString(),
  );

  await client.setStubOracle(
    mangoGroup.publicKey,
    mangoGroup.oracles[1],
    payer,
    100,
  );

  cache = await mangoGroup.loadCache(connection);
  await liqeeAccount.reload(connection);
  await liqorAccount.reload(connection);

  console.log(
    'Liqee Maint Health',
    liqeeAccount.getHealthRatio(mangoGroup, cache, 'Maint').toString(),
  );
  console.log(
    'Liqor Maint Health',
    liqorAccount.getHealthRatio(mangoGroup, cache, 'Maint').toString(),
  );

  // Run the liquidator process for 60s
  const liquidator = spawn('yarn', ['liquidator'], {
    env: {
      CLUSTER: 'devnet',
      GROUP: 'devnet.3',
      KEYPAIR: keypairPath,
      LIQOR_PK: liqorAccount.publicKey.toBase58(),
      PATH: process.env.PATH
    },
  });

  liquidator.stdout.on('data', (data) => {
    console.log(`Liquidator stdout: ${data}`);
  });

  liquidator.stderr.on('data', (data) => {
    console.error(`Liquidator stderr: ${data}`);
  });

  liquidator.on('close', (code) => {
    console.log(`Liquidator exited with code ${code}`);
  });

  await sleep(60000);
  liquidator.kill();

  await liqeeAccount.reload(connection);
  await liqorAccount.reload(connection);

  console.log(
    'Liqee Maint Health',
    liqeeAccount.getHealthRatio(mangoGroup, cache, 'Maint').toString(),
  );
  console.log('Liqee Bankrupt', liqeeAccount.isBankrupt);
  console.log(
    'Liqor Maint Health',
    liqorAccount.getHealthRatio(mangoGroup, cache, 'Maint').toString(),
  );
}

testPerpLiquidationAndBankruptcy();
