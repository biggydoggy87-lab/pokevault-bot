/**
 * PokeVault Bot — Manual Transfer Edition FINAL
 * NFT detection: original working Helius version (found Venusaur correctly)
 * WebSocket: heartbeat added to keep connection alive during draw
 */

require('dotenv').config({ override: false });
const { Connection, PublicKey, Keypair, LAMPORTS_PER_SOL } = require('@solana/web3.js');
const { TOKEN_PROGRAM_ID } = require('@solana/spl-token');
const fetch     = require('node-fetch');
const crypto    = require('crypto');
const WebSocket = require('ws');
const express   = require('express');
const cors      = require('cors');
const http      = require('http');

const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const HELIUS = 'https://mainnet.helius-rpc.com/?api-key=e4cb8eed-5e8f-42d7-afb4-00102a9e9302';

const CFG = {
  TOKEN_CA:    process.env.TOKEN_CA           || '4P3AW7azyDDBxYLDbdwKHphHqCKzoAqF5j3wDN9ypump',
  WALLET_PK:   process.env.CREATOR_WALLET_PK  || '',
  ADMIN_KEY:   process.env.ADMIN_KEY           || '2727',
  PORT:        parseInt(process.env.PORT        || '3001'),
  INTERVAL_MS: 3 * 60 * 1000, // 3 mins for testing
};

function decodeBase58(str) {
  const A = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  let r = BigInt(0);
  for (const c of str) { const i = A.indexOf(c); if (i < 0) throw new Error('Bad b58'); r = r * 58n + BigInt(i); }
  const b = []; while (r > 0n) { b.unshift(Number(r & 0xffn)); r >>= 8n; }
  for (const c of str) { if (c === '1') b.unshift(0); else break; }
  return Uint8Array.from(b);
}

const connection = new Connection(HELIUS, 'confirmed');
let wallet;
try {
  if (CFG.WALLET_PK) {
    let key;
    try { const b = require('bs58'); key = typeof b.decode === 'function' ? b.decode(CFG.WALLET_PK) : b.default.decode(CFG.WALLET_PK); }
    catch { key = decodeBase58(CFG.WALLET_PK); }
    wallet = Keypair.fromSecretKey(key);
    console.log('[OK] Wallet:', wallet.publicKey.toBase58());
  }
} catch(e) { console.log('[WARN] Wallet:', e.message); }

const STATE = {
  draws: 0, cardsOut: 0, totalVal: 0, totalSpent: 0,
  holders: 0, holdersList: [], solBalance: 0, solPrice: 160,
  lastDrawTime: null, winners: [], recentCards: [],
  nextDrawMs: CFG.INTERVAL_MS,
  lastDrawResult: null,
  givenOutMints: [], // tracks which NFTs have already been drawn so they are not repeated
};

let wss;

function send(ws, type, payload) {
  try { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type, payload, ts: Date.now() })); } catch {}
}

function broadcast(type, payload) {
  if (!wss) return;
  const msg = JSON.stringify({ type, payload, ts: Date.now() });
  wss.clients.forEach(c => { try { if (c.readyState === WebSocket.OPEN) c.send(msg); } catch {} });
}

function log(msg, type = 'info') {
  console.log(`[${new Date().toISOString()}] ${msg}`);
  broadcast('feed', { msg, type });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getSolPrice() {
  try {
    const r = await fetch('https://price.jup.ag/v6/price?ids=SOL');
    const d = await r.json();
    if (d?.data?.SOL?.price) STATE.solPrice = d.data.SOL.price;
  } catch {}
}

async function getWalletBalance() {
  if (!wallet) return 0;
  try {
    const bal = await connection.getBalance(wallet.publicKey);
    STATE.solBalance = bal / LAMPORTS_PER_SOL;
    broadcast('treasury', { feeBalanceSol: STATE.solBalance, feeBalanceUsd: STATE.solBalance * STATE.solPrice, solBalance: STATE.solBalance, solBalanceUsd: STATE.solBalance * STATE.solPrice });
    return STATE.solBalance;
  } catch { return 0; }
}

// ── PURE ON-CHAIN NFT DETECTION — no indexers, no API keys ────────────────
const METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

async function getMetadataAddress(mint) {
  const [pda] = await PublicKey.findProgramAddress(
    [Buffer.from('metadata'), METADATA_PROGRAM_ID.toBuffer(), new PublicKey(mint).toBuffer()],
    METADATA_PROGRAM_ID
  );
  return pda;
}

// Metaplex metadata uses 4-byte LE length prefix before each string field
function readLengthPrefixedString(buf, offset) {
  try {
    const len = buf.readUInt32LE(offset);
    const str = buf.slice(offset + 4, offset + 4 + len).toString('utf8');
    return { value: str.replace(/\0/g, '').trim(), nextOffset: offset + 4 + len };
  } catch {
    return { value: '', nextOffset: offset + 4 };
  }
}

async function fetchJsonUri(uri) {
  if (!uri) return null;
  try {
    const url = uri.startsWith('ipfs://') ? uri.replace('ipfs://', 'https://ipfs.io/ipfs/') : uri;
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

async function getWalletNfts() {
  if (!wallet) return [];
  const results = [];

  try {
    // Get every SPL token account owned by this wallet — pure RPC, no indexer
    const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
      wallet.publicKey,
      { programId: TOKEN_PROGRAM_ID }
    );
    log(`SPL token accounts: ${tokenAccounts.value.length}`, 'info');

    // NFTs = amount 1, decimals 0
    const nftAccounts = tokenAccounts.value.filter(a => {
      const info = a.account.data.parsed.info;
      return Number(info.tokenAmount.amount) === 1 && info.tokenAmount.decimals === 0;
    });
    log(`NFT-shaped accounts: ${nftAccounts.length}`, nftAccounts.length > 0 ? 'ok' : 'warn');

    for (const acct of nftAccounts) {
      const mint = acct.account.data.parsed.info.mint;
      let name = 'Collector Crypt Card', image = '';

      try {
        const metaPda = await getMetadataAddress(mint);
        const metaAccountInfo = await connection.getAccountInfo(metaPda);

        if (metaAccountInfo?.data) {
          const buf = metaAccountInfo.data;
          // Layout: key(1) + updateAuth(32) + mint(32) = 65, then length-prefixed name, symbol, uri
          let offset = 65;
          const nameResult = readLengthPrefixedString(buf, offset);
          offset = nameResult.nextOffset;
          const symbolResult = readLengthPrefixedString(buf, offset);
          offset = symbolResult.nextOffset;
          const uriResult = readLengthPrefixedString(buf, offset);

          const rawName = nameResult.value;
          const rawUri  = uriResult.value;

          log(`Parsed metadata — name: "${rawName}" uri: "${rawUri}"`, 'info');

          if (rawName) name = rawName;

          if (rawUri) {
            const json = await fetchJsonUri(rawUri);
            if (json) {
              name  = json.name  || name;
              image = json.image || json.animation_url || '';
              log(`Fetched JSON — name: "${json.name}" image: "${json.image}"`, 'info');
            } else {
              log(`Could not fetch/parse JSON from URI: ${rawUri}`, 'warn');
            }
          }
        } else {
          log(`No metadata account for mint ${mint}`, 'info');
        }
      } catch(e) {
        log(`Metadata read error for ${mint}: ${e.message}`, 'warn');
      }

      log(`Card: ${name} | mint: ${mint} | image: ${image ? 'YES' : 'NO'}`, 'info');
      results.push({ nftAddress: mint, name, image, price: 25, rarity: 'COMMON', grade: 'NFT' });
    }
  } catch(e) {
    log('Token account fetch error: ' + e.message, 'error');
  }

  log(`Total cards in vault: ${results.length}`, results.length > 0 ? 'ok' : 'warn');
  return results;
}

async function getEligibleHolders() {
  if (!CFG.TOKEN_CA) return [];
  const holders = [];
  try {
    const mint = new PublicKey(CFG.TOKEN_CA);
    for (const prog of [TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID.toBase58()]) {
      try {
        const r = await fetch(HELIUS, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getProgramAccounts',
            params: [prog, { encoding: 'jsonParsed', filters: [{ memcmp: { offset: 0, bytes: mint.toBase58() } }] }]
          })
        });
        const data = await r.json();
        if (data.result?.length > 0) {
          for (const acct of data.result) {
            const info = acct.account?.data?.parsed?.info;
            if (!info) continue;
            const tokens = parseFloat(info.tokenAmount?.uiAmountString || '0');
            if (tokens >= 1) holders.push({ wallet: info.owner, tokens, tickets: 1 });
          }
          if (holders.length) break;
        }
      } catch {}
    }
  } catch(e) { log('Holders error: ' + e.message, 'error'); }
  STATE.holders = holders.length;
  STATE.holdersList = holders;
  broadcast('holders', { count: holders.length, holders });
  log(`${holders.length} eligible holders`, holders.length ? 'ok' : 'warn');
  return holders;
}

async function runDraw() {
  log(`=== DRAW #${STATE.draws + 1} STARTING ===`, 'draw');
  console.log(`[DRAW] Connected WS clients: ${wss ? wss.clients.size : 0}`);

  try {
    await getWalletBalance();
    const holders = await getEligibleHolders();
    if (!holders.length) { log('No holders — skipping', 'warn'); return; }

    const nfts = await getWalletNfts();
    if (!nfts.length) { log('No cards in wallet — add a card!', 'warn'); return; }

    // Pick a random card from the vault that hasn't already been given out
    const availableCards = nfts.filter(n => !STATE.givenOutMints.includes(n.nftAddress));
    const cardPool = availableCards.length > 0 ? availableCards : nfts; // fallback if all given out (e.g. duplicates)
    const card = cardPool[Math.floor(Math.random() * cardPool.length)];
    log(`Vault has ${nfts.length} card(s), ${availableCards.length} not yet given out — picked: ${card.name}`, 'info');

    const pool   = holders.map(h => h.wallet);
    const seed   = crypto.randomBytes(16).toString('hex') + Date.now();
    const hmac   = crypto.createHmac('sha256', seed);
    hmac.update(STATE.draws.toString());
    const winner = pool[parseInt(hmac.digest('hex').slice(0, 8), 16) % pool.length];
    log(`Winner: ${winner}`, 'ok');

    const drawPayload = {
      drawNumber: STATE.draws + 1,
      card: card.name, image: card.image, price: card.price,
      grade: card.grade, rarity: card.rarity,
      wallet: winner, winner: winner,
    };

    STATE.lastDrawResult = { status: 'running', card, winner, drawPayload };

    console.log(`[DRAW] Broadcasting drawStart to ${wss.clients.size} clients`);
    broadcast('cardReceived', { name: card.name, image: card.image, price: card.price, rarity: card.rarity, grade: card.grade });
    await sleep(300);
    broadcast('drawStart', drawPayload);

    await sleep(8000);

    STATE.draws++;
    STATE.cardsOut++;
    STATE.totalVal += card.price;
    STATE.lastDrawTime = new Date();
    const now = new Date();
    const dateStr = `${now.toLocaleString('default',{month:'short'})} ${now.getDate()}, ${String(now.getFullYear()).slice(2)}`;
    const record = { wallet: winner, card: card.name, price: card.price, image: card.image, grade: card.grade, rarity: card.rarity, date: dateStr };
    STATE.winners.unshift(record);
    STATE.recentCards.unshift({ name: card.name, image: card.image, price: card.price, rarity: card.rarity, grade: card.grade, date: dateStr });
    STATE.givenOutMints.push(card.nftAddress); // mark this card as given out so it won't repeat

    const completePayload = { wallet: winner, winner, card: card.name, image: card.image, price: card.price, grade: card.grade, rarity: card.rarity, date: dateStr, state: getPublicState() };
    broadcast('drawComplete', completePayload);

    STATE.lastDrawResult = { status: 'complete', card, winner, drawPayload, completePayload };
    setTimeout(() => { STATE.lastDrawResult = null; }, 10 * 60 * 1000);

    log(`=== DRAW #${STATE.draws} COMPLETE ===`, 'ok');
    log(`Card: ${card.name} | Winner: ${winner}`, 'ok');
    log(`ACTION: Transfer ${card.name} to ${winner} via collectorcrypt.com`, 'warn');

  } catch(e) {
    log('Draw error: ' + e.message, 'error');
    console.error(e);
  }
}

function getPublicState() {
  return {
    draws: STATE.draws, cardsOut: STATE.cardsOut, totalVal: STATE.totalVal, totalSpent: STATE.totalVal,
    holders: STATE.holders, holders_list: STATE.holdersList,
    feeBalanceSol: STATE.solBalance, feeBalanceUsd: STATE.solBalance * STATE.solPrice,
    solBalance: STATE.solBalance, solBalanceUsd: STATE.solBalance * STATE.solPrice,
    solPrice: STATE.solPrice, botRunning: true, tokenCA: CFG.TOKEN_CA,
    winners: STATE.winners.slice(0, 50), recentCards: STATE.recentCards.slice(0, 20),
    nextDrawIn: STATE.nextDrawMs, drawNumber: STATE.draws + 1,
    lastDrawMinsAgo: STATE.lastDrawTime ? Math.floor((Date.now() - STATE.lastDrawTime.getTime()) / 60000) : null,
    lastDrawResult: STATE.lastDrawResult,
  };
}

function startServer() {
  const app = express();
  app.use(cors({ origin: '*', methods: ['GET','POST','OPTIONS'] }));
  app.options('*', cors());
  app.use(express.json());

  function isAdmin(req) {
    return (req.headers['x-admin-key'] || req.body?.adminKey || req.query?.adminKey) === CFG.ADMIN_KEY;
  }

  app.get('/health',      (_,res) => res.json({ ok: true, draws: STATE.draws, clients: wss?.clients?.size || 0, givenOut: STATE.givenOutMints.length }));
  app.get('/api/state',   (_,res) => res.json(getPublicState()));
  app.get('/api/winners', (_,res) => res.json(STATE.winners));
  app.get('/api/holders', (_,res) => res.json({ count: STATE.holders, holders: STATE.holdersList }));
  app.get('/api/nfts',    async (_,res) => res.json(await getWalletNfts()));

  const triggerDraw = (req, res) => {
    if (!isAdmin(req)) return res.status(401).json({ error: 'bad key' });
    console.log('[FORCE DRAW] triggered');
    res.json({ ok: true, message: 'Draw triggered' });
    setTimeout(runDraw, 100);
  };
  app.post('/admin/force-draw', triggerDraw);
  app.post('/admin/forcedraw',  triggerDraw);
  app.post('/api/spin',         triggerDraw);
  app.post('/admin/reset',      (req, res) => {
    if (!isAdmin(req)) return res.status(401).json({ error: 'bad key' });
    STATE.lastDrawResult = null;
    res.json({ ok: true });
    log('Reset by admin', 'warn');
  });

  const server = http.createServer(app);
  wss = new WebSocket.Server({ server });

  // HEARTBEAT — keeps connections alive so draw events always reach frontend
  const heartbeat = setInterval(() => {
    wss.clients.forEach(ws => {
      if (ws.isAlive === false) { return ws.terminate(); }
      ws.isAlive = false;
      try { ws.ping(); } catch {}
    });
  }, 15000);

  wss.on('close', () => clearInterval(heartbeat));

  wss.on('connection', ws => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    console.log('[WS] Client connected, total:', wss.clients.size);

    send(ws, 'state',   getPublicState());
    send(ws, 'holders', { count: STATE.holdersList.length, holders: STATE.holdersList });

    // Replay last draw to late-connecting clients
    if (STATE.lastDrawResult) {
      const r = STATE.lastDrawResult;
      if (r.status === 'running') {
        setTimeout(() => {
          send(ws, 'cardReceived', { name: r.card.name, image: r.card.image, price: r.card.price, rarity: r.card.rarity, grade: r.card.grade });
          setTimeout(() => send(ws, 'drawStart', r.drawPayload), 300);
        }, 500);
      } else if (r.status === 'complete') {
        setTimeout(() => send(ws, 'drawComplete', r.completePayload), 500);
      }
    }

    ws.on('message', d => {
      try { const { type } = JSON.parse(d); if (type === 'ping') send(ws, 'pong', {}); } catch {}
    });
    ws.on('error', () => {});
    ws.on('close', () => console.log('[WS] Client disconnected, remaining:', wss.clients.size));
  });

  server.listen(CFG.PORT, () => console.log(`[OK] PokeVault on port ${CFG.PORT}`));
}

let countdownMs = CFG.INTERVAL_MS;
setInterval(() => {
  countdownMs -= 1000;
  STATE.nextDrawMs = countdownMs;
  if (countdownMs <= 0) { countdownMs = CFG.INTERVAL_MS; runDraw(); }
  broadcast('tick', { nextDrawIn: countdownMs, feeBalanceSol: STATE.solBalance, feeBalanceUsd: STATE.solBalance * STATE.solPrice, solBalance: STATE.solBalance, solBalanceUsd: STATE.solBalance * STATE.solPrice, holders: STATE.holders, lastDrawMinsAgo: STATE.lastDrawTime ? (Date.now() - STATE.lastDrawTime.getTime()) / 60000 : null });
}, 1000);

setInterval(getWalletBalance, 60 * 1000);

async function main() {
  console.log('[OK] PokeVault starting...');
  startServer();
  await getSolPrice();
  await getWalletBalance();
  await getEligibleHolders();
  const nfts = await getWalletNfts();
  console.log(`[OK] Ready — ${nfts.length} card(s) in vault, ${STATE.holders} holder(s)`);
}

main().catch(console.error);
