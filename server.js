// =====================================================================
//  Guerra dos Reinos - servidor da live
//  Lê comentários, curtidas e presentes da sua live (YouTube ou TikTok)
//  e manda tudo para o jogo em tempo real.
//
//  Variáveis (opcionais) no Render:
//    YOUTUBE_CHANNEL = seu canal do YouTube (@ ou link). Se não colocar, o jogo pergunta.
//    YT_API_KEY      = chave da YouTube Data API (opcional, só para contar as CURTIDAS da live)
//    TIKTOK_USER     = seu @ do TikTok (sem o @).
//    SIGN_API_KEY    = chave grátis da eulerstream.com (ajuda se a conexão do TikTok falhar muito)
// =====================================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');

// ---------------------------------------------------------------------
//  WebSocket mínimo (servidor -> jogo), sem dependências extras
// ---------------------------------------------------------------------
function wsAccept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  return socket;
}
function wsFrame(str, opcode = 1) {
  const data = Buffer.from(str);
  const len = data.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, data]);
}

// ---------------------------------------------------------------------
//  Salas: uma conexão com o TikTok por @, compartilhada por todos os jogos abertos
// ---------------------------------------------------------------------
const rooms = new Map();

function cleanUser(u) {
  return String(u || '').trim().replace(/^https?:\/\/(www\.)?tiktok\.com\//i, '').replace(/^@/, '').replace(/\/live.*$/i, '').replace(/[^\w.]/g, '').slice(0, 40);
}

function getRoom(user, platform) {
  const key = platform + ':' + user;
  let room = rooms.get(key);
  if (!room) {
    room = { key, user, platform, clients: new Set(), conn: null, status: 'connecting', detail: '', retry: null, closing: false, giftLogged: false };
    rooms.set(key, room);
    if (platform === 'yt') connectYouTube(room); else connectTikTok(room);
  }
  return room;
}

function send(room, obj) {
  const frame = wsFrame(JSON.stringify(obj));
  for (const s of room.clients) { try { s.write(frame); } catch (e) {} }
}
function setStatus(room, status, detail) {
  room.status = status; room.detail = detail || '';
  console.log(`[${room.platform} ${room.user}] ${status}${detail ? ' - ' + detail : ''}`);
  send(room, { type: 'status', status, detail: room.detail, user: room.user, platform: room.platform });
}

// --- leitura defensiva dos campos (mudam entre versões da biblioteca) ---
const nameOf = d => (d.user && (d.user.nickname || d.user.uniqueId)) || d.nickname || d.uniqueId || 'anônimo';
const idOf   = d => String((d.user && (d.user.uniqueId || d.user.userId)) || d.uniqueId || d.userId || nameOf(d));
function coinsOf(d) {
  const v = d.diamondCount ?? (d.giftDetails && d.giftDetails.diamondCount) ?? (d.gift && (d.gift.diamondCount ?? d.gift.diamond_count))
    ?? (d.extendedGiftInfo && d.extendedGiftInfo.diamond_count) ?? (d.giftInfo && d.giftInfo.diamondCount);
  return Number(v) || 1;
}
function giftTypeOf(d) {
  return d.giftType ?? (d.giftDetails && d.giftDetails.giftType) ?? (d.gift && (d.gift.type ?? d.gift.giftType)) ?? (d.extendedGiftInfo && d.extendedGiftInfo.type);
}

async function connectTikTok(room) {
  if (room.closing) return;
  let lib;
  try { lib = await import('tiktok-live-connector'); }
  catch (e) { setStatus(room, 'error', 'Biblioteca tiktok-live-connector não instalada'); return; }
  const Conn = lib.TikTokLiveConnection || lib.WebcastPushConnection || (lib.default && (lib.default.TikTokLiveConnection || lib.default.WebcastPushConnection));
  const opts = { enableExtendedGiftInfo: true, processInitialData: false };
  if (process.env.SIGN_API_KEY) {
    opts.signApiKey = process.env.SIGN_API_KEY;
    if (lib.SignConfig) lib.SignConfig.apiKey = process.env.SIGN_API_KEY;
  }
  const conn = new Conn(room.user, opts);
  room.conn = conn;
  setStatus(room, 'connecting');

  // comentários -> escolher time
  conn.on('chat', d => send(room, { type: 'comment', user: nameOf(d), id: idOf(d), text: d.comment || '' }));

  // curtidas
  conn.on('like', d => send(room, { type: 'like', user: nameOf(d), id: idOf(d), count: Number(d.likeCount ?? d.count ?? 1) || 1 }));

  // presentes (sequências de combo só contam quando terminam)
  conn.on('gift', d => {
    if (!room.giftLogged) { room.giftLogged = true; console.log('[debug] primeiro presente:', JSON.stringify(d).slice(0, 800)); }
    const streakable = Number(giftTypeOf(d)) === 1;
    if (streakable && !d.repeatEnd) return;
    const count = Math.max(1, Number(d.repeatCount || 1));
    send(room, { type: 'gift', user: nameOf(d), id: idOf(d), coins: coinsOf(d), count,
      gift: (d.giftDetails && d.giftDetails.giftName) || (d.gift && d.gift.name) || (d.extendedGiftInfo && d.extendedGiftInfo.name) || '' });
  });

  conn.on('streamEnd', () => { setStatus(room, 'offline', 'A live terminou'); scheduleRetry(room, 30); });
  conn.on('disconnected', () => { if (room.status === 'live') { setStatus(room, 'reconnecting'); scheduleRetry(room, 5); } });
  conn.on('error', e => console.log(`[@${room.user}] erro:`, e && (e.message || e)));

  try {
    const state = await conn.connect();
    setStatus(room, 'live', 'Sala ' + (state && state.roomId || ''));
  } catch (e) {
    const msg = String((e && (e.message || e.exception || e)) || 'erro');
    const offline = /offline|not.*live|UserOffline|isn't online|LIVE has ended/i.test(msg + (e && e.name));
    setStatus(room, offline ? 'offline' : 'error', offline ? 'Você ainda não está ao vivo' : msg.slice(0, 160));
    scheduleRetry(room, offline ? 20 : 30);
  }
}

function scheduleRetry(room, sec) {
  clearTimeout(room.retry);
  if (room.closing) return;
  room.retry = setTimeout(() => {
    try { room.conn && room.conn.disconnect && room.conn.disconnect(); } catch (e) {}
    if (room.platform === 'yt') connectYouTube(room); else connectTikTok(room);
  }, sec * 1000);
}

function dropClient(room, socket) {
  room.clients.delete(socket);
  if (room.clients.size === 0) {
    // ninguém mais com o jogo aberto: desliga depois de 1 minuto
    setTimeout(() => {
      if (room.clients.size) return;
      room.closing = true; clearTimeout(room.retry);
      try { room.conn && room.conn.disconnect && room.conn.disconnect(); } catch (e) {}
      rooms.delete(room.key);
      console.log(`[${room.platform} ${room.user}] desligado (jogo fechado)`);
    }, 60000);
  }
}


// =====================================================================
//  YOUTUBE: lê o chat da live (sem chave, sem limite de cota)
//  Comandos no chat viram as tropas (no lugar dos presentes do TikTok).
//  Super Chat / Super Sticker também valem (quando o canal for monetizado).
// =====================================================================
const YT = process.env.YT_BASE || 'https://www.youtube.com';   // (YT_BASE só serve para testes)

// comando no chat -> "moedas" (mesma tabela de tropas do jogo) + tempo de espera por pessoa
const YT_COMMANDS = [
  { words: ['!3dragoes', '!3dragões', '!dragoes', '!dragões', '!galaxy'], coins: 1000, cooldown: 600 },
  { words: ['!dragao', '!dragão', '!dragon'],                              coins: 500,  cooldown: 300 },
  { words: ['!catapulta'],                                                 coins: 100,  cooldown: 120 },
  { words: ['!mago'],                                                      coins: 30,   cooldown: 60 },
  { words: ['!cavaleiro', '!cavalo'],                                      coins: 5,    cooldown: 30 },
  { words: ['!lanceiro', '!lança', '!lanca', '!rosa'],                     coins: 1,    cooldown: 10 },
];
// Super Chat (valor em reais) -> "moedas"
function superChatCoins(reais) {
  if (reais >= 100) return 1000;
  if (reais >= 50) return 500;
  if (reais >= 20) return 100;
  if (reais >= 10) return 30;
  if (reais >= 5) return 5;
  return 1;
}

function cleanChannel(u) {
  let v = String(u || '').trim();
  const m = v.match(/(?:youtube\.com\/|youtu\.be\/)?(@[\w.\-]+|channel\/UC[\w-]{22}|UC[\w-]{22})/i);
  if (m) v = m[1];
  else v = '@' + v.replace(/^@/, '');
  v = v.replace(/^channel\//i, '');
  if (!/^(@[\w.\-]{2,60}|UC[\w-]{22})$/.test(v)) return '';
  return v;
}
function channelUrl(ch) { return ch.startsWith('@') ? `${YT}/${ch}/live` : `${YT}/channel/${ch}/live`; }

const YT_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
  'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
  'Cookie': 'CONSENT=YES+1; SOCS=CAI',
};

function textOfRuns(msg) {
  if (!msg) return '';
  if (msg.simpleText) return msg.simpleText;
  return (msg.runs || []).map(r => r.text != null ? r.text
    : r.emoji ? ((r.emoji.shortcuts && r.emoji.shortcuts[0]) || (r.emoji.emojiId && r.emoji.emojiId.length < 4 ? r.emoji.emojiId : '')) : '').join('');
}
function parseMoney(str) {
  str = String(str || '');
  let num = str.replace(/[^\d.,]/g, '');
  if (/[.,]\d{2}$/.test(num)) num = num.slice(0, -3).replace(/[.,]/g, '') + '.' + num.slice(-2);
  else num = num.replace(/[.,]/g, '');
  let v = parseFloat(num) || 0;
  if (!/R\$/.test(str)) {                       // moeda estrangeira: conversão aproximada para reais
    if (/US\$|\$|€|£|CHF/.test(str)) v *= 5.5;
    else if (/¥|₩|₹|₱/.test(str)) v *= 0.05;
  }
  return v;
}

async function ytFetchLivePage(ch) {
  const r = await fetch(channelUrl(ch), { headers: YT_HEADERS, redirect: 'follow' });
  if (r.status === 404) throw Object.assign(new Error('Canal não encontrado: ' + ch), { fatal: true });
  const html = await r.text();
  const key = (html.match(/["']INNERTUBE_API_KEY["']\s*:\s*["']([^"']+)["']/) || [])[1];
  const ver = (html.match(/["']clientVersion["']\s*:\s*["']([\d.]+)["']/) || [])[1];
  const vid = (html.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/watch\?v=([\w-]{11})"/) || [])[1];
  const cont = (html.match(/["']reloadContinuationData["']\s*:\s*\{\s*["']continuation["']\s*:\s*["']([^"']+)["']/) || [])[1]
            || (html.match(/["']continuation["']\s*:\s*["']([^"']+)["']/) || [])[1];
  const isLive = /["']isLiveNow["']\s*:\s*true/.test(html) || /["']isLive["']\s*:\s*true/.test(html);
  if (!vid || !cont || !key || !isLive) return null;
  return { key, ver: ver || '2.20240101.00.00', vid, cont };
}

async function connectYouTube(room) {
  if (room.closing) return;
  const myRun = room.run = (room.run || 0) + 1;
  room.conn = { disconnect() { room.run++; } };
  room.cooldowns = room.cooldowns || new Map();
  setStatus(room, 'connecting');
  let info;
  try { info = await ytFetchLivePage(room.user); }
  catch (e) {
    setStatus(room, 'error', String(e.message || e).slice(0, 160));
    return scheduleRetry(room, e.fatal ? 120 : 30);
  }
  if (myRun !== room.run) return;
  if (!info) { setStatus(room, 'offline', 'Você ainda não está ao vivo'); return scheduleRetry(room, 20); }

  room.videoId = info.vid;
  setStatus(room, 'live', 'Vídeo ' + info.vid);
  if (process.env.YT_API_KEY) pollLikes(room, myRun);

  let cont = info.cont, first = true, fails = 0;
  const seen = new Set();
  while (myRun === room.run && !room.closing) {
    let wait = 2000;
    try {
      const r = await fetch(`${YT}/youtubei/v1/live_chat/get_live_chat?key=${info.key}&prettyPrint=false`, {
        method: 'POST',
        headers: { ...YT_HEADERS, 'Content-Type': 'application/json' },
        body: JSON.stringify({ context: { client: { clientName: 'WEB', clientVersion: info.ver, hl: 'pt', gl: 'BR' } }, continuation: cont }),
      });
      if (!r.ok) throw new Error('chat HTTP ' + r.status);
      const j = await r.json();
      const lc = j && j.continuationContents && j.continuationContents.liveChatContinuation;
      if (!lc) throw Object.assign(new Error('chat encerrado'), { ended: true });
      const c = (lc.continuations || [])[0] || {};
      const cd = c.invalidationContinuationData || c.timedContinuationData || c.reloadContinuationData || {};
      if (cd.continuation) cont = cd.continuation;
      if (cd.timeoutMs) wait = Math.min(Math.max(cd.timeoutMs, 1000), 5000);
      if (!first) for (const a of lc.actions || []) handleYtAction(room, a, seen);
      else for (const a of lc.actions || []) { const it = a.addChatItemAction && a.addChatItemAction.item; const rd = it && Object.values(it)[0]; if (rd && rd.id) seen.add(rd.id); }
      first = false; fails = 0;
    } catch (e) {
      fails++;
      console.log(`[yt ${room.user}] erro no chat:`, e.message || e);
      if (e.ended || fails >= 5) {
        if (myRun !== room.run) return;
        setStatus(room, 'offline', 'A live terminou');
        return scheduleRetry(room, 20);
      }
      wait = 3000 * fails;
    }
    await new Promise(r => setTimeout(r, wait));
  }
}

function handleYtAction(room, a, seen) {
  const item = a.addChatItemAction && a.addChatItemAction.item;
  if (!item) return;
  const type = Object.keys(item)[0], rd = item[type];
  if (!rd || !rd.id || seen.has(rd.id)) return;
  seen.add(rd.id); if (seen.size > 3000) seen.clear();
  const user = (rd.authorName && rd.authorName.simpleText || 'anônimo').replace(/^@/, '');
  const id = 'yt:' + (rd.authorExternalChannelId || user);
  const text = textOfRuns(rd.message || rd.headerSubtext);

  // Super Chat / Super Sticker: vale como presente
  if (type === 'liveChatPaidMessageRenderer' || type === 'liveChatPaidStickerRenderer') {
    const reais = parseMoney(rd.purchaseAmountText && rd.purchaseAmountText.simpleText);
    if (text) send(room, { type: 'comment', user, id, text });
    send(room, { type: 'gift', user, id, coins: superChatCoins(reais), count: 1, gift: 'Super Chat' });
    return;
  }
  // novo membro do canal: ganha um Mago
  if (type === 'liveChatMembershipItemRenderer') {
    send(room, { type: 'gift', user, id, coins: 30, count: 1, gift: 'Membro' });
    return;
  }
  if (type !== 'liveChatTextMessageRenderer') return;

  // comentário normal (serve para escolher AZUL ou VERMELHO)
  send(room, { type: 'comment', user, id, text });

  // comandos: !lanceiro, !cavaleiro, !mago, !catapulta, !dragao, !3dragoes
  const low = ' ' + text.toLowerCase() + ' ';
  const cmd = YT_COMMANDS.find(c => c.words.some(w => low.includes(w + ' ')));
  if (!cmd) return;
  const ck = id + '|' + cmd.coins, t = Date.now(), until = room.cooldowns.get(ck) || 0;
  if (t < until) {
    const nk = ck + '|aviso';
    if ((room.cooldowns.get(nk) || 0) < t) {
      room.cooldowns.set(nk, until);
      send(room, { type: 'notice', text: '⏳ ' + user + ': espere ' + Math.ceil((until - t) / 1000) + 's para ' + cmd.words[0] });
    }
    return;
  }
  room.cooldowns.set(ck, t + cmd.cooldown * 1000);
  if (room.cooldowns.size > 5000) room.cooldowns.clear();
  send(room, { type: 'gift', user, id, coins: cmd.coins, count: 1, gift: cmd.words[0] });
}

// curtidas da live (opcional: precisa de YT_API_KEY). O YouTube não diz QUEM curtiu, só o total.
async function pollLikes(room, myRun) {
  let last = null;
  while (myRun === room.run && !room.closing) {
    try {
      const r = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${room.videoId}&key=${process.env.YT_API_KEY}`);
      const j = await r.json();
      const n = Number(j && j.items && j.items[0] && j.items[0].statistics && j.items[0].statistics.likeCount);
      if (Number.isFinite(n)) {
        if (last != null && n > last) send(room, { type: 'like', anon: true, user: '❤️', id: 'yt:likes', count: n - last });
        last = n;
      }
    } catch (e) { console.log(`[yt ${room.user}] erro nas curtidas:`, e.message || e); }
    await new Promise(r => setTimeout(r, 15000));   // 1 consulta a cada 15s = cabe na cota grátis
  }
}

// ---------------------------------------------------------------------
//  HTTP: serve o jogo
// ---------------------------------------------------------------------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    fs.readFile(GAME, (err, buf) => {
      if (err) { res.writeHead(500); return res.end('index.html não encontrado'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    });
    return;
  }
  res.writeHead(404); res.end('não encontrado');
});

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') return socket.destroy();
  const platform = (url.searchParams.get('p') || (process.env.YOUTUBE_CHANNEL ? 'yt' : process.env.TIKTOK_USER ? 'tt' : 'yt')) === 'tt' ? 'tt' : 'yt';
  const user = platform === 'yt'
    ? cleanChannel(process.env.YOUTUBE_CHANNEL || url.searchParams.get('user'))
    : cleanUser(process.env.TIKTOK_USER || url.searchParams.get('user'));
  if (!wsAccept(req, socket)) return;
  if (!user) { socket.write(wsFrame(JSON.stringify({ type: 'status', status: 'nouser' }))); return; }
  const room = getRoom(user, platform);
  room.closing = false;
  room.clients.add(socket);
  socket.write(wsFrame(JSON.stringify({ type: 'status', status: room.status, detail: room.detail, user, platform })));
  const ping = setInterval(() => { try { socket.write(wsFrame('', 9)); } catch (e) {} }, 25000);
  socket.on('data', buf => { if ((buf[0] & 0x0f) === 8) socket.end(); });
  const bye = () => { clearInterval(ping); dropClient(room, socket); };
  socket.on('close', bye); socket.on('error', bye);
});

server.listen(PORT, () => console.log('Guerra dos Reinos rodando na porta ' + PORT));
