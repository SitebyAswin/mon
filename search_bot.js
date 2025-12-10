'use strict';
/**
 * search_bot.polished.js
 * Polished copy with Group Auto-Delete Configuration & Admin Dashboard.
 */

require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

const TOKEN = process.env.SEARCH_BOT_TOKEN;
const ADMIN_USER_ID = process.env.ADMIN_USER_ID || process.env.ADMIN_ID || '';
const TESTER_BOT_USERNAME = process.env.TESTER_BOT_USERNAME || process.env.BOT_USERNAME || 'Testerforcloudbot';
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID; // set to your admin Telegram id
const SUGGESTIONS_FILE = path.join(__dirname, 'suggestions.json');

if (!TOKEN) {
  console.error('Missing SEARCH_BOT_TOKEN in .env');
  process.exit(1);
}

// helper: ensure suggestions file exists
if (!fs.existsSync(SUGGESTIONS_FILE)) fs.writeFileSync(SUGGESTIONS_FILE, JSON.stringify([]));

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const INDEX_PATH = path.join(DATA_DIR, 'index.js');
const USERS_PATH = path.join(DATA_DIR, 'users.json');
const BOTNAMES_PATH = path.join(DATA_DIR, 'botnames.json');
const SETTINGS_PATH = path.join(DATA_DIR, 'settings.json'); // New settings file

const ITEMS_PER_PAGE = Number(process.env.ITEMS_PER_PAGE || 8);
const INLINE_MAX = Number(process.env.INLINE_MAX || 20);

const bot = new TelegramBot(TOKEN, { polling: true });

// ------------------ Utilities ------------------
function ensureDataDir() {
  try { if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) { console.warn('ensureDataDir failed', e && e.message); }
}
ensureDataDir();

function loadJSON(filePath, defaultObj) {
  try {
    if (!fs.existsSync(filePath)) return defaultObj;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw || 'null') || defaultObj;
  } catch (e) { console.warn('loadJSON', filePath, e && e.message); return defaultObj; }
}
function saveJSON(filePath, obj) {
  try { fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8'); } catch (e) { console.warn('saveJSON failed', filePath, e && e.message); }
}

// ------------------ Settings Management ------------------
// Default: Groups have auto-delete ON, set to 60 seconds.
let botSettings = loadJSON(SETTINGS_PATH, { 
  groupAutoDelete: true, 
  groupDeleteTimer: 60 
});

function saveSettings() {
  saveJSON(SETTINGS_PATH, botSettings);
}

// Send a message that auto-deletes after timeoutMs
// If timeoutMs is 0 or null, it acts as a normal sendMessage (no delete)
async function sendAutoDelete(chatId, text, options = {}, timeoutMs = 0) {
  const sent = await bot.sendMessage(chatId, text, options);
  if (!sent || !sent.message_id) return sent;

  if (timeoutMs > 0) {
    setTimeout(() => {
      bot.deleteMessage(chatId, sent.message_id).catch(() => {
        // ignore delete errors (e.g., no rights / already deleted)
      });
    }, timeoutMs);
  }

  return sent;
}

function escapeHtml(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
function short(text, n = 120) { if (!text) return ''; return text.length > n ? text.slice(0, n - 1) + '…' : text; }
function levenshtein(a,b){ if(!a) return b?b.length:0; if(!b) return a.length; a=a.toLowerCase(); b=b.toLowerCase(); const m=a.length,n=b.length; const dp=Array.from({length:m+1},()=>new Array(n+1).fill(0)); for(let i=0;i<=m;i++) dp[i][0]=i; for(let j=0;j<=n;j++) dp[0][j]=j; for(let i=1;i<=m;i++) for(let j=1;j<=n;j++){ const cost = a[i-1]===b[j-1]?0:1; dp[i][j]=Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost); } return dp[m][n]; }
function similarity(a,b){ if(!a||!b) return 0; const maxLen = Math.max(a.length, b.length, 1); return 1 - (levenshtein(a,b)/maxLen); }

// Turn "Baaghi 2_NB3WIVTV415G" into "Baaghi 2"
function prettyNameFromFilename(name) {
  if (!name) return '';
  let base = String(name).replace(/\.js$/i, '');
  const parts = base.split('_');
  if (parts.length <= 1) return base;
  const last = parts[parts.length - 1];
  if (/^\d+$/.test(last) && parts.length >= 2) {
    const prev = parts[parts.length - 2];
    if (/^(?=.*[A-Za-z])[A-Za-z0-9]{6,}$/.test(prev)) {
      return parts.slice(0, parts.length - 2).join(' ');
    }
  }
  if (/^(?=.*[A-Za-z])[A-Za-z0-9]{6,}$/.test(last)) {
    return parts.slice(0, -1).join(' ');
  }
  return base;
}

// ------------------ Caching & file helpers ------------------
let indexCache = { tokens: {}, order: [], _raw: null, loadedAt: 0 };
let batchMetaCache = {}; 

function clearCaches() {
  indexCache = { tokens: {}, order: [], _raw: null, loadedAt: 0 };
  batchMetaCache = {};
}

function safeRequire(fp) {
  try { delete require.cache[require.resolve(fp)]; return require(fp); } catch (e) { return null; }
}

function loadIndex() {
  try {
    if (indexCache._raw && (Date.now() - indexCache.loadedAt < 5000)) return indexCache;
    const idx = safeRequire(INDEX_PATH) || {};
    indexCache.tokens = idx.tokens || {};
    indexCache.order = Array.isArray(idx.order) ? idx.order.slice() : Array.from(new Set(Object.values(indexCache.tokens || {})));
    indexCache._raw = idx;
    indexCache.loadedAt = Date.now();
    return indexCache;
  } catch (e) {
    console.warn('[search_bot] loadIndex failed', e && e.message);
    return { tokens: {}, order: [] };
  }
}

function readBatch(filename) {
  const p = path.join(DATA_DIR, filename + '.js');
  try { delete require.cache[require.resolve(p)]; return require(p); } catch (e) { return null; }
}

function metaForBatch(filename) {
  const cached = batchMetaCache[filename];
  if (cached && (Date.now() - cached.loadedAt < 10_000)) return cached;
  const batch = readBatch(filename);
  let display_name = '';
  if (batch && batch.display_name && String(batch.display_name).trim()) display_name = String(batch.display_name).trim();
  else if (batch && batch.filename) display_name = prettyNameFromFilename(batch.filename);
  else display_name = prettyNameFromFilename(filename);
  let previewText = '';
  if (batch && Array.isArray(batch.files) && batch.files.length) {
    const f = batch.files[0];
    previewText = (f && (f.caption || f.text || f.file_name || '')) || '';
  }
  const obj = { display_name, previewText, loadedAt: Date.now() };
  batchMetaCache[filename] = obj;
  return obj;
}

// watch invalidation
try {
  let dt = null;
  fs.watch(DATA_DIR, { persistent: false }, (ev, fn) => {
    if (dt) clearTimeout(dt);
    dt = setTimeout(() => { clearCaches(); dt = null; }, 250);
  });
} catch (e) {}

// ------------------ Persistent users ------------------
let knownUsers = loadJSON(USERS_PATH, {});
function upsertUser(user) {
  if (!user || !user.id) return;
  const id = String(user.id);
  const entry = knownUsers[id] || {};
  entry.id = user.id;
  entry.first_name = user.first_name || entry.first_name || '';
  entry.username = user.username || entry.username || '';
  entry.last_seen_iso = new Date().toISOString();
  knownUsers[id] = entry;
  saveJSON(USERS_PATH, knownUsers);
}

// ------------------ Link-Converter bot-name registrations ------------------
let userBotNames = loadJSON(BOTNAMES_PATH, {});
function setUserBotName(userId, botname) { userBotNames[String(userId)] = botname; saveJSON(BOTNAMES_PATH, userBotNames); }
function getUserBotName(userId) { return userBotNames[String(userId)]; }

// ------------------ Action map ------------------
const actionMap = {}; 
function makeActionId() { return Math.random().toString(36).slice(2,10) + Date.now().toString(36).slice(-4); }
function addAction(obj) {
  const id = makeActionId();
  actionMap[id] = Object.assign({}, obj, { createdAt: Date.now() });
  setTimeout(() => { delete actionMap[id]; }, 15*60*1000); 
  return id;
}

// ------------------ Trackers ------------------
const chatIds = new Set();
let broadcastQueue = {};
const pendingReports = {}; 

// ------------------ Helper: Build Settings Menu ------------------
function buildSettingsMenu() {
  const isOn = botSettings.groupAutoDelete;
  const timer = botSettings.groupDeleteTimer;
  
  const text = `<b>⚙️ Bot Settings (Admin)</b>\n\n` +
               `<b>Group Auto-Delete:</b> ${isOn ? '✅ ON' : '❌ OFF'}\n` +
               `<b>Timer Duration:</b> ${timer} seconds\n\n` +
               `<i>When ON, search results in groups will be deleted after the set time.</i>`;

  const kb = [
    [
      { text: isOn ? '🔴 Turn OFF' : '🟢 Turn ON', callback_data: 'setting_toggle_delete' }
    ],
    [
      { text: 'Set 30s', callback_data: 'setting_time_30' },
      { text: 'Set 1m', callback_data: 'setting_time_60' },
      { text: 'Set 2m', callback_data: 'setting_time_120' }
    ],
    [
      { text: 'Set 5m', callback_data: 'setting_time_300' },
      { text: 'Set 10m', callback_data: 'setting_time_600' }
    ],
    [{ text: '❌ Close', callback_data: 'close' }]
  ];

  return { text, reply_markup: { inline_keyboard: kb } };
}

// ------------------ Message search handler ------------------
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.text || !msg.chat) return;
    const text = String(msg.text || '').trim();
    if (!text || text.startsWith('/')) return; // ignore commands

    const q = text.toLowerCase();
    const idx = loadIndex();
    const tokens = Object.keys(idx.tokens || {});
    
    // Calculate Timeout based on Settings
    let deleteTimeout = 0;
    const isGroup = msg.chat.type === 'group' || msg.chat.type === 'supergroup';

    if (isGroup) {
      if (botSettings.groupAutoDelete) {
        deleteTimeout = botSettings.groupDeleteTimer * 1000;
      } else {
        deleteTimeout = 0;
      }
    } else {
      deleteTimeout = 2 * 60 * 1000; // Default 2 mins for private chat
    }

    if (!tokens.length) {
      return sendAutoDelete(msg.chat.id, 'No indexed batches available yet.', {}, deleteTimeout);
    }

    // collect matches
    const matches = [];
    for (const t of tokens) {
      if (matches.length >= 8) break;
      const fname = idx.tokens[t];
      const meta = metaForBatch(fname) || { display_name: fname, previewText: '' };
      const nameLower = String(meta.display_name || '').toLowerCase();
      if (nameLower.includes(q)) {
        matches.push({ token: t, fname, title: meta.display_name || fname });
      }
    }

    if (matches.length === 0) {
      for (const t of tokens) {
        if (matches.length >= 6) break;
        const fname = idx.tokens[t];
        const meta = metaForBatch(fname) || { display_name: fname, previewText: '' };
        try {
          if (similarity(String(meta.display_name || ''), q) > 0.55) {
            matches.push({ token: t, fname, title: meta.display_name || fname });
          }
        } catch (e) { }
      }
    }

    if (!matches.length) {
      const help = `No batches matched "${escapeHtml(text)}".\n\nTry inline search: type @${TESTER_BOT_USERNAME} ${escapeHtml(text)} in any chat, or use /view to browse the index.`;
      const googleUrl = `https://www.google.com/search?q=${encodeURIComponent('"' + String(text) + '"')}`;
      
      // FIX: Use addAction for suggestion to avoid BUTTON_DATA_INVALID
      const suggestAct = addAction({ type: 'suggest_text', text: text });
      
      const keyboardFallback = {
        inline_keyboard: [
          [{ text: '🔎 Google exact (search)', url: googleUrl }],
          [{ text: '📣 Suggest this query', callback_data: `act_${suggestAct}` }]
        ]
      };
      return sendAutoDelete(msg.chat.id, help, { parse_mode: 'HTML', reply_markup: keyboardFallback }, deleteTimeout);
    }

    const userBot = getUserBotName(String(msg.from && msg.from.id)) || TESTER_BOT_USERNAME || '';
    const rows = [];
    const lines = [];
    for (const m of matches.slice(0, 8)) {
      const display = m.title || m.fname;
      const deepLink = userBot
        ? `https://t.me/${userBot}?start=${encodeURIComponent(m.token)}`
        : `https://t.me/?start=${encodeURIComponent(m.token)}`;
      lines.push(`• ${escapeHtml(String(display))} — <a href="${deepLink}">Open</a>`);
      
      // FIX: Use addAction for token suggestion
      const suggestTokenAct = addAction({ type: 'suggest_token', token: m.token });
      
      rows.push([
        { text: short(display, 30), url: deepLink },
        { text: '📣 Suggest', callback_data: `act_${suggestTokenAct}` }
      ]);
    }

    // FIX: Use addAction for query suggestion
    const suggestTextAct = addAction({ type: 'suggest_text', text: text });

    rows.push([
      { text: '📣 Suggest this query', callback_data: `act_${suggestTextAct}` },
      { text: '📚 Browse /view', callback_data: 'view_1' }
    ]);

    await sendAutoDelete(
      msg.chat.id,
      `<b>Search results for:</b> ${escapeHtml(text)}\n\n${lines.join('\n')}`,
      {
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: { inline_keyboard: rows }
      },
      deleteTimeout
    );

  } catch (err) {
    console.error('[search_bot] message-search error', err && err.message);
  }
});

// ------------------ Build view page payload ------------------
function buildViewPagePayload(page = 0) {
  const idx = loadIndex();
  const order = idx.order || [];
  const total = order.length;
  const totalPages = Math.max(1, Math.ceil(total / ITEMS_PER_PAGE));
  const p = Math.min(Math.max(0, page), totalPages - 1);
  const start = p * ITEMS_PER_PAGE;
  const slice = order.slice(start, start + ITEMS_PER_PAGE);

  const lines = [];
  for (let i = 0; i < slice.length; i++) {
    const fname = slice[i];
    const meta = metaForBatch(fname);
    const tokens = idx.tokens || {};
    const tokenKey = Object.keys(tokens).find(k => tokens[k] === fname) || '';
    lines.push(`${start + i + 1}. ${escapeHtml(meta.display_name)} — <code>${escapeHtml(tokenKey)}</code>`);
  }

  const text = `Index — Page ${p+1}/${totalPages}\n\n` + (lines.length ? lines.join('\n') : 'No items');
  const kb = [];
  for (let i = 0; i < slice.length; i++) {
    const fname = slice[i];
    const tokenKey = Object.keys(idx.tokens || {}).find(k => idx.tokens[k] === fname) || '';
    const deepLink = `https://t.me/${TESTER_BOT_USERNAME}?start=${encodeURIComponent(tokenKey)}`;
    const actId = addAction({ type: 'preview', fname, token: tokenKey, userId: null });
    kb.push([{ text: short(metaForBatch(fname).display_name || fname, 24), callback_data: `act_${actId}` }, { text: 'Open', url: deepLink }]);
  }
  kb.push([{ text: `Use /view <page> to see other pages`, callback_data: 'noop' }, { text: 'Close', callback_data: 'close' }]);

  return { text, reply_markup: { inline_keyboard: kb }, p, totalPages };
}

// ------------------ Recent uploads payload ------------------
function buildRecentPayload() {
  const idx = loadIndex();
  const recent = (idx.order || []).slice(-12).reverse();
  const lines = [];
  const kb = [];
  for (let i=0;i<recent.length;i++){
    const fname = recent[i];
    const meta = metaForBatch(fname);
    const tokenKey = Object.keys(idx.tokens || {}).find(k => idx.tokens[k] === fname) || '';
    const deepLink = `https://t.me/${TESTER_BOT_USERNAME}?start=${encodeURIComponent(tokenKey)}`;
    lines.push(`${i+1}. ${escapeHtml(meta.display_name)} — <code>${escapeHtml(tokenKey)}</code>`);
    const actId = addAction({ type: 'preview', fname, token: tokenKey, userId: null });
    kb.push([{ text: short(meta.display_name, 28), callback_data: `act_${actId}` }, { text: 'Open', url: deepLink }]);
  }
  if (lines.length === 0) lines.push('No recent uploads');
  kb.push([{ text: 'Close', callback_data: 'close' }]);
  const text = `Recent uploads — showing latest ${recent.length}\n\n` + lines.join('\n');
  return { text, reply_markup: { inline_keyboard: kb } };
}

function saveSuggestionToFile(suggestion) {
  try {
    const raw = fs.readFileSync(SUGGESTIONS_FILE, 'utf8') || '[]';
    const arr = JSON.parse(raw);
    const recentSame = arr.find(s =>
      ((s.type === suggestion.type) &&
       (s.type === 'token' ? s.token === suggestion.token : s.text === suggestion.text) &&
       s.from && s.from.id === suggestion.from.id &&
       (new Date() - new Date(s.ts) < 30 * 1000)
      )
    );
    if (!recentSame) {
      arr.push(suggestion);
      fs.writeFileSync(SUGGESTIONS_FILE, JSON.stringify(arr, null, 2), 'utf8');
    }
  } catch (e) { console.error('saveSuggestionToFile error', e && e.message); }
}

// ------------------ Callback handler ------------------
bot.on('callback_query', async (cb) => {
  const data = (cb && cb.data) || '';
  const chatId = cb.message && cb.message.chat && cb.message.chat.id;
  
  try {
    // --- Settings / Admin Callbacks ---
    if (data.startsWith('setting_')) {
      if (String(cb.from.id) !== String(ADMIN_USER_ID)) {
        return bot.answerCallbackQuery(cb.id, { text: 'Admin only', show_alert: true });
      }

      if (data === 'setting_toggle_delete') {
        botSettings.groupAutoDelete = !botSettings.groupAutoDelete;
        saveSettings();
      } 
      else if (data.startsWith('setting_time_')) {
        const seconds = parseInt(data.replace('setting_time_', ''), 10);
        if (!isNaN(seconds)) {
          botSettings.groupDeleteTimer = seconds;
          botSettings.groupAutoDelete = true; // Auto-enable if setting time
          saveSettings();
        }
      }

      // Refresh Menu
      const { text, reply_markup } = buildSettingsMenu();
      try {
        await bot.editMessageText(text, {
          chat_id: chatId,
          message_id: cb.message.message_id,
          reply_markup: reply_markup,
          parse_mode: 'HTML'
        });
      } catch(e) {} // ignore redundant edit
      await bot.answerCallbackQuery(cb.id, { text: 'Settings updated' });
      return;
    }

    // --- Standard Callbacks ---
    if (data === 'view_1') {
      const payload = buildViewPagePayload(0);
      try {
        await bot.editMessageText(payload.text, {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: payload.reply_markup,
          parse_mode: 'HTML'
        });
      } catch (e) {
        await bot.sendMessage(cb.message.chat.id, payload.text, { reply_markup: payload.reply_markup, parse_mode: 'HTML' });
      }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    if (data === 'recent') {
      const payload = buildRecentPayload();
      try {
        await bot.editMessageText(payload.text, {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: payload.reply_markup,
          parse_mode: 'HTML'
        });
      } catch (e) {
        await bot.sendMessage(cb.message.chat.id, payload.text, { reply_markup: payload.reply_markup, parse_mode: 'HTML' });
      }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

if (data === 'help') {
      if (cb.from) upsertUser(cb.from);
      
      const idxRaw = loadIndex();
      const orderPresent = Array.isArray(idxRaw._raw && idxRaw._raw.order) && idxRaw._raw.order.length > 0;
      
      const helpHtml = `<b>Search &amp; Browse Bot — Help</b>\n\n` +
        `<b>Quick start</b>\n` +
        `• Use <code>/view &lt;page&gt;</code> — view that page of the index (1-based). Example: <code>/view 3</code>.\n` +
        `• Use inline search: open any chat and type <code>@${escapeHtml(TESTER_BOT_USERNAME)} &lt;query&gt;</code> to search quickly.\n` +
        `• When you Preview an item you'll be asked: “Is this the file you requested?” — choose <b>Yes</b> to get the files, or <b>No</b> to report to admin.\n\n` +
        `<b>Notes on ordering</b>\n` +
        `The bot shows batches in the order defined in <code>index.order</code>. ${ orderPresent ? 'This matches the sending order.' : '<i>index.order not present — fallback ordering is used.</i>' }\n\n` +
        `<b>Common commands (users)</b>\n` +
        `• <code>/view &lt;page&gt;</code> — View indexed batches (page 1 is <code>/view 1</code>).\n` +
        `• <code>/listfiles</code> — alias to <code>/view 1</code>.\n` +
        `• During preview: confirm to receive files or report if wrong.\n\n` +
        `<b>Admin commands:</b>\n` +
        `• <code>/settings</code> — ⚙️ Configure Group Auto-Delete Timer.\n` +
        `• <code>/broadcast &lt;message&gt;</code> — Prepare a broadcast.\n` +
        `• <code>/clearcache</code> — Clear in-memory index caches.\n` +
        `• <code>/listuser [page]</code> — List known users.\n`;

      const kb = {
        inline_keyboard: [
          [{ text: '📚 View page 1', callback_data: 'view_1' }],
          [{ text: '🆕 Recent uploads', callback_data: 'recent' }, { text: '⚠️ Report issue', callback_data: 'report_issue' }],
          [{ text: '🔗 Open Tester bot', url: `https://t.me/${TESTER_BOT_USERNAME}` }]
        ]
      };

      try {
        await bot.editMessageText(helpHtml, {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: kb,
          parse_mode: 'HTML'
        });
      } catch (e) {
        await bot.sendMessage(cb.message.chat.id, helpHtml, { reply_markup: kb, parse_mode: 'HTML' });
      }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    if (data === 'report_issue') {
      const userIdStr = String(cb.from && cb.from.id);
      const fallbackChatId = (cb.message && cb.message.chat && cb.message.chat.id) || userIdStr;
      pendingReports[userIdStr] = { general: true, fromId: cb.from && cb.from.id, fallbackChatId, createdAt: Date.now() };
      setTimeout(() => { delete pendingReports[userIdStr]; }, 30 * 60 * 1000);
      const prompt = 'Please send the text describing the issue. Your message will be forwarded to the admin for review.';
      try {
        await bot.sendMessage(cb.from.id, prompt);
        await bot.answerCallbackQuery(cb.id, { text: 'Please send the issue description (I messaged you).' });
      } catch (e) {
        await bot.answerCallbackQuery(cb.id, { text: 'Please send the issue description in this chat.', show_alert: true });
        await bot.sendMessage(fallbackChatId, prompt);
      }
      return;
    }

    // Action map handling
    // Action map handling
    if (data.startsWith('act_')) {
      const id = data.slice(4);
      const action = actionMap[id];
      if (!action) { await bot.answerCallbackQuery(cb.id, { text: 'Action expired', show_alert: true }); return; }

      // --- Handling Suggestions via Action ID (Fixes BUTTON_DATA_INVALID) ---
      if (action.type === 'suggest_token' || action.type === 'suggest_text') {
        const isToken = (action.type === 'suggest_token');
        const val = isToken ? action.token : action.text;
        
        // Lookup title if it's a token
        let title = val;
        if (isToken) {
           const idx = loadIndex();
           const fname = (idx.tokens && idx.tokens[val]) || '';
           const meta = fname ? (metaForBatch(fname) || {}) : {};
           title = meta.display_name || fname || val;
        }

        const suggestion = {
          type: isToken ? 'token' : 'text',
          token: isToken ? val : undefined,
          text: isToken ? undefined : val,
          title: isToken ? title : undefined,
          from: { id: cb.from.id, username: cb.from.username, name: cb.from.first_name },
          ts: new Date().toISOString()
        };

        saveSuggestionToFile(suggestion);
        
        // Notify Admin
        const adminMsg = `📣 New suggestion (${isToken ? 'token' : 'text'})\n` +
                         `Val: ${title || val}\n` +
                         `From: ${suggestion.from.name} (@${suggestion.from.username || '-'})`;
        try { await bot.sendMessage(ADMIN_CHAT_ID, adminMsg); } catch(e) {}
        
        await bot.answerCallbackQuery(cb.id, { text: 'Thanks! Suggestion sent to admin.', show_alert: true });
        delete actionMap[id];
        return;
      }

      // --- Existing Preview/Send Logic ---
      if (action.type === 'preview') {
        const fname = action.fname;
        const token = action.token;
        const batch = readBatch(fname);
        if (!batch) { await bot.answerCallbackQuery(cb.id, { text: 'Preview not found', show_alert: true }); return; }
        try {
            await bot.sendMessage(cb.message.chat.id, `Preview — ${escapeHtml(batch.display_name || fname)}`, { parse_mode: 'HTML' });
        } catch (e) {}
        const sendAct = addAction({ type: 'send_files', fname, token, userId: cb.from && cb.from.id });
        const reportAct = addAction({ type: 'report_to_admin', fname, token, userId: cb.from && cb.from.id });
        const deepLink = `https://t.me/${TESTER_BOT_USERNAME}?start=${encodeURIComponent(token)}`;
        await bot.sendMessage(cb.message.chat.id, 'Is this the file you requested?', {
          reply_markup: {
            inline_keyboard: [
              [{ text: '✅ Yes — Send files', callback_data: `act_${sendAct}` }, { text: '❌ No — Report', callback_data: `act_${reportAct}` }],
              [{ text: 'Open in Tester bot', url: deepLink }, { text: 'Close', callback_data: 'close' }]
            ]
          }
        });
        await bot.answerCallbackQuery(cb.id);
        return;
      }

      if (action.type === 'send_files') {
        if (!cb.from || (String(action.userId) !== String(cb.from.id) && String(cb.from.id) !== String(ADMIN_USER_ID))) {
          await bot.answerCallbackQuery(cb.id, { text: 'Not authorized', show_alert: true });
          return;
        }
        const fname = action.fname;
        const batch = readBatch(fname);
        if (!batch) { await bot.answerCallbackQuery(cb.id, { text: 'Files missing', show_alert: true }); delete actionMap[id]; return; }
        for (let i=0;i<(batch.files||[]).length;i++){
          const f = batch.files[i];
          try {
            if (f.file_id) await bot.sendDocument(cb.from.id, f.file_id, { caption: f.caption || '' });
            else await bot.sendMessage(cb.from.id, f.caption || 'Item');
          } catch (e) {}
        }
        delete actionMap[id];
        await bot.answerCallbackQuery(cb.id, { text: 'Files sent' });
        return;
      }

      if (action.type === 'report_to_admin') {
        const userKey = String(cb.from && cb.from.id);
        pendingReports[userKey] = { fname: action.fname, token: action.token, createdAt: Date.now() };
        delete actionMap[id];
        await bot.sendMessage(cb.from.id, 'Please describe what you needed (it will be forwarded to admin).');
        await bot.answerCallbackQuery(cb.id);
        return;
      }
    }

    if (data === 'close') {
      try { await bot.deleteMessage(cb.message.chat.id, cb.message.message_id); } catch (e) {}
      return await bot.answerCallbackQuery(cb.id);
    }
    
// Tools Menu
    if (data === 'tools') {
      const toolsText = `<b>🛠️ Tools Menu</b>\n\nChoose a tool from the options below:`;
      const kb = {
        inline_keyboard: [
          [{ text: '🔗 Link Converter', callback_data: 'link_converter' }],
          [{ text: '🎬 Movie Text Converter', callback_data: 'tool_convertmovie' }],
          [{ text: '🔙 Back to Main Menu', callback_data: 'start_menu' }]
        ]
      };
      try {
        await bot.editMessageText(toolsText, {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: kb,
          parse_mode: 'HTML'
        });
      } catch (e) {
        await bot.sendMessage(cb.message.chat.id, toolsText, { reply_markup: kb, parse_mode: 'HTML' });
      }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    // Movie Converter Help Screen
    if (data === 'tool_convertmovie') {
      const msg = `<b>🎬 Movie Text Converter</b>\n\n` +
                  `This tool formats raw movie info text into a clean list.\n\n` +
                  `<b>How to use:</b>\n` +
                  `1. Find a message with messy movie details.\n` +
                  `2. Reply to it with <code>/cnt</code> (or <code>/convertmovie</code>).\n` +
                  `3. The bot will reply with the formatted text.`;
      
      const kb = {
        inline_keyboard: [
          [{ text: '🔙 Back to Tools', callback_data: 'tools' }]
        ]
      };
      
      try {
        await bot.editMessageText(msg, {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: kb,
          parse_mode: 'HTML'
        });
      } catch (e) {
        await bot.sendMessage(cb.message.chat.id, msg, { reply_markup: kb, parse_mode: 'HTML' });
      }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    if (data === 'start_menu') {
      const { menuText, kb } = startMenuFor(cb.from);
      try { await bot.editMessageText(menuText, { chat_id: cb.message.chat.id, message_id: cb.message.message_id, reply_markup: kb, parse_mode: 'HTML' }); } catch(e) { await bot.sendMessage(cb.message.chat.id, menuText, { reply_markup: kb, parse_mode: 'HTML' }); }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    if (data === 'link_converter') {
      const userId = String(cb.from && cb.from.id);
      const currentBotName = getUserBotName(userId) || null;
      const shown = currentBotName ? `@${currentBotName}` : 'not set';
      const lcText = `<b>Link Converter</b>\n\nRegistered bot: <code>${escapeHtml(shown)}</code>\nUse <code>/regbname &lt;bot_username&gt;</code> to change it.`;
      const kb = { inline_keyboard: [[{ text: '🔙 Back to Tools', callback_data: 'tools' }]] };
      try { await bot.editMessageText(lcText, { chat_id: cb.message.chat.id, message_id: cb.message.message_id, reply_markup: kb, parse_mode: 'HTML' }); } catch(e) { await bot.sendMessage(cb.message.chat.id, lcText, { reply_markup: kb, parse_mode: 'HTML' }); }
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    // Suggestions from search results
    if (data.startsWith('suggest_token:') || data.startsWith('suggest_text:')) {
      const isToken = data.startsWith('suggest_token:');
      const val = data.split(':')[1];
      const suggestion = {
        type: isToken ? 'token' : 'text',
        token: isToken ? val : undefined,
        text: isToken ? undefined : decodeURIComponent(val),
        from: { id: cb.from.id, username: cb.from.username, name: cb.from.first_name },
        ts: new Date().toISOString()
      };
      saveSuggestionToFile(suggestion);
      await bot.sendMessage(ADMIN_CHAT_ID, `📣 Suggestion (${suggestion.type})\nVal: ${suggestion.token || suggestion.text}\nFrom: ${suggestion.from.name}`);
      await bot.sendMessage(cb.message.chat.id, 'Thanks — suggestion sent to admin.');
      await bot.answerCallbackQuery(cb.id);
      return;
    }

    await bot.answerCallbackQuery(cb.id);
  } catch (err) {
    console.error('[search_bot] callback error', err && err.message);
  }
});

// ------------------ Admin: Settings Command ------------------
bot.onText(/\/settings/, async (msg) => {
  if (String(msg.from.id) !== String(ADMIN_USER_ID)) {
    return sendAutoDelete(msg.chat.id, '❌ You are not authorized to use this command.');
  }
  const { text, reply_markup } = buildSettingsMenu();
  return bot.sendMessage(msg.chat.id, text, { parse_mode: 'HTML', reply_markup });
});

// ------------------ View / Listfiles ------------------
bot.onText(/\/view(?:\s+(\d+))?/, async (msg, match) => {
  const page = Math.max(0, (match && match[1]) ? Number(match[1]) - 1 : 0);
  const payload = buildViewPagePayload(page);
  return sendAutoDelete(msg.chat.id, payload.text, { reply_markup: payload.reply_markup, parse_mode: 'HTML' });
});

bot.onText(/\/listfiles(?:\s+(\d+))?/, (msg, match) => {
  const page = Math.max(0, (match && match[1]) ? Number(match[1]) - 1 : 0);
  const payload = buildViewPagePayload(page);
  return sendAutoDelete(msg.chat.id, payload.text, { reply_markup: payload.reply_markup, parse_mode: 'HTML' });
});

// ------------------ Start / Help / User Info ------------------
function startMenuFor(user) {
  const name = user && (user.first_name || user.username) ? (user.first_name || user.username) : 'there';
  const menuText = `Hi ${escapeHtml(name)} 👋\n\nWelcome to the Search & Browse bot.`;
  const kb = {
    inline_keyboard: [
      [{ text: '📚 View index', callback_data: 'view_1' }, { text: '🆕 Recent', callback_data: 'recent' }],
      [{ text: '🛠️ Tools', callback_data: 'tools' }],
      [{ text: '❓ Help', callback_data: 'help' }, { text: '⚠️ Report', callback_data: 'report_issue' }]
    ]
  };
  return { menuText, kb };
}

bot.onText(/\/start(?:\s+(.+))?/, (msg, match) => {
  if (msg.from) upsertUser(msg.from);
  const payload = match && match[1] ? match[1].trim() : '';
  if (!payload) {
    const { menuText, kb } = startMenuFor(msg.from);
    return sendAutoDelete(msg.chat.id, menuText, { reply_markup: kb, parse_mode: 'HTML' });
  }
  const idx = loadIndex();
  const fname = idx.tokens && idx.tokens[payload];
  if (!fname) return sendAutoDelete(msg.chat.id, 'Token not found.');
  const deepLink = `https://t.me/${TESTER_BOT_USERNAME}?start=${encodeURIComponent(payload)}`;
  const sendAct = addAction({ type: 'send_files', fname, token: payload, userId: msg.from.id });
  const reportAct = addAction({ type: 'report_to_admin', fname, token: payload, userId: msg.from.id });
  const kb = { inline_keyboard: [
    [{ text: '✅ Yes — Send files', callback_data: `act_${sendAct}` }, { text: '❌ No — Report', callback_data: `act_${reportAct}` }],
    [{ text: 'Open in Tester bot', url: deepLink }, { text: 'Close', callback_data: 'close' }]
  ]};
  return sendAutoDelete(msg.chat.id, `Open: ${escapeHtml(fname)}`, { reply_markup: kb, parse_mode: 'HTML' });
});

bot.onText(/\/help/, async (msg) => {
  if (msg.from) upsertUser(msg.from);
  const idxRaw = loadIndex();
  const orderPresent = Array.isArray(idxRaw._raw && idxRaw._raw.order) && idxRaw._raw.order.length > 0;

  const helpHtml = `<b>Search &amp; Browse Bot — Help</b>\n\n` +
    `<b>Quick start</b>\n` +
    `• Use <code>/view &lt;page&gt;</code> — view that page of the index (1-based). Example: <code>/view 3</code>.\n` +
    `• Use inline search: open any chat and type <code>@${escapeHtml(TESTER_BOT_USERNAME)} &lt;query&gt;</code> to search quickly.\n` +
    `• When you Preview an item you'll be asked: “Is this the file you requested?” — choose <b>Yes</b> to get the files, or <b>No</b> to report to admin.\n\n` +
    `<b>Notes on ordering</b>\n` +
    `The bot shows batches in the order defined in <code>index.order</code>. ${ orderPresent ? 'This matches the sending order.' : '<i>index.order not present — fallback ordering is used.</i>' }\n\n` +
    `<b>Common commands (users)</b>\n` +
    `• <code>/view &lt;page&gt;</code> — View indexed batches (page 1 is <code>/view 1</code>).\n` +
    `• <code>/listfiles</code> — alias to <code>/view 1</code>.\n` +
    `• During preview: confirm to receive files or report if wrong.\n\n` +
    `<b>Admin commands:</b>\n` +
    `• <code>/settings</code> — ⚙️ Configure Group Auto-Delete Timer.\n` +
    `• <code>/broadcast &lt;message&gt;</code> — Prepare a broadcast.\n` +
    `• <code>/clearcache</code> — Clear in-memory index caches.\n` +
    `• <code>/listuser [page]</code> — List known users.\n`;

  const kb = {
    inline_keyboard: [
      [{ text: '📚 View page 1', callback_data: 'view_1' }],
      [{ text: '🆕 Recent uploads', callback_data: 'recent' }, { text: '⚠️ Report issue', callback_data: 'report_issue' }],
      [{ text: '🔗 Open Tester bot', url: `https://t.me/${TESTER_BOT_USERNAME}` }]
    ]
  };
  return sendAutoDelete(msg.chat.id, helpHtml, { reply_markup: kb, parse_mode: 'HTML' });
});

bot.onText(/\/clearcache/, (msg) => {
  if (String(msg.from.id) !== String(ADMIN_USER_ID)) return;
  clearCaches();
  return sendAutoDelete(msg.chat.id, 'Caches cleared.');
});

// ------------------ /cnt (Movie Converter) ------------------
bot.onText(/\/(?:convertmovie|cnt)(?:\s+([\s\S]+))?/, async (msg, match) => {
  if (msg.from) upsertUser(msg.from);

  const srcText =
    (msg.reply_to_message && (msg.reply_to_message.text || msg.reply_to_message.caption)) ||
    (match && match[1]) ||
    '';

  if (!srcText.trim()) {
    return sendAutoDelete(msg.chat.id,
      '<b>Usage:</b> Reply to a raw movie post with <code>/cnt</code>.',
      { parse_mode: 'HTML' }
    );
  }

  // Detect Type
  const isSeries = /TV\s*Series|Web\s*Series|Season|Ep(isode)?\s*\d/i.test(srcText);
  const typeLabel = isSeries ? '📺 TV Series' : '🎬 Movie';

  const lines = srcText.split('\n').map(l => l.trim()).filter(Boolean);

  const data = {
    title: '', rating: '', genre: '',
    language: '', story: '', quality: ''
  };

  // Helper: Aggressive cleaner
  const cleanVal = (str, labelRegex) => {
    let s = str.replace(labelRegex, '').trim();
    let prev;
    do {
      prev = s;
      s = s.replace(/^[\u200B\uFEFF\uFE0F\s:\-]+/u, '');
      s = s.replace(/^(Movie|Title|Film|Name|TV\s*Series|Rating|IMDb|Genre|Language|Audio|Story(\s*Line)?|Plot|Synopsis)[\s]*[:\-]+[\s]*/i, '');
    } while (s !== prev);
    return s.trim();
  };

  const startRegex = (k) => new RegExp(`^[\\u200B\\uFEFF\\uFE0F\\s]*(${k})`, 'i');
  const extractRegex = (k) => new RegExp(`^[\\u200B\\uFEFF\\uFE0F\\s]*(${k})[\\s\\S]*?[:\\-]`, 'i');

  // Parse lines
  for (const line of lines) {
    if (!data.title && startRegex('Movie|Title|Name|Film|TV\\s*Series').test(line)) {
      data.title = cleanVal(line, extractRegex('Movie|Title|Name|Film|TV\\s*Series'));
      continue;
    }
    if (!data.rating && startRegex('Rating|IMDb|Score').test(line)) {
      const raw = cleanVal(line, extractRegex('Rating|IMDb|Score'));
      const num = raw.match(/(\d+(\.\d+)?)/);
      data.rating = num ? (raw.includes('/') ? raw : `${num[1]} / 10`) : raw;
      continue;
    }
    if (!data.genre && startRegex('Genre|Category').test(line)) {
      data.genre = cleanVal(line, extractRegex('Genre|Category'));
      continue;
    }
    if (!data.language && startRegex('Language|Audio').test(line)) {
      data.language = cleanVal(line, extractRegex('Language|Audio'));
      continue;
    }
    if (!data.story && startRegex('Story|Plot|Synopsis|Description').test(line)) {
      data.story = cleanVal(line, extractRegex('Story|Plot|Synopsis|Description'));
      continue;
    }
    if (!data.quality && startRegex('Quality|Res|Resolution').test(line)) {
      data.quality = cleanVal(line, extractRegex('Quality|Res|Resolution'));
      continue;
    }
  }

  // Fallback title
  if (!data.title && lines.length > 0) {
    const first = lines[0];
    if (!first.startsWith('http') && first.length < 150) {
      data.title = cleanVal(first, extractRegex('Movie|Title|Name|Film'));
    }
  }

  // Build Output
  const out = [];

  if (data.title)    out.push(`<b>${typeLabel}: ${escapeHtml(data.title)}</b>`);
  if (data.rating)   out.push(`<b>⭐️ Rating: ${escapeHtml(data.rating)}</b>`);
  if (data.genre)    out.push(`<b>⚙ Genre: ${escapeHtml(data.genre)}</b>`);
  if (data.language) out.push(`<b>🗣 Language: ${escapeHtml(data.language)}</b>`);
  if (data.quality)  out.push(`<b>💿 Quality: ${escapeHtml(data.quality)}</b>`);
  
  if (data.story) {
    out.push(`<b><blockquote>📖 Story Line:${escapeHtml(data.story)}</blockquote></b>`);
  }

  if (out.length === 0) return sendAutoDelete(msg.chat.id, 'Could not parse movie details.');

  await sendAutoDelete(msg.chat.id, out.join('\n'), {
    parse_mode: 'HTML',
    disable_web_page_preview: true
  });
});

// ------------------ Message events (User Reports & Link Converter) ------------------
bot.on('message', async (msg) => {
  if (msg && msg.from && msg.from.is_bot) return;
  if (msg.from) upsertUser(msg.from);

  // remove join/left messages
  if (msg.new_chat_members || msg.left_chat_member) {
    try { await bot.deleteMessage(msg.chat.id, msg.message_id); } catch (e) {}
    return;
  }

  // Pending reports
  const pending = pendingReports[String(msg.from && msg.from.id)];
  if (pending && msg.text) {
    const user = msg.from;
    const forwardText = pending.general 
      ? `General report from ${user.first_name}\n\n${msg.text}`
      : `File report from ${user.first_name} (token: ${pending.token})\n\n${msg.text}`;
    try {
      await bot.sendMessage(ADMIN_USER_ID, forwardText);
      await bot.sendMessage(msg.chat.id, 'Report forwarded to admin.');
    } catch (e) {}
    delete pendingReports[String(msg.from.id)];
    return;
  }

  // Link Converter
  const registeredBotName = getUserBotName(String(msg.from.id));
  if (registeredBotName && msg.text && (msg.text.includes('/start') || msg.text.includes('t.me'))) {
    // simple heuristic: if it looks like a conversion request
    const lines = msg.text.split('\n');
    const out = [];
    let changed = false;
    for (const line of lines) {
      if (line.includes('/start') || line.includes('start=')) {
        // rough extraction logic
        const tokenMatch = line.match(/start[=_]([a-zA-Z0-9_-]+)/);
        if (tokenMatch) {
            const token = tokenMatch[1];
            const name = line.split(/https?:\/\//)[0].replace(/\/start.*/, '').trim() || 'Item';
            out.push(`${name} — <a href="https://t.me/${registeredBotName}?start=${token}">Open</a>`);
            changed = true;
            continue;
        }
      }
      out.push(line);
    }
    if (changed) {
      await bot.sendMessage(msg.chat.id, out.join('\n'), { parse_mode: 'HTML', disable_web_page_preview: true });
    }
  }
});

bot.onText(/\/regbname\s+(.+)/, (msg, match) => {
  const userId = String(msg.from.id);
  const botname = match[1].trim().replace(/^@/, '');
  setUserBotName(userId, botname);
  return sendAutoDelete(msg.chat.id, `Bot username set to: @${botname}`);
});

bot.on('polling_error', (err) => console.error('[search_bot] polling_error', err && err.message));
console.log('[search_bot] running — Admin: ' + ADMIN_USER_ID);