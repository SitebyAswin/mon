require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const { URL } = require('url');
const TelegramBot = require('node-telegram-bot-api');
const crypto = require('crypto');
const axios = require('axios');
const SUGGESTIONS_FILE = './suggestions.json';

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const USER_DIR = path.join(__dirname, 'userdata');
if (!fs.existsSync(USER_DIR)) fs.mkdirSync(USER_DIR, { recursive: true });

const INDEX_FILE = path.join(DATA_DIR, 'index.js');
const META_FILE = path.join(DATA_DIR, 'meta.json');
const TELEGRAPH_INDEX_FILE = path.join(DATA_DIR, 'telegraph_index.json'); // <— ADD THIS
const UPDATES_CHANNEL_URL = process.env.UPDATES_CHANNEL_URL || '';
const DISCUSSION_GROUP_URL = process.env.DISCUSSION_GROUP_URL || '';
const ADMIN_CONTACT_URL = process.env.ADMIN_CONTACT_URL || '';

const BOT_TOKEN = process.env.BOT_TOKEN;

const ADMIN_ID = process.env.ADMIN_ID ? Number(process.env.ADMIN_ID) : NaN;

if (!BOT_TOKEN) throw new Error('Please set BOT_TOKEN in .env');
if (!ADMIN_ID) console.warn('ADMIN_ID not set — admin-only checks will only warn.');

process.on('unhandledRejection', (r) => console.error('[UNHANDLED REJECTION]', r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT EXCEPTION]', e && e.stack ? e.stack : e));

// ---------- file helpers ----------
function atomicWriteFileSync(filepath, data) {
  const tmp = filepath + '.tmp';
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filepath);
}
function readIndex() {
  try { delete require.cache[require.resolve(INDEX_FILE)]; return require(INDEX_FILE); } catch { return { tokens: {}, order: [] }; }
}
function writeIndex(obj) { atomicWriteFileSync(INDEX_FILE, 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }
function readMeta() {
  try { return fs.existsSync(META_FILE) ? JSON.parse(fs.readFileSync(META_FILE)) : { batch_meta: {}, release_cache: {}, index_page_size: 8 }; } catch { return { batch_meta: {}, release_cache: {}, index_page_size: 8 }; }
}
function writeMeta(obj) { atomicWriteFileSync(META_FILE, JSON.stringify(obj, null, 2)); }

// --- Processing message helper (temporary status that disappears) ---
async function showProcessing(chatId, text = '⏳ Working...', opts = {}) {
  // Send the status message
  const m = await bot.sendMessage(chatId, text, { parse_mode: 'HTML', ...opts }).catch(() => null);
  const message_id = m && m.message_id;

  return {
    // update the text if you want
    async update(nextText) {
      if (!message_id) return;
      try {
        await bot.editMessageText(nextText, {
          chat_id: chatId,
          message_id,
          parse_mode: 'HTML'
        });
      } catch (_) {}
    },
    // delete it cleanly when done
    async done() {
      if (!message_id) return;
      try { await bot.deleteMessage(chatId, message_id); } catch (_) {}
    }
  };
}

// ---------- token & filenames ----------
function generateToken(len = 12) {
  const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  const rnd = crypto.randomBytes(len);
  let out = '';
  for (let i = 0; i < len; i++) out += CHARS[rnd[i] % CHARS.length];
  return out;
}
function sanitizeFilenameForDisk(name) {
  if (!name) return null;
  let s = String(name).trim();
  s = s.replace(/^[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}\s]+/u, '');
  s = s.replace(/^(?:🎬\s*)?(?:Movie|TV Series|TV|Series|Show|🎞️)\s*[:\-–—]\s*/i, '');
  s = s.replace(/\s*\[[0-9]{4}\]\s*$/,'');
  s = s.replace(/\s*[-–—]\s*[0-9]{4}\s*$/,'');
  s = s.replace(/[\u{1F300}-\u{1F9FF}\u2600-\u26FF\p{So}]/gu, '').trim();
  s = s.split(/\r?\n/)[0].trim();
  s = s.replace(/[^a-zA-Z0-9 \-_.]/g, '');
  s = s.replace(/\s+/g, ' ').trim();
  if (s.length > 60) s = s.slice(0,60).trim();
  if (!s) return null;
  return s;
}
function filenameToPath(filename) {
  const safe = filename.replace(/[^a-zA-Z0-9-_.]/g, '_');
  return path.join(DATA_DIR, safe + '.js');
}

function createBatchFile(filename, token, adminId) {
  const obj = { token, filename, adminId, createdAt: new Date().toISOString(), files: [], ratings: {}, display_name: filename };
  atomicWriteFileSync(filenameToPath(filename), 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n');
  return obj;
}
function readBatchFile(filename) {
  try { delete require.cache[require.resolve(filenameToPath(filename))]; return require(filenameToPath(filename)); } catch { return null; }
}
function writeBatchFile(filename, obj) { atomicWriteFileSync(filenameToPath(filename), 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }

function registerTokenInIndex(token, filename) {
  const idx = readIndex();
  if (!idx.tokens) idx.tokens = {};
  idx.tokens[token] = filename;
  if (!idx.order) idx.order = [];
  if (!idx.order.includes(filename)) idx.order.push(filename);
  writeIndex(idx);
}

function renameBatchFileOnDisk(oldFilename, newFilenameBase, token, displayNameFull) {
  try {
    const oldPath = filenameToPath(oldFilename);
    let finalNewFilename = newFilenameBase ? `${newFilenameBase}_${token}` : `batch_${token}`;
    let finalNewPath = filenameToPath(finalNewFilename);
    let suffix = 1;
    while (fs.existsSync(finalNewPath)) {
      finalNewFilename = `${newFilenameBase}_${token}_${suffix}`;
      finalNewPath = filenameToPath(finalNewFilename);
      suffix++;
    }
    const batch = readBatchFile(oldFilename);
    if (!batch) return null;
    batch.filename = finalNewFilename;
    batch.display_name = displayNameFull ? String(displayNameFull).trim().slice(0,200) : finalNewFilename;
    atomicWriteFileSync(finalNewPath, 'module.exports = ' + JSON.stringify(batch, null, 2) + ';\n');
    try { if (fs.existsSync(oldPath)) fs.unlinkSync(oldPath); } catch (e) { console.warn('unlink failed', e && e.message); }
    const idx = readIndex();
    if (!idx.tokens) idx.tokens = {};
    if (token) idx.tokens[token] = finalNewFilename;
    if (!idx.order) idx.order = [];
    const pos = idx.order.indexOf(oldFilename);
    if (pos !== -1) idx.order[pos] = finalNewFilename;
    else idx.order.push(finalNewFilename);
    writeIndex(idx);
    return finalNewFilename;
  } catch (e) { console.warn('renameBatchFileOnDisk failed', e && e.message); return null; }
}

// ---------- pending flows (admin) ----------
const pendingBatches = {}; // chatId -> pending add/new batch state
const pendingAddTo = {}; // chatId -> { token, filename, files: [] } when admin uses /addto

function startPendingBatch(adminChatId, filename) {
  const token = generateToken();
  const initialFilename = filename && String(filename).trim().length > 0 ? filename.trim() : (`batch_${token}`);
  pendingBatches[adminChatId] = { filename: initialFilename, token, files: [], createdAt: new Date().toISOString(), autoNamed: !filename || String(filename).trim().length===0 };
  createBatchFile(initialFilename, token, adminChatId);
  registerTokenInIndex(token, initialFilename);
  return pendingBatches[adminChatId];
}
function startPendingAddTo(adminChatId, token) {
  const idx = readIndex();
  const filename = idx.tokens && idx.tokens[token];
  if (!filename) return null;
  pendingAddTo[adminChatId] = { token, filename, files: [] };
  return pendingAddTo[adminChatId];
}

// ---------- bot startup ----------
const bot = new TelegramBot(BOT_TOKEN, { polling: true, filepath: true });
let BOT_USERNAME = null; // Declare BOT_USERNAME here
(async () => {
  try {
    const me = await bot.getMe();
    BOT_USERNAME = me && me.username ? me.username : null;
    console.log('Bot username:', BOT_USERNAME);
  } catch (e) {
    console.warn('Could not get bot username', e && e.message);
  }
})();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------- formatting helpers ----------
// NOTE: This function outputs HTML.
// You MUST set parse_mode: "HTML" when using its output with safeSendMessage.
function escapeHtml(s) {
  if (s === undefined || s === null) return '';
  // Telegram's HTML parser is quite tolerant, but basic escaping for safety is good.
  // We need to escape &, <, > that are not part of our intended tags.
  // Also need to be careful with ' and " if they are not inside attributes
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    // Telegram's HTML parser is usually fine with ' and ", but escaping them is safest if they might appear in text
    // .replace(/'/g, '&#039;')
    // .replace(/"/g, '&quot;');
}

function formatCaptionHtmlForPreview(rawCaption) {
  if (rawCaption === undefined || rawCaption === null) return '';

  let finalHtmlParts = [];
  const lines = String(rawCaption).split('\n'); // Original newlines from raw caption

  let inStoryLineBlock = false;
  let currentBlockquoteContent = []; // Buffer to collect lines for the current <blockquote>

  // Helper to flush current blockquote buffer if not empty
  const flushBlockquote = () => {
    if (currentBlockquoteContent.length > 0) {
      // Join blockquote lines with actual newline characters \n
      // Telegram's HTML parser will treat these as newlines within the <blockquote> block
      finalHtmlParts.push(`<blockquote>${currentBlockquoteContent.join('\n')}</blockquote>`);
      currentBlockquoteContent = []; // Reset buffer
    }
  };

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i]; // DO NOT trim here, as leading spaces might be part of the text
                         // and we're handling newlines separately.
                         // Let's re-evaluate trimming for individual lines. Telegram usually
                         // handles leading/trailing whitespace around entities well.
                         // For consistency with original, let's keep trim() for parsing logic.
    let trimmedLine = line.trim();

    if (trimmedLine.startsWith('📖 Story Line:')) {
      // Flush previous blockquote if any
      flushBlockquote();

      inStoryLineBlock = true;
      // The "📖 Story Line:" itself should be bold and start the blockquote content
      currentBlockquoteContent.push(`<b>${escapeHtml(line)}</b>`); // Use original line here to preserve leading/trailing spaces if any
    } else if (inStoryLineBlock) {
      // Content within the story line block
      if (line === '') { // An empty line in raw input should be an empty line in output
          currentBlockquoteContent.push('');
      } else {
          currentBlockquoteContent.push(`<b>${escapeHtml(line)}</b>`); // Use original line
      }
    } else {
      // If we were in a story line block and now are not, flush it.
      flushBlockquote();

      // All other lines (not part of the story line block) should just be bold
      // and directly added to finalHtmlParts.
      if (line !== '') {
        finalHtmlParts.push(`<b>${escapeHtml(line)}</b>`); // Use original line
      } else {
        finalHtmlParts.push(''); // Preserve empty lines for \n separation
      }
      inStoryLineBlock = false; // Ensure we're explicitly out of a story block
    }
  }

  // Ensure any remaining blockquote content is flushed at the very end of the caption
  flushBlockquote();

  // Finally, join all parts with actual newline characters `\n`.
  // Telegram's HTML parser will treat these `\n` as line breaks.
  let finalCaptionHtml = finalHtmlParts.join('\n');

  return finalCaptionHtml;
}

// And somewhere else in your code, where you were calling this function:
//
// const caption = formatCaptionHtmlForPreview(someRawText);
// await safeSendMessage(chatId, caption, { parse_mode: "HTML" }); // IMPORTANT: parse_mode: "HTML"

// ---------- detection ----------
async function detectNameFromFile(fileMeta) {
  try {
    if (fileMeta && fileMeta.caption) {
      const firstLine = String(fileMeta.caption).split(/\r?\n/).map(l=>l.trim()).find(l=>l && l.length>0);
      if (firstLine) {
        const raw = firstLine.trim().slice(0,200);
        const sanitized = sanitizeFilenameForDisk(raw) || null;
        if (sanitized) return { rawLine: raw, sanitized };
        return { rawLine: raw, sanitized: null };
      }
    }
    const fn = (fileMeta.file_name||'').toLowerCase();
    const mime = (fileMeta.mime_type||'').toLowerCase();
    const textLike = mime.startsWith('text/') || /\.(txt|nfo|srt|ass|sub|md|csv|log)$/i.test(fn);
    if (fileMeta.file_id && textLike) {
      try {
        const fileUrl = await bot.getFileLink(fileMeta.file_id);
        const firstChunk = await new Promise((resolve, reject) => {
          let got = '';
          const req = https.get(fileUrl, (res) => {
            res.setTimeout(5000);
            res.on('data', (d) => {
              try { got += d.toString('utf8'); } catch (e) {}
              if (got.length > 8192) { req.destroy(); resolve(got.slice(0,8192)); }
            });
            res.on('end', () => resolve(got));
          });
          req.on('error', (err) => reject(err));
          req.on('timeout', () => { req.destroy(); resolve(got); });
        });
        if (firstChunk && firstChunk.length>0) {
          const lines = firstChunk.split(/\r?\n/).map(l=>l.trim()).filter(l=>l && l.length>0);
          if (lines.length>0) {
            const raw = lines[0].trim().slice(0,200);
            const sanitized = sanitizeFilenameForDisk(raw) || null;
            if (sanitized) return { rawLine: raw, sanitized };
            return { rawLine: raw, sanitized: null };
          }
        }
      } catch (e) { console.warn('detectNameFromFile: read failed', e && e.message); }
    }
    if (fileMeta && fileMeta.file_name) {
      const base = String(fileMeta.file_name).replace(/\.[^/.]+$/, '');
      const raw = base.trim().slice(0,200);
      const sanitized = sanitizeFilenameForDisk(raw) || null;
      if (sanitized) return { rawLine: raw, sanitized };
      return { rawLine: raw, sanitized: null };
    }
    return null;
  } catch (e) { console.warn('detectNameFromFile error', e && e.message); return null; }
}

// ---------- add file to pending batch ----------
async function addFileToPending(adminChatId, fileMeta) {
  const cur = pendingBatches[adminChatId];
  if (!cur) return null;
  cur.files.push(fileMeta);
  let batch = readBatchFile(cur.filename) || createBatchFile(cur.filename, cur.token, adminChatId);
  batch.files.push(fileMeta);
  writeBatchFile(cur.filename, batch);

  if (cur.autoNamed && cur.files.length === 1) {
    try {
      const detected = await detectNameFromFile(fileMeta);
      if (detected) {
        const raw = detected.rawLine || null;
        const sanitized = detected.sanitized || null;
        const token = cur.token;
        const newBase = sanitized || (`batch`);
        const finalName = renameBatchFileOnDisk(cur.filename, newBase, token, raw || newBase);
        if (finalName) {
          cur.filename = finalName;
          batch = readBatchFile(finalName);
        }
      }
    } catch (e) { console.warn('auto detect failed', e && e.message); }
    cur.autoNamed = false;
  }
  return batch;
}

// ---------- add file to existing batch (admin) ----------
async function addFileToExistingBatch(adminChatId, token, fileMeta) {
  const idx = readIndex();
  const filename = idx.tokens && idx.tokens[token];
  if (!filename) return null;
  const batch = readBatchFile(filename);
  if (!batch) return null;
  batch.files.push(fileMeta);
  writeBatchFile(filename, batch);
  return batch;
}

// ---------- send helpers ----------
async function attemptSendWithRetry(fn) {
  try { return await fn(); } catch (err) {
    const transient = err && (err.code==='ECONNRESET' || (err.message && err.message.includes('ECONNRESET')));
    if (transient) { await sleep(500); return await fn(); }
    throw err;
  }
}
async function sendBatchItemToChat(chatId, batch, f) {
  try {
    const captionHtml = f.caption ? formatCaptionHtmlForPreview(f.caption) : null;
    const opts = captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {};
    if (f.type === 'text' && f.text) return await attemptSendWithRetry(() => bot.sendMessage(chatId, captionHtml ? captionHtml : (f.text||''), captionHtml ? { parse_mode: 'HTML' } : {}));
    if (f.type === 'document' && f.file_id) return await attemptSendWithRetry(() => bot.sendDocument(chatId, f.file_id, opts));
    if (f.type === 'photo' && f.file_id) return await attemptSendWithRetry(() => bot.sendPhoto(chatId, f.file_id, opts));
    if (f.type === 'video' && f.file_id) return await attemptSendWithRetry(() => bot.sendVideo(chatId, f.file_id, opts));
    if (f.type === 'audio' && f.file_id) return await attemptSendWithRetry(() => bot.sendAudio(chatId, f.file_id, opts));
    if (f.type === 'forward' && f.source_chat_id && f.source_message_id) {
      try { return await attemptSendWithRetry(() => bot.copyMessage(chatId, f.source_chat_id, f.source_message_id)); }
      catch (copyErr) {
        console.warn('copyMessage failed', copyErr && (copyErr.response && copyErr.response.body ? copyErr.response.body : copyErr.message));
        if (f.file_id) {
          try {
            if (f.mime_type && f.mime_type.startsWith('video')) return await bot.sendVideo(chatId, f.file_id, opts);
            if (f.mime_type && f.mime_type.startsWith('audio')) return await bot.sendAudio(chatId, f.file_id, opts);
            if (f.file_name && /\.(jpg|jpeg|png|gif|webp)$/i.test(f.file_name)) return await bot.sendPhoto(chatId, f.file_id, opts);
            return await bot.sendDocument(chatId, f.file_id, opts);
          } catch (fallbackErr) { console.warn('fallback failed', fallbackErr); await safeSendMessage(chatId, '⚠️ One item could not be delivered (fallback failed).'); return; }
        } else {
          await safeSendMessage(chatId, '⚠️ One item could not be retrieved from source. It may be private/deleted.');
          if (ADMIN_ID) try { await safeSendMessage(ADMIN_ID, `Failed to copy message for token ${batch.token} — source:${f.source_chat_id}, msg:${f.source_message_id}`); } catch(_) {}
          return;
        }
      }
    }
    const sent = await sendBatchItemToChat(chatId, batch, batch.files[i]);
    await bot.sendChatAction(chatId, firstFile.type === 'photo' ? 'upload_photo' : 'upload_document').catch(()=>{});
    await sendBatchItemToChat(chatId, batch, firstFile);
    if (sent && sent.message_id) sentMessageIds.push(sent.message_id);
    await safeSendMessage(chatId, 'Unsupported file type or metadata missing.');
  } catch (e) { console.warn('send file fail', e && (e.response && e.response.body ? e.response.body : e.message)); }
}

/// --- REPLACE the old scheduleDeletionForMessages with this one ---
async function scheduleDeletionForMessages(chatId, messageIds, seconds, batchToken, batchDisplayName) {
  if (!Array.isArray(messageIds) || messageIds.length === 0) return;
  const ms = Number(seconds) * 1000;
  if (!ms || ms <= 0) return;

  // Local escape helper (reuse global escapeMarkdownV2 if present)
  const escapeMd = (text) => {
    if (typeof escapeMarkdownV2 === 'function') {
      return escapeMarkdownV2(text);
    }
    if (text === null || text === undefined) text = '';
    text = String(text);
    // Escape all characters that are special in MarkdownV2
    return text.replace(/[_*[\]()~`>#+\-=|{}.!]/g, '\\$&');
  };

  // Calculate minutes and ensure it's escaped for MarkdownV2
  const minutesRaw = (ms / 60000).toFixed(1);
  const escapedMinutes = escapeMd(minutesRaw);

  const escapedBatchToken  = batchToken ? escapeMd(batchToken) : '';
  const escapedBatchName   = batchDisplayName ? escapeMd(batchDisplayName) : '';
  const batchLinePre       = escapedBatchName ? `*Batch:* ${escapedBatchName}\n\n` : '';
  const batchLinePost      = batchLinePre; // same line for post-deletion

  let noticeMessageId = null; // To store the message ID of the immediate notice

  // 1. Send an IMMEDIATE warning about impending deletion
  try {
    // Pre-deletion re-access part
    const preDeletionReaccessPart = escapedBatchToken
      ? `_You can reaccess this batch by clicking_ \\/start\\_${escapedBatchToken}`
      : '_If you want to keep these files, please save them to your \\"Saved Messages\\" or \\"Download\\" them now\\._';

    const sentNotice = await safeSendMessage(
      chatId,
      `*⚠️ IMPORTANT NOTICE:*\n\n` +
      batchLinePre +
      `Your uploaded files will be automatically deleted in *${escapedMinutes} minutes* due to our copyright and data retention policies\\.\n\n` +
      `> Please ensure you save them to your \\"Saved Messages\\" or \\"Download\\" them immediately if you wish to retain them\\.\n\n` +
      `Thank you for your understanding\\.\n\n` +
      `${preDeletionReaccessPart}`,
      { parse_mode: 'MarkdownV2' }
    );

    if (sentNotice && sentNotice.message_id) {
      noticeMessageId = sentNotice.message_id;
    }
  } catch (error) {
    console.error('Failed to send immediate pre-deletion notice:', error);
  }

  if (noticeMessageId) {
    messageIds.push(noticeMessageId);
  }

  // 2. Delete the messages after the delay and send a post-deletion notice
  setTimeout(async () => {
    let deletedCount = 0;
    for (const mid of messageIds) {
      try {
        await bot.deleteMessage(chatId, mid);
        deletedCount++;
      } catch (e) {
        // Optional: debug log
        // console.warn(`Failed to delete message ${mid} in chat ${chatId}: ${e.message}`);
      }
    }

    if (deletedCount > 0) {
      // Post-deletion re-access part
      const postDeletionReaccessPart = escapedBatchToken
        ? (escapedBatchName
            ? `_You can reaccess *${escapedBatchName}* by clicking_ \\/start\\_${escapedBatchToken}`
            : `_You can reaccess this batch by clicking_ \\/start\\_${escapedBatchToken}`)
        : '_If you need to view files again, you can use the \\/start command with its token, or re-upload them\\._';

      const postDeletionNotice =
        `✅ Your file\\(s\\) have been deleted after *${escapedMinutes} minutes* as per our copyright policy\\.\n\n` +
        `> Reason: To avoid copyright issues and ensure bot operation\\.\n\n` +
        batchLinePost + // e.g. "*Batch:* Name"
        `If you need the file again, ${postDeletionReaccessPart}\n\n` +
        `Thank you for understanding\\.`;      

      try {
        await safeSendMessage(chatId, postDeletionNotice, { parse_mode: 'MarkdownV2' });
      } catch (error) {
        console.error('Failed to send post-deletion notice:', error);
      }
    }
  }, ms);
}

// ---------- browse helpers ----------
const browseSessions = {};
function makeBrowseKeyboardForIndex(pos, total, token) {
  const left = { text: '◀️Prev', callback_data: 'browse_left' };
  const view = { text: '🔲 Show files', callback_data: 'browse_view' };
  const right = { text: 'Next▶️', callback_data: 'browse_right' };
  const random = { text: '🎲 Random', callback_data: 'browse_random' };
  const viewList = { text: '📃 View as list', callback_data: 'browse_list' };
  const viewIndex = { text: '🗂️ View index', callback_data: 'view_index' };
  const goto = { text: '🔢 Go to page', callback_data: 'browse_goto' }; // NEW
  return { inline_keyboard: [[left, view, right], [random, viewList, goto], [viewIndex]] };
}
function buildFilesKeyboardForBatch(token, batch, asAdmin = false) {
  const buttons = [];
  for (let i = 0; i < (batch.files||[]).length; i++) {
    const small = batch.files[i].file_name ? (' — ' + batch.files[i].file_name.slice(0,20)) : '';
    buttons.push({ text: `${i+1}${small}`, callback_data: `browse_file_${token}_${i}` });
  }
  const rows = [];
  for (let i = 0; i < buttons.length; i += 3) rows.push(buttons.slice(i, i+3));
  if (asAdmin) {
    for (let i=0;i<(batch.files||[]).length;i++) {
      const up = { text: '🔼', callback_data: `file_up_${token}_${i}` };
      const down = { text: '🔽', callback_data: `file_down_${token}_${i}` };
      rows.push([ { text: `Edit #${i+1}`, callback_data: `file_edit_${token}_${i}` }, up, down ]);
    }
  }
  rows.push([{ text: 'Close', callback_data: 'browse_files_close' }]);
  return { inline_keyboard: rows };
}
function buildListViewForBatch(token, batch, asAdmin = false) {
  const lines = [];
  for (let i = 0; i < (batch.files||[]).length; i++) {
    const f = batch.files[i];
    const title = (f.caption || f.text || f.file_name || '').split(/\r?\n/)[0] || `File ${i+1}`;
    const short = escapeHtml(String(title).slice(0, 200));
    lines.push(`${i+1}. ${short}`);
  }
  const text = `<b>${escapeHtml(batch.display_name || batch.filename)}</b>\n\n` + lines.join('\n');
  const kb = buildFilesKeyboardForBatch(token, batch, asAdmin);
  kb.inline_keyboard.push([{ text: '🔙 Back to preview', callback_data: 'browse_back_to_preview' }]);
  return { text, keyboard: kb };
}

async function replaceBrowseMessage(chatId, oldMessageId, fileObj, captionHtml) {
  try {
    if (fileObj.type === 'photo' && fileObj.file_id) {
      try {
        await bot.editMessageMedia({ type: 'photo', media: fileObj.file_id }, { chat_id: chatId, message_id: oldMessageId });
        if (captionHtml) try { await bot.editMessageCaption(captionHtml, { chat_id: chatId, message_id: oldMessageId, parse_mode: 'HTML' }); } catch (_) {}
        return { edited: true, message_id: oldMessageId };
      } catch (e) {}
    }
    let newMsg;
    const opts = captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {};
    if (fileObj.type === 'document' && fileObj.file_id) newMsg = await bot.sendDocument(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'photo' && fileObj.file_id) newMsg = await bot.sendPhoto(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'video' && fileObj.file_id) newMsg = await bot.sendVideo(chatId, fileObj.file_id, opts);
    else if (fileObj.type === 'text' && fileObj.text) newMsg = await bot.sendMessage(chatId, captionHtml ? captionHtml : fileObj.text, captionHtml ? { parse_mode: 'HTML' } : {});
    else if (fileObj.type === 'forward' && fileObj.source_chat_id && fileObj.source_message_id) {
      try { newMsg = await bot.copyMessage(chatId, fileObj.source_chat_id, fileObj.source_message_id); } catch (err) {
        if (fileObj.file_id) newMsg = await bot.sendDocument(chatId, fileObj.file_id, opts);
        else newMsg = await bot.sendMessage(chatId, captionHtml || 'Item unavailable', captionHtml ? { parse_mode: 'HTML' } : {});
      }
    } else newMsg = await bot.sendMessage(chatId, captionHtml || 'Item', captionHtml ? { parse_mode: 'HTML' } : {});
    try { await bot.deleteMessage(chatId, oldMessageId); } catch (_) {}
    return { edited: false, newMessage: newMsg };
  } catch (e) { console.warn('replaceBrowseMessage failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; }
}

// --------- Simple Channel Message Forward Browser ---------
// Note: new third param `opts` — pass { force: true } to override hidden check.
async function sendChannelForwardByIndex(chatId, index, opts = {}) {
  const force = !!(opts && opts.force);
  const meta = getMsgMeta();
  const items = meta.channel_forwards || [];
  const total = items.length;

  if (!total) {
    return safeSendMessage(chatId, 'No channel messages configured yet.');
  }
  if (index < 0 || index >= total) {
    return safeSendMessage(chatId, 'Invalid page.');
  }

  const item = items[index];

  // If the item is marked hidden and caller didn't request a force-show, do not display it.
  if (item && item.hidden && !force) {
    // Reply with a neutral message — keep the deep-link working (force=true will bypass).
    return safeSendMessage(chatId, 'This channel message is currently hidden. Use the shared link to open it.');
  }

  const kb = {
    inline_keyboard: [
      [
        { text: '◀️ Prev',  callback_data: `chmsg_prev|${index}` },
        { text: '❌ Close', callback_data: `chmsg_close|${index}` },
        { text: 'Next ▶️', callback_data: `chmsg_next|${index}` }
      ],
      [
        { text: '🏠 Home', callback_data: 'home_open_main' }
      ]
    ]
  };

  try {
    return await bot.copyMessage(
      chatId,
      item.source_chat_id,
      item.source_message_id,
      { reply_markup: kb }
    );
  } catch (e) {
    console.error('sendChannelForwardByIndex failed', e && (e.response && e.response.body ? e.response.body : e.message));
    return safeSendMessage(
      chatId,
      'Failed to fetch that channel message. Make sure the bot is in the channel and the link is valid.'
    );
  }
}

// ---------- per-user tracking ----------
function userFilePath(userId) { return path.join(USER_DIR, `${userId}.js`); }
function readUserData(userId) {
  const p = userFilePath(userId);
  try { delete require.cache[require.resolve(p)]; return require(p); } catch { return { id: userId, username: null, first_name: null, last_name: null, actions: [] }; }
}
function writeUserData(userId, obj) { const p = userFilePath(userId); atomicWriteFileSync(p, 'module.exports = ' + JSON.stringify(obj, null, 2) + ';\n'); }
function recordUserAction(user, action) {
  try {
    const uid = user.id;
    const obj = readUserData(uid);
    obj.id = uid;
    if (user.username) obj.username = user.username;
    if (user.first_name) obj.first_name = user.first_name;
    if (user.last_name) obj.last_name = user.last_name;
    obj.actions = obj.actions || [];
    obj.actions.push(Object.assign({ ts: new Date().toISOString() }, action));
    if (obj.actions.length > 200) obj.actions = obj.actions.slice(obj.actions.length - 200);
    writeUserData(uid, obj);
  } catch (e) { console.warn('recordUserAction failed', e && e.message); }
}

// ---------- index builder (quick, uses cached meta) ----------
// global in-memory lookup for callbacks (keeps callback_data short)
const __callbackTokenMap = {}; // key -> { token, display, createdAt }

// generate a short stable key
function makeCbKey() {
  return 'k' + Math.random().toString(36).slice(2, 9);
}

// cleanup old keys every 10 minutes (keys older than 30 min)
setInterval(() => {
  const cutoff = Date.now() - 30 * 60 * 1000;
  for (const k of Object.keys(__callbackTokenMap)) {
    if (!__callbackTokenMap[k] || __callbackTokenMap[k].createdAt < cutoff) delete __callbackTokenMap[k];
  }
}, 10 * 60 * 1000);

// Escaper for MarkdownV2
function escapeMarkdownV2(s = '') {
  return String(s)
    .replace(/\\/g, '\\\\')
    .replace(/_/g, '\\_')
    .replace(/\*/g, '\\*')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)')
    .replace(/~/g, '\\~')
    .replace(/`/g, '\\`')
    .replace(/>/g, '\\>')
    .replace(/#/g, '\\#')
    .replace(/\+/g, '\\+')
    .replace(/-/g, '\\-')
    .replace(/=/g, '\\=')
    .replace(/\|/g, '\\|')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\./g, '\\.')
    .replace(/!/g, '\\!');
}

// Small label shortener for button text
function shortLabel(text, max = 32) {
  if (!text) return '';
  const s = String(text).trim();
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

// ---------- index builder (quick, uses cached meta) ----------
// View-only index builder (plain text for index, inline keyboard for navigation)
function buildIndexTextAndKeyboardQuick(page = 0, _requesterIsAdmin = false) {
  const idx = readIndex();
  const meta = readMeta();
  const order = Array.isArray(idx.order) ? idx.order : [];
  const total = order.length;
  const pageSize = 10;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const p = Math.min(Math.max(0, page), totalPages - 1);
  const start = p * pageSize;
  const end = Math.min(start + pageSize, total);

  const lines = [];
  const itemKeyboardRows = [];

  for (let i = start; i < end; i++) {
    const fname = order[i];
    const n = i + 1;
    const batch = readBatchFile(fname) || {};
    const display = batch.display_name || batch.filename || fname || 'Untitled';
    const cleanedDisplay = String(display).replace(/\s+/g, ' ').trim();

    const token = Object.keys(idx.tokens || {}).find(t => idx.tokens[t] === fname) || '';

    // Text line
    lines.push(`${n}. ${cleanedDisplay}`);

    if (token) {
      const key = makeCbKey();
      __callbackTokenMap[key] = { token: String(token), display: cleanedDisplay, createdAt: Date.now() };

      const openLabel = shortLabel(cleanedDisplay, 28);
      const encodedToken = encodeURIComponent(String(token));
      const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME)
        ? `https://t.me/${BOT_USERNAME}?start=${encodedToken}`
        : `https://t.me/?start=${encodedToken}`;

      const copyCb = `copytoken|${key}`;

      itemKeyboardRows.push([
        { text: `🔗 ${openLabel}`, url: openUrl },
        { text: '🔐 Token', callback_data: copyCb }
      ]);
    } else {
      itemKeyboardRows.push([
        { text: `${n}. ${shortLabel(cleanedDisplay, 40)}`, callback_data: 'noop' }
      ]);
    }
  }

  // Pagination buttons
  const keyboardRows = [];
  const navRowTop = [];
  if (p > 0) navRowTop.push({ text: '⬅️ Prev', callback_data: `index_prev_${p - 1}` });
  navRowTop.push({ text: `Page ${p + 1}/${totalPages}`, callback_data: `index_page_${p}` });
  if (p < totalPages - 1) navRowTop.push({ text: 'Next ➡️', callback_data: `index_next_${p + 1}` });

  if (navRowTop.length) keyboardRows.push(navRowTop);
  for (const r of itemKeyboardRows) keyboardRows.push(r);
  if (navRowTop.length) keyboardRows.push(navRowTop);

  const headerTitle = 'FILE INDEX : Click on the buttons to view files or token';
  const rangeText = `SHOWING ${start + 1} – ${Math.min(start + pageSize, total)} OF ${total}`;

  const text = [headerTitle, rangeText, ...(lines.length ? lines : ['NO ITEMS FOUND'])].join('\n');

  return {
    text,
    keyboard: { inline_keyboard: keyboardRows },
    page: p,
    totalPages,
    pageSize
  };
}

// ---------- small wrappers ----------
async function safeSendMessage(chatId, text, opts = {}) { try { return await bot.sendMessage(chatId, String(text || ''), opts); } catch (e) { console.warn('safeSendMessage failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }
async function safeAnswerCallbackQuery(id, opts = {}) { try { return await bot.answerCallbackQuery(id, opts); } catch (e) { console.warn('safeAnswerCallbackQuery failed', e && (e.response && e.response.body ? e.response.body : e.message)); return null; } }
// Helper: send long HTML text in safe chunks (Telegram limit ≈ 4096 chars)
async function sendLongHtmlMessage(chatId, htmlText, extraOpts = {}) {
  const MAX_LEN = 3800; // keep a safety margin below 4096
  const text = String(htmlText || '');
  const opts = Object.assign({ parse_mode: 'HTML' }, extraOpts);

  if (text.length <= MAX_LEN) {
    return safeSendMessage(chatId, text, opts);
  }

  const lines = text.split('\n');
  let chunk = '';
  let first = true;

  for (const line of lines) {
    const candidate = (chunk ? chunk + '\n' : '') + line;

    if (candidate.length > MAX_LEN) {
      if (chunk) {
        await safeSendMessage(chatId, chunk, opts);
      }
      chunk = line; // start new chunk
      first = false;
    } else {
      chunk = candidate;
    }
  }

  if (chunk) {
    await safeSendMessage(chatId, chunk, opts);
  }
}

// Build compact nodes for a list of batches
function buildBatchListNodes(batches, botUser) {
  const content = [];
  for (let i = 0; i < batches.length; i++) {
    const b = batches[i];
    const title = b.title || b.display_name || b.filename || ('Batch ' + (b.token || ''));
    const link = botUser ? `https://t.me/${botUser}?start=${encodeURIComponent(b.token)}` : null;
    content.push({
      tag: 'p',
      children: [
        { tag: 'strong', children: [ `${i + 1}. ${title}` ] }
      ]
    });
    const metaChildren = [{ tag: 'code', children: [ b.token ] }];
    if (link) { metaChildren.push(' — '); metaChildren.push({ tag: 'a', attrs: { href: link }, children: ['Open'] }); }
    content.push({ tag: 'p', children: metaChildren });
    const arr = (b.files || b.items || b.media || b.links || b.entries || []);
    const updated = (b.updated_at || b.created_at) ? new Date(Date.parse(b.updated_at || b.created_at)).toISOString().replace('T',' ').replace(/\.\d+Z$/,'Z') : null;
    const details = [`Items: ${Array.isArray(arr) ? arr.length : 0}`];
    if (updated) details.push(`Updated: ${updated}`);
    if (b.filename) details.push(`File: ${b.filename}`);
    content.push({ tag: 'p', children: [ details.join(' | ') ] });
    content.push({ tag: 'p', children: ['— — —'] });
  }
  return content;
}

// REPLACE the existing telegraphCreatePage with this improved version
// (adds optional existingUrl param, consistent logging and same retry/backoff)
async function telegraphCreatePage({ title, author_name, content, existingUrl = null } = {}) {
  if (!title || !content) throw new Error('telegraphCreatePage: title and content are required');

  // If an existingUrl is provided, we still return it — caller should compute whether it needs to be refreshed.
  if (existingUrl) {
    try { return existingUrl; } catch (e) { /* fallthrough to create new page */ }
  }

  const accessToken = (process.env.TELEGRAPH_ACCESS_TOKEN || '').trim();
  const params = new URLSearchParams();
  params.append('title', String(title).slice(0, 128));
  params.append('author_name', author_name || 'Bot');
  params.append('content', JSON.stringify(content));
  params.append('return_content', 'false');
  if (accessToken) params.append('access_token', accessToken);

  const httpsAgent = new https.Agent({ keepAlive: true });
  const axiosOpts = { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000, httpsAgent };

  const maxAttempts = 4;
  let attempt = 0, lastErr = null;
  while (++attempt <= maxAttempts) {
    try {
      const res = await axios.post('https://api.telegra.ph/createPage', params.toString(), axiosOpts);
      if (res && res.data && res.data.ok && res.data.result && res.data.result.path) {
        const url = 'https://telegra.ph/' + res.data.result.path;
        console.log(`telegraphCreatePage: created ${url} (title="${title}", attempt=${attempt})`);
        return url;
      }
      lastErr = new Error('Unexpected Telegraph response: ' + JSON.stringify(res && res.data));
    } catch (err) {
      lastErr = err;
      const code = err && err.code;
      const transient = code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'EAI_AGAIN'
                      || (err.message && (err.message.includes('socket hang up') || err.message.includes('rate limit')));
      // Simple exponential backoff for transient errors
      if (!transient) break;
      const wait = Math.min(2000 * Math.pow(2, attempt - 1), 30_000);
      console.warn(`telegraphCreatePage transient error (attempt ${attempt}) — waiting ${wait}ms —`, err && err.message);
      await new Promise(r => setTimeout(r, wait));
    }
  }
  console.error('telegraphCreatePage failed after attempts', lastErr && (lastErr.stack || lastErr.message));
  throw lastErr || new Error('Failed to create Telegraph page');
}

/**
 * Build compact nodes for a list of batches, with global numbering offset.
 * batches: array of batch objects (a slice)
 * botUser: BOT_USERNAME
 * offset: integer — how many batches precede this slice (for global numbering)
 */

function buildBatchListNodesWithOffset(batches, botUser, offset = 0) {
  const content = [];
  for (let i = 0; i < batches.length; i++) {
    const idxGlobal = offset + i + 1; // global numbering: 1-based
    const b = batches[i];
    const title = b.title || b.display_name || b.filename || ('Batch ' + (b.token || ''));
    const link = botUser ? `https://t.me/${botUser}?start=${encodeURIComponent(b.token)}` : null;

    // Title line with global number
    content.push({
      tag: 'p',
      children: [
        { tag: 'strong', children: [ `${idxGlobal}. ${title}` ] }
      ]
    });

    // Token monospace + Open link (if bot username present)
    const metaChildren = [{ tag: 'code', children: [ b.token ] }];
    if (link) {
      metaChildren.push(' — ');
      metaChildren.push({ tag: 'a', attrs: { href: link }, children: ['Open'] });
    }
    content.push({ tag: 'p', children: metaChildren });

    // Details line
    const arr = (b.files || b.items || b.media || b.links || b.entries || []);
    const updated = (b.updated_at || b.created_at) ? new Date(Date.parse(b.updated_at || b.created_at)).toISOString().replace('T',' ').replace(/\.\d+Z$/,'Z') : null;
    const details = [`Items: ${Array.isArray(arr) ? arr.length : 0}`];
    if (updated) details.push(`Updated: ${updated}`);
    if (b.filename) details.push(`File: ${b.filename}`);
    content.push({ tag: 'p', children: [ details.join(' | ') ] });

    // Soft divider
    content.push({ tag: 'p', children: ['— — —'] });
  }
  return content;
}

// REPLACE createTelegraphIndexPages with this chunked, incremental + queued implementation.
// It writes/reads data/telegraph_index.json and uses readMeta/writeMeta to persist state.
async function createTelegraphIndexPages(currentBatch /* optional */, opts = {}) {
  opts = opts || {};
  const FORCE = !!opts.force;
  const meta = readMeta();
  meta.telegraph_index = meta.telegraph_index || { pages: [], updated_at: null };
  const indexPath = path.join(DATA_DIR, 'telegraph_index.json');

  // Helper: compute stable hash of nodes for change detection
  function computeNodesHash(nodes) {
    const h = crypto.createHash('sha1');
    h.update(JSON.stringify(nodes));
    return h.digest('hex');
  }

  // Helper: persist the telegraph_index.json file
  function persistTelegraphIndex(obj) {
    try {
      atomicWriteFileSync(indexPath, JSON.stringify(obj, null, 2));
    } catch (e) { console.warn('persistTelegraphIndex failed', e && e.message); }
  }

  // read existing mapping (cached pages)
  let savedIndex = { pages: [] };
  try { if (fs.existsSync(indexPath)) savedIndex = JSON.parse(fs.readFileSync(indexPath, 'utf8') || '{}'); } catch (e) { savedIndex = { pages: [] }; }

  // Build ordered list of batch objects
  const idx = readIndex() || {};
  const order = Array.isArray(idx.order) ? idx.order.slice() : null;
  const tokensMap = idx.tokens || {};
  const filenamesOrdered = order && order.length ? order.slice() : Object.values(tokensMap || {});
  const allBatches = [];
  for (const filename of filenamesOrdered) {
    const b = readBatchFile(filename);
    if (b && b.token) allBatches.push(b);
  }

  // fallback sort if no explicit order
  if (!order || !order.length) {
    allBatches.sort((a,b) => {
      const ta = Date.parse(a.updated_at || a.created_at || 0) || 0;
      const tb = Date.parse(b.updated_at || b.created_at || 0) || 0;
      return tb - ta;
    });
  }

  const botUser = (typeof BOT_USERNAME === 'string' && BOT_USERNAME.trim()) ? BOT_USERNAME.trim() : null;

  // Header nodes (same as before)
  const headerNodes = [];
  if (currentBatch && currentBatch.token) {
    const curTitle = currentBatch.title || currentBatch.display_name || currentBatch.filename || ('Batch ' + currentBatch.token);
    const curLink = botUser ? `https://t.me/${botUser}?start=${encodeURIComponent(currentBatch.token)}` : null;
    const arr = (currentBatch.files || currentBatch.items || currentBatch.media || currentBatch.links || currentBatch.entries || []);
    const updated = (currentBatch.updated_at || currentBatch.created_at) ? new Date(Date.parse(currentBatch.updated_at || currentBatch.created_at)).toISOString().replace('T',' ').replace(/\.\d+Z$/,'Z') : null;

    headerNodes.push({ tag: 'h3', children: ['Current Batch'] });
    headerNodes.push({ tag: 'p', children: [{ tag: 'strong', children: [curTitle] }] });
    const line = [{ tag: 'code', children: [ currentBatch.token ] }];
    if (curLink) { line.push(' — '); line.push({ tag: 'a', attrs: { href: curLink }, children: ['Open'] }); }
    headerNodes.push({ tag: 'p', children: line });
    const pieces = [`Items: ${Array.isArray(arr) ? arr.length : 0}`];
    if (updated) pieces.push(`Updated: ${updated}`);
    headerNodes.push({ tag: 'p', children: [ pieces.join(' | ') ] });
    if (currentBatch.description) headerNodes.push({ tag: 'p', children: ['Desc: ', { tag: 'em', children: [ currentBatch.description ] }] });
    headerNodes.push({ tag: 'hr' });
  }

  // Chunking parameters
  // Make this very large so that, in normal usage, everything fits on one page.
  // Telegraph will still fail if the page is *really* huge, but for typical
  // batch counts this will produce a single index page.
  const SOFT_JSON_LIMIT = 4_000_000;
  const pagesMeta = []; // will contain { fromIndex, toIndex, hash, url, updatedAt }

  // Build chunks (we make minimal chunks by testing JSON size)
  let startIndex = 0;
  while (startIndex < allBatches.length) {
    let endIndex = startIndex;
    let nodes = headerNodes.slice();
    while (endIndex < allBatches.length) {
      const slice = allBatches.slice(endIndex, endIndex + 1);
      const candidateNodes = buildBatchListNodesWithOffset(slice, botUser, endIndex);
      const nextNodes = nodes.concat(candidateNodes);
      const size = Buffer.byteLength(JSON.stringify(nextNodes), 'utf8');
      if (size > SOFT_JSON_LIMIT) break;
      nodes = nextNodes;
      endIndex++;
    }
    if (endIndex === startIndex) {
      // single batch too big, force include one
      const slice = allBatches.slice(endIndex, endIndex + 1);
      nodes = headerNodes.concat(buildBatchListNodesWithOffset(slice, botUser, endIndex));
      endIndex++;
    }
    const chunkHash = computeNodesHash(nodes);
    pagesMeta.push({ fromIndex: startIndex, toIndex: endIndex - 1, nodes, hash: chunkHash });
    startIndex = endIndex;
  }

  // If no batches, keep a single empty chunk
  if (pagesMeta.length === 0) {
    const nodes = headerNodes.length ? headerNodes : [{ tag: 'p', children: ['No batches found.'] }];
    const chunkHash = computeNodesHash(nodes);
    pagesMeta.push({ fromIndex: 0, toIndex: -1, nodes, hash: chunkHash });
  }

  // Determine which pages need creation (changed or missing)
  const tasks = [];
  for (let i = 0; i < pagesMeta.length; i++) {
    const p = pagesMeta[i];
    const saved = (savedIndex.pages && savedIndex.pages[i]) ? savedIndex.pages[i] : null;
    const needsCreate = FORCE || !saved || saved.hash !== p.hash || !saved.url;
    tasks.push({ index: i, from: p.fromIndex, to: p.toIndex, nodes: p.nodes, hash: p.hash, needsCreate, oldUrl: saved && saved.url });
  }

  // Concurrency-limited runner (simple semaphore)
  const CONCURRENCY = Number(process.env.TELEGRAPH_CONCURRENCY || 2);
  const results = new Array(tasks.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= tasks.length) break;
      const t = tasks[idx];
      try {
        if (!t.needsCreate) {
          // reuse old URL and mark updatedAt preserved
          results[idx] = { url: t.oldUrl, hash: t.hash, from: t.from, to: t.to, updatedAt: savedIndex.pages && savedIndex.pages[idx] && savedIndex.pages[idx].updatedAt ? savedIndex.pages[idx].updatedAt : new Date().toISOString() };
          console.log(`telegraph index page #${idx+1} unchanged — reusing ${t.oldUrl}`);
          continue;
        }
        // create page with retry/backoff via telegraphCreatePage (already has retry)
        const pageTitleBase = currentBatch && currentBatch.token
          ? `Index • ${currentBatch.title || currentBatch.display_name || currentBatch.filename || currentBatch.token}`
          : 'Index • All Batches';
        const title = `${pageTitleBase} • Page ${idx + 1}`;
        const author_name = currentBatch?.uploader_name || 'Bot';
        console.log(`telegraph index: creating page #${idx+1} (batches ${t.from+1}-${t.to+1})`);
        const url = await telegraphCreatePage({ title, author_name, content: t.nodes });
        results[idx] = { url, hash: t.hash, from: t.from, to: t.to, updatedAt: new Date().toISOString() };
        // small delay between creations to be gentle with Telegraph API
        await new Promise(r => setTimeout(r, 250));
      } catch (err) {
        console.warn(`telegraph index: failed to create page #${idx+1}`, err && (err.message || err));
        results[idx] = { url: null, hash: t.hash, from: t.from, to: t.to, updatedAt: null, error: (err && err.message) || 'failed' };
      }
    }
  }

  // start workers
  const ws = [];
  for (let i = 0; i < CONCURRENCY; i++) ws.push(worker());
  await Promise.all(ws);

  // Compose final mapping and persist
  const finalPages = [];
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r && r.url) {
      finalPages.push({ page: i+1, fromIndex: r.from, toIndex: r.to, url: r.url, hash: r.hash, updatedAt: r.updatedAt });
    } else {
      // fallback: if there was an older url at same slot, keep it
      const older = (savedIndex.pages && savedIndex.pages[i]) ? savedIndex.pages[i] : null;
      finalPages.push(older ? Object.assign({}, older) : { page: i+1, fromIndex: tasks[i].from, toIndex: tasks[i].to, url: null, hash: tasks[i].hash, updatedAt: r && r.updatedAt || null });
    }
  }

  const out = { pages: finalPages, generatedAt: new Date().toISOString(), totalBatches: allBatches.length };
  persistTelegraphIndex(out);

  // also save into meta.json for quick in-memory reference
  meta.telegraph_index = out;
  meta.telegraph_index.updated_at = new Date().toISOString();
  try { writeMeta(meta); } catch (e) { console.warn('writeMeta failed', e && e.message); }

  // return array of URLs (in order)
  const urls = finalPages.map(p => p.url).filter(Boolean);
  return urls;
}

// ----------------- TELEGRAPH index for message keys (Public messages) -----------------
// Creates (or returns cached) Telegraph index for all message keys.
// opts = { force: boolean } — if force === true, rebuild index and individual pages.
async function createTelegraphIndexForMsgKeys(opts = {}) {
  const force = !!(opts && opts.force);
  const meta = getMsgMeta();
  meta.saved_texts = meta.saved_texts || {};
  meta.msg_keys_telegraph = meta.msg_keys_telegraph || { indexUrl: null, key_pages: {}, updated_at: null };

  const saved = meta.saved_texts || {};
  const keys = Object.keys(saved).sort();

  // Load existing telegraph_index.json (if any) so we can store msg_keys alongside batch index
  let teleIndex = {};
  try {
    if (fs.existsSync(TELEGRAPH_INDEX_FILE)) {
      const raw = fs.readFileSync(TELEGRAPH_INDEX_FILE, 'utf8');
      teleIndex = raw ? JSON.parse(raw) : {};
    }
  } catch (e) {
    console.warn('Failed to read TELEGRAPH_INDEX_FILE', e && e.message);
    teleIndex = {};
  }
  teleIndex = teleIndex && typeof teleIndex === 'object' ? teleIndex : {};
  teleIndex.msg_keys = teleIndex.msg_keys || {
    indexUrl: null,
    keyPages: {},
    updatedAt: null,
    totalKeys: 0
  };

  // Fast path: if we already have a cached index URL and not forcing, just ensure telegraph_index.json is in sync
  if (!force && meta.msg_keys_telegraph && meta.msg_keys_telegraph.indexUrl) {
    if (!teleIndex.msg_keys.indexUrl) {
      // First time migrating to telegraph_index.json; mirror existing meta cache
      teleIndex.msg_keys = {
        indexUrl: meta.msg_keys_telegraph.indexUrl,
        keyPages: meta.msg_keys_telegraph.key_pages || {},
        updatedAt: meta.msg_keys_telegraph.updated_at || new Date().toISOString(),
        totalKeys: keys.length
      };
      try {
        atomicWriteFileSync(TELEGRAPH_INDEX_FILE, JSON.stringify(teleIndex, null, 2));
      } catch (e) {
        console.warn('Failed to write TELEGRAPH_INDEX_FILE (fast path)', e && e.message);
      }
    }
    return {
      indexUrl: meta.msg_keys_telegraph.indexUrl,
      keyPages: meta.msg_keys_telegraph.key_pages || {}
    };
  }

  // Build/refresh pages for keys
  const keyPages = {};
  for (const k of keys) {
    try {
      // when rebuilding, force page creation; otherwise attempt cached inside createTelegraphPageForMsgKey
      keyPages[k] = await createTelegraphPageForMsgKey(k, { force: !!force });
    } catch (e) {
      console.warn('Failed to build Telegraph page for key', k, e && e.message);
    }
  }

  // Build index nodes (only include keys where page exists)
  const nodes = [{ tag: 'h2', children: ['Stored Key files'] }];
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    const url = keyPages[k];
    if (!url) continue;
    const count = Array.isArray(saved[k]) ? saved[k].length : 0;
    nodes.push({
      tag: 'p',
      children: [
        {
          tag: 'a',
          attrs: { href: url },
          children: [ `${k} (${count})` ]
        }
      ]
    });
  }

  // create index page
  const indexUrl = await telegraphCreatePage({
    title: 'Public Messages — Index',
    author_name: (typeof BOT_USERNAME === 'string' && BOT_USERNAME) ? BOT_USERNAME : 'Bot',
    content: nodes
  });

  // Save latest index url (cache in meta)
  meta.msg_keys_telegraph = meta.msg_keys_telegraph || {};
  meta.msg_keys_telegraph.indexUrl = indexUrl;
  meta.msg_keys_telegraph.key_pages = keyPages;
  meta.msg_keys_telegraph.updated_at = new Date().toISOString();
  saveMsgMeta(meta);

  // Also mirror into telegraph_index.json under a separate "msg_keys" bucket
  teleIndex.msg_keys = {
    indexUrl,
    keyPages,
    updatedAt: meta.msg_keys_telegraph.updated_at,
    totalKeys: keys.length
  };

  try {
    atomicWriteFileSync(TELEGRAPH_INDEX_FILE, JSON.stringify(teleIndex, null, 2));
  } catch (e) {
    console.warn('Failed to write TELEGRAPH_INDEX_FILE', e && e.message);
  }

  return { indexUrl, keyPages };
}

// ----------------- TELEGRAPH for message-store keys -----------------
// Create a telegraph page for a saved message-key (all items under that key).
// --- Inline children builder: returns children[] (no wrapping <p>) ---
function htmlToTelegraphInlineChildren(html) {
  const URL_RE = /\b((https?:\/\/|www\.)[^\s<>{}|\^\[\]"]+)\b/gi;

  // extract <a href> ... </a> first, replace by tokens
  const anchors = [];
  let s = String(html || '')
    .replace(/<br\s*\/?>/gi, ' ')        // inline; avoid forced new lines
    .replace(/<\/p>/gi, ' ');            // inline; avoid paragraph breaks

  let aId = 0;
  // tolerate backslash-escaped quotes like href=\"...\" — remove escaping so our regex can match
  s = s.replace(/\\+(['"])\s*/g, '$1');

  s = s.replace(/<a\s+[^>]*href\s*=\s*(['"])(.*?)\1[^>]*>([\s\S]*?)<\/a>/gi,
    (_m, _q, href, inner) => {
      const id = aId++;
      const cleanText = String(inner || '').replace(/<[^>]+>/g, '').trim();
      anchors[id] = {
        href: String(href || '').trim(),
        text: cleanText || String(href || '').trim()
      };
      return `\u0001A${id}\u0002`; // token
    });

  // drop other tags but keep their text
  s = s
    .replace(/<(\/)?(div|span|h[1-6]|ul|ol|li|blockquote|pre|code|u|s)[^>]*>/gi, '')
    .replace(/<(\/)?(b|strong|i|em)[^>]*>/gi, '')
    .replace(/<[^>]+>/g, '')            // any remaining tags
    .replace(/\s+/g, ' ')               // collapse spaces
    .trim();

  const children = [];
  let idx = 0;
  const tokenRe = /\u0001A(\d+)\u0002/g;
  let m;

  // helper: push plain segment with autolink
  const pushAutoLinked = (txt) => {
    let last = 0;
    String(txt || '').replace(URL_RE, (match, url, _scheme, pos) => {
      if (pos > last) children.push(txt.slice(last, pos));
      const href = url.startsWith('http') ? url : `https://${url}`;
      children.push({ tag: 'a', attrs: { href }, children: [url] });
      last = pos + match.length;
    });
    if (last < String(txt || '').length) children.push(String(txt || '').slice(last));
  };

  while ((m = tokenRe.exec(s)) !== null) {
    const pre = s.slice(idx, m.index);
    if (pre) pushAutoLinked(pre);
    const a = anchors[Number(m[1])];
    if (a) {
      const href = a.href.startsWith('http') ? a.href : `https://${a.href}`;
      children.push({ tag: 'a', attrs: { href }, children: [a.text] });
    }
    idx = m.index + m[0].length;
  }
  const tail = s.slice(idx);
  if (tail) pushAutoLinked(tail);

  if (!children.length) children.push(''); // ensure non-empty
  return children;
}

// Returns the created page URL on success, throws on failure.
// Produces:
//   <h3>Key</h3>
//   <p>#1  <inline content></p>
//   <hr>
//   <p>#2  <inline content></p> ...
async function createTelegraphPageForMsgKey(key, opts = {}) {
  const force = !!(opts && opts.force);
  const meta = getMsgMeta();
  meta.msg_keys_telegraph = meta.msg_keys_telegraph || { indexUrl: null, key_pages: {}, updated_at: null };

  // If we have a cached page and not forcing, return it
  const cached = meta.msg_keys_telegraph.key_pages && meta.msg_keys_telegraph.key_pages[key];
  if (cached && !force) return cached;

  const arr = (meta && meta.saved_texts && meta.saved_texts[key]) ? meta.saved_texts[key] : [];
  const nodes = [];
  nodes.push({ tag: 'h3', children: [ String(key) ] });

  if (!arr || arr.length === 0) {
    nodes.push({ tag: 'p', children: ['No items'] });
  } else {
    for (let i = 0; i < arr.length; i++) {
      const item = arr[i];
      const raw = (item && (item.html || item.caption || item.text)) ?? String(item || '');
      const inline = htmlToTelegraphInlineChildren(raw);
      nodes.push({ tag: 'p', children: [`#${i + 1}  `, ...inline] });
      if (i !== arr.length - 1) nodes.push({ tag: 'hr' });
    }
  }

  const url = await telegraphCreatePage({
    title: `${String(key)} — Messages`,
    author_name: (typeof BOT_USERNAME === 'string' && BOT_USERNAME) ? BOT_USERNAME : 'Bot',
    content: nodes
  });

  // store in cache
  meta.msg_keys_telegraph = meta.msg_keys_telegraph || {};
  meta.msg_keys_telegraph.key_pages = meta.msg_keys_telegraph.key_pages || {};
  meta.msg_keys_telegraph.key_pages[key] = url;
  meta.msg_keys_telegraph.updated_at = new Date().toISOString();
  saveMsgMeta(meta);

  return url;
}

// Minimal HTML -> Telegraph nodes (keeps <b>/<strong>, <i>/<em>, <a href>, and breaks on <br>)
// Improved: un-escape backslash-escaped quotes before extracting anchors.
function htmlToTelegraphNodes(html) {
  // 1) Normalize line breaks
  let s = String(html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n');

  // 1.5) Unescape any backslash-escaped quote sequences commonly pasted (e.g. href=\\\"...\")
  s = s.replace(/\\+(['"])\s*/g, '$1');

  // 2) Extract anchors first so we can safely strip other tags
  const anchors = [];
  let anchorId = 0;
  s = s.replace(/<a\s+[^>]*href\s*=\s*(?:'([^']*)'|"([^"]*)"|([^>\s]+))[^>]*>([\s\S]*?)<\/a>/gi,
    (_m, href1, href2, href3, inner) => {
      const href = String(href1 || href2 || href3 || '').trim();
      const id = anchorId++;
      anchors[id] = {
        href,
        text: String(inner || '').replace(/<[^>]+>/g, '').trim() || href
      };
      return `\u0001A${id}\u0002`; // token
    });

  // 3) Remove any other tags except basic bold/italic (we’ll flatten them as text)
  s = s.replace(/<(\/)?(div|span|h[1-6]|ul|ol|li|blockquote|pre|code|u|s)[^>]*>/gi, '');
  s = s.replace(/<(\/)?(b|strong|i|em)[^>]*>/gi, ''); // keep their text only

  // 4) Strip remaining tags
  s = s.replace(/<[^>]+>/g, '');

  // 5) Split paragraphs by blank lines
  const paras = s.split(/\n{2,}/g).map(t => t.trim()).filter(Boolean);

  // 6) Build children with restored <a> and auto-linkify bare URLs
  const URL_RE = /\b((https?:\/\/|www\.)[^\s<>{}|\^\[\]"]+)\b/gi;
  const out = [];

  for (const para of (paras.length ? paras : [''])) {
    const children = [];
    let idx = 0;
    const tokenRe = /\u0001A(\d+)\u0002/g;
    let m;
    while ((m = tokenRe.exec(para)) !== null) {
      const pre = para.slice(idx, m.index);
      if (pre) {
        // linkify bare urls in pre
        let last = 0;
        pre.replace(URL_RE, (match, url, _scheme, pos) => {
          if (pos > last) children.push(pre.slice(last, pos));
          const href = url.startsWith('http') ? url : `https://${url}`;
          children.push({ tag: 'a', attrs: { href }, children: [url] });
          last = pos + match.length;
        });
        if (last < pre.length) children.push(pre.slice(last));
      }
      const a = anchors[Number(m[1])] || null;
      if (a) {
        const href = a.href && a.href.startsWith('http') ? a.href : `https://${a.href}`;
        children.push({ tag: 'a', attrs: { href }, children: [a.text] });
      }
      idx = m.index + m[0].length;
    }

    const tail = para.slice(idx);
    if (tail) {
      let last = 0;
      tail.replace(URL_RE, (match, url, _scheme, pos) => {
        if (pos > last) children.push(tail.slice(last, pos));
        const href = url.startsWith('http') ? url : `https://${url}`;
        children.push({ tag: 'a', attrs: { href }, children: [url] });
        last = pos + match.length;
      });
      if (last < tail.length) children.push(tail.slice(last));
    }

    if (!children.length) children.push('');
    out.push({ tag: 'p', children });
  }
  return out;
}

// ---------------- Telegraph-safe content builders ----------------
// turns raw text into node children with clickable links
function paragraphChildrenWithLinks(text) {
  const URL_RE = /\b((https?:\/\/|www\.)[^\s<>{}|\^\[\]"]+)\b/gi;
  const out = [];
  let lastIndex = 0;
  text.replace(URL_RE, (m, url, _scheme, idx) => {
    if (idx > lastIndex) out.push(text.slice(lastIndex, idx));
    const href = url.startsWith('http') ? url : `https://${url}`;
    out.push({ tag: 'a', attrs: { href }, children: [url] });
    lastIndex = idx + m.length;
  });
  if (lastIndex < text.length) out.push(text.slice(lastIndex));
  // Telegraph requires children to be non-empty; ensure at least something
  if (!out.length) out.push('');
  return out;
}

// ---------------- Telegraph-safe content builders (preserve <a>, basic inline tags) ----------------
const URL_RE = /\b((https?:\/\/|www\.)[^\s<>{}|\^\[\]"]+)\b/gi;

// decode a tiny subset of entities
function decodeEntities(s) {
  return String(s || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

// ----------------- improved extractAnchors -----------------
// parse <a href="...">text</a> into placeholders, tolerant of backslash-escaped quotes
function extractAnchors(html) {
  const anchors = [];
  let i = 0;

  // remove backslash escapes before quotes so href=\"...\" becomes href="..."
  // also handles multiple backslashes (\\\" -> \")
  const preclean = String(html || '').replace(/\\+(['"])/g, '$1');

  // now extract anchors safely
  const replaced = preclean.replace(
    /<a\s+[^>]*href\s*=\s*(['"])(.*?)\1[^>]*>([\s\S]*?)<\/a>/gi,
    (_, __, href, inner) => {
      const id = i++;
      // decode HTML entities and strip other tags from inner text
      anchors.push({
        href: decodeEntities(String(href || '').trim().replace(/\\+(['"])/g, '$1')),
        text: decodeEntities(String(inner || '').replace(/<[^>]+>/g, '').trim())
      });
      return `\u0001A${id}\u0002`; // token
    }
  );

  return { replaced, anchors };
}

// split into paragraphs by <br> and </p>
function splitParagraphs(html) {
  return String(html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n')
    .split(/\n{2,}/g)
    .map(s => s.trim())
    .filter(Boolean);
}

// turn a text line with tokens + plain URLs into telegraph children
function buildChildren(line, anchors) {
  const out = [];
  let idx = 0;
  const tokenRe = /\u0001A(\d+)\u0002/g;

  // first, break around tokens so we can linkify plain segments
  let m;
  while ((m = tokenRe.exec(line)) !== null) {
    const pre = line.slice(idx, m.index);
    if (pre) {
      // linkify bare URLs in pre
      let last = 0;
      pre.replace(URL_RE, (match, url, _scheme, pos) => {
        if (pos > last) out.push(pre.slice(last, pos));
        const href = url.startsWith('http') ? url : `https://${url}`;
        out.push({ tag: 'a', attrs: { href }, children: [url] });
        last = pos + match.length;
      });
      if (last < pre.length) out.push(pre.slice(last));
    }
    const aIdx = Number(m[1]);
    const a = anchors[aIdx];
    if (a) {
      const label = a.text || a.href;
      const href = a.href.startsWith('http') ? a.href : `https://${a.href}`;
      out.push({ tag: 'a', attrs: { href }, children: [label] });
    }
    idx = m.index + m[0].length;
  }
  const tail = line.slice(idx);
  if (tail) {
    let last = 0;
    tail.replace(URL_RE, (match, url, _scheme, pos) => {
      if (pos > last) out.push(tail.slice(last, pos));
      const href = url.startsWith('http') ? url : `https://${url}`;
      out.push({ tag: 'a', attrs: { href }, children: [url] });
      last = pos + match.length;
    });
    if (last < tail.length) out.push(tail.slice(last));
  }

  // ensure at least something
  if (!out.length) out.push('');
  return out;
}

// allow a tiny subset of inline tags by turning them into plain text markers;
// Telegraph supports <b>, <i>, <u>, <s>, <code>, but it’s safer to keep them as plain text or simple wrappers
function simplifyInline(html) {
  return String(html || '')
    // drop block-level tags but keep text they wrap
    .replace(/<(\/)?(div|h[1-6]|ul|ol|li|blockquote|pre|code|span)[^>]*>/gi, '')
    // keep these as raw text (Telegraph can accept them but we avoid nesting issues)
    .replace(/<(\/)?(b|strong|i|em|u|s|code)[^>]*>/gi, '');
}

// MAIN: preserve <a>, remove other tags, keep text; convert to <p> nodes with children[]
function htmlToTelegraphSafeNodes(html, { heading } = {}) {
  try {
    const { replaced, anchors } = extractAnchors(simplifyInline(html));
    const paras = splitParagraphs(replaced.replace(/<[^>]+>/g, '')); // strip remaining tags
    const nodes = [];

    if (heading) nodes.push({ tag: 'h4', children: [heading.slice(0, 120)] });

    for (const para of paras.slice(0, 500)) {
      const children = buildChildren(decodeEntities(para), anchors);
      nodes.push({ tag: 'p', children });
      if (nodes.length > 900) break;
    }

    if (!nodes.length) nodes.push({ tag: 'p', children: ['(empty)'] });
    return nodes;
  } catch (e) {
    return [
      { tag: 'h4', children: [heading ? heading.slice(0,120) : ''] },
      { tag: 'p', children: ['(content unavailable)'] }
    ];
  }
}

function buildNodesFromEntities(text, entities = [], { heading } = {}) {
  try {
    const nodes = [];
    if (heading) nodes.push({ tag: 'h4', children: [heading.slice(0, 120)] });

    const s = String(text || '');
    if (!s) {
      nodes.push({ tag: 'p', children: ['(empty)'] });
      return nodes;
    }

    // Build one paragraph; you can split on newlines if you want multiple <p>
    const children = [];
    let i = 0;
    const sorted = [...entities].sort((a,b) => a.offset - b.offset);

    for (const ent of sorted) {
      if (ent.offset > i) children.push(s.slice(i, ent.offset));
      const seg = s.substr(ent.offset, ent.length);

      if (ent.type === 'text_link' && ent.url) {
        const href = ent.url.startsWith('http') ? ent.url : `https://${ent.url}`;
        children.push({ tag: 'a', attrs: { href }, children: [seg] });
      } else if (ent.type === 'url') {
        const href = seg.startsWith('http') ? seg : `https://${seg}`;
        children.push({ tag: 'a', attrs: { href }, children: [seg] });
      } else if (ent.type === 'bold') {
        children.push(seg); // keep as plain text; Telegraph can handle <b> but simple is safe
      } else if (ent.type === 'italic') {
        children.push(seg);
      } else if (ent.type === 'underline') {
        children.push(seg);
      } else if (ent.type === 'strikethrough') {
        children.push(seg);
      } else if (ent.type === 'code') {
        children.push(seg);
      } else {
        children.push(seg);
      }
      i = ent.offset + ent.length;
    }
    if (i < s.length) children.push(s.slice(i));

    nodes.push({ tag: 'p', children: children.length ? children : [''] });
    return nodes;
  } catch {
    return [{ tag: 'p', children: [String(text || '')] }];
  }
}

function safeTelegraphTitle(s) {
  const t = String(s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
  return t.slice(0, 128) || 'Messages';
}

// helper: safe page title (no markup, short)
function safeTelegraphTitle(s) {
  const t = String(s || '').replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
  return t.slice(0, 128) || 'Messages';
}

/**
 * Send admin the telegraph index pages as inline buttons that open the pages.
 * Also includes action buttons (post/broadcast/regenerate/skip).
 */
async function sendAdminTelegraphIndex(batch, adminChatId) {
  const adminId = adminChatId || ADMIN_ID;
  try {
    const urls = await createTelegraphIndexPages(batch); // returns [url1, url2, ...]
    // Build message text: mention current batch token in monospace
    const firstUrl = urls[0];
    const tokenPart = batch && batch.token ? `<code>${batch.token}</code>` : '';
    let text = `Index pages created ${ tokenPart ? `for batch ${tokenPart}` : 'for all batches' } (${urls.length} pages):\n\n`;
    urls.forEach((u, i) => {
      text += `Page ${i + 1}: ${u}\n`;
    });

    // Build inline keyboard with page buttons (3 per row)
    const kb = { inline_keyboard: [] };
    const perRow = 3;
    for (let i = 0; i < urls.length; i += perRow) {
      const row = [];
      for (let j = i; j < Math.min(i + perRow, urls.length); j++) {
        row.push({ text: `Page ${j + 1}`, url: urls[j] });
      }
      kb.inline_keyboard.push(row);
    }

    // Add action row(s)
    kb.inline_keyboard.push([
      { text: '📣 Post to channel', callback_data: `admin_post_channel_${batch.token}` },
      { text: '📢 Broadcast', callback_data: `admin_broadcast_${batch.token}` }
    ]);
    kb.inline_keyboard.push([
      { text: '🔁 Regenerate pages', callback_data: `admin_regen_page_${batch.token}` },
      { text: '⏭ Skip', callback_data: `admin_skip_${batch.token}` }
    ]);

    await safeSendMessage(adminId, text, { parse_mode: 'HTML', reply_markup: kb });
    return urls;
  } catch (e) {
    try { await safeSendMessage(adminId, `Failed to create Telegraph pages for batch ${batch && batch.token}: ${e && e.message}`); } catch(_) {}
    throw e;
  }
}

/* ------------------ Auto-clear chat feature ------------------
   Usage (in chat):
     /autoclear on        - enables auto-clear with default 15 minutes (admin only in groups)
     /autoclear on 10     - enables auto-clear with 10 minutes
     /autoclear off       - disables auto-clear

   Implementation notes:
   - Uses getMsgMeta() / saveMsgMeta(meta) for persistence in meta.auto_clear & meta.pending_deletes
   - Schedules deletes via setTimeout for immediate short-term reliability AND uses a periodic sweep to catch missed items on restart.
   - Requires the bot to have permission to delete messages in the chat (bot must be admin in groups to delete other users' messages).
*/

// in-memory map of scheduled timers to avoid duplicate setTimeouts across restarts
const __autoClearTimers = new Map();

// Default duration (seconds) => 15 minutes
const AUTO_CLEAR_DEFAULT_SEC = 15 * 60;

// Ensure meta paths exist
function _ensureAutoClearMeta() {
  const meta = getMsgMeta();
  meta.auto_clear = meta.auto_clear || {};         // chatId -> seconds
  meta.pending_deletes = meta.pending_deletes || []; // array of { chatId, messageId, deleteAt }
  saveMsgMeta(meta);
  return meta;
}

// ----------------- Channel deep-link helper -----------------
function getChannelStartLinkForIndex(idx) {
  const botUser = (typeof BOT_USERNAME === 'string' && BOT_USERNAME) ? BOT_USERNAME : null;
  const payload = `CHMSG_${Number(idx)}`;
  return botUser ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(payload)}` : `https://t.me/?start=${encodeURIComponent(payload)}`;
}

// Enable auto-clear for a chat (durationSec default 15min)
function enableAutoClearForChat(chatId, durationSec = AUTO_CLEAR_DEFAULT_SEC) {
  const meta = getMsgMeta();
  meta.auto_clear = meta.auto_clear || {};
  meta.auto_clear[String(chatId)] = Number(durationSec) || AUTO_CLEAR_DEFAULT_SEC;
  saveMsgMeta(meta);
  // no immediate rebuild/telegraph interaction needed; just persist config
  return true;
}

// Disable auto-clear for a chat
function disableAutoClearForChat(chatId) {
  const meta = getMsgMeta();
  if (meta.auto_clear && meta.auto_clear[String(chatId)]) {
    delete meta.auto_clear[String(chatId)];
    saveMsgMeta(meta);
  }
  // also clear any pending timers for this chat
  for (const key of Array.from(__autoClearTimers.keys())) {
    if (key.startsWith(`${chatId}:`)) {
      clearTimeout(__autoClearTimers.get(key));
      __autoClearTimers.delete(key);
    }
  }
  // cleanup pending_deletes entries
  meta.pending_deletes = (meta.pending_deletes || []).filter(d => String(d.chatId) !== String(chatId));
  saveMsgMeta(meta);
  return true;
}

// Schedule a message for auto-deletion (persisted & in-memory timer)
function scheduleAutoDelete(chatId, messageId, delayMs) {
  if (!chatId || !messageId) return;
  const meta = _ensureAutoClearMeta();

  const now = Date.now();
  const deleteAt = now + Math.max(0, Number(delayMs || 0));

  // Add to persisted queue (avoid duplicates)
  meta.pending_deletes = meta.pending_deletes || [];
  const exists = meta.pending_deletes.find(d => String(d.chatId) === String(chatId) && Number(d.messageId) === Number(messageId));
  if (!exists) {
    meta.pending_deletes.push({ chatId: String(chatId), messageId: Number(messageId), deleteAt });
    saveMsgMeta(meta);
  }

  // Also schedule an in-memory timer for near-term deletion
  const timerKey = `${chatId}:${messageId}`;
  if (__autoClearTimers.has(timerKey)) return; // already scheduled

  const ms = Math.max(0, deleteAt - now);
  const t = setTimeout(async () => {
    __autoClearTimers.delete(timerKey);
    try {
      await bot.deleteMessage(chatId, messageId).catch(()=>{});
    } catch (e) {
      // ignore; maybe no permissions
    }
    // remove from persisted queue
    try {
      const m = getMsgMeta();
      m.pending_deletes = (m.pending_deletes || []).filter(d => !(String(d.chatId) === String(chatId) && Number(d.messageId) === Number(messageId)));
      saveMsgMeta(m);
    } catch (e) {}
  }, ms + 250); // small safety offset
  __autoClearTimers.set(timerKey, t);
}
// --- Auto-schedule deletion for messages the BOT sends ---
// Place this after scheduleAutoDelete(...) so it can call that helper.

(function enableAutoScheduleForBotSends() {
  // Helper to get per-chat auto-clear seconds (0/undefined if disabled)
  function _getAutoClearSecForChat(chatId) {
    try {
      const meta = getMsgMeta();
      if (!meta || !meta.auto_clear) return 0;
      return Number(meta.auto_clear[String(chatId)] || 0) || 0;
    } catch (e) {
      return 0;
    }
  }

  // Wrap existing safeSendMessage (if defined) so bot messages sent via it are scheduled.
  if (typeof safeSendMessage === 'function') {
    const _origSafeSendMessage = safeSendMessage;
    safeSendMessage = async function wrappedSafeSendMessage(chatId, textOrOpts, opts) {
      try {
        // Call original
        const res = await _origSafeSendMessage(chatId, textOrOpts, opts);
        try {
          const delaySec = _getAutoClearSecForChat(chatId);
          if (delaySec && res && (res.message_id || res.message?.message_id)) {
            const mid = res.message_id || (res.message && res.message.message_id);
            scheduleAutoDelete(chatId, mid, delaySec * 1000);
          }
        } catch (e) { /* ignore scheduling errors */ }
        return res;
      } catch (e) {
        // preserve original behaviour on error
        throw e;
      }
    };
  }

  // Wrap bot.sendMessage so direct calls are also scheduled.
  if (bot && typeof bot.sendMessage === 'function') {
    const _origBotSendMessage = bot.sendMessage.bind(bot);
    bot.sendMessage = async function wrappedBotSendMessage(chatId, text, options) {
      // Call original
      const res = await _origBotSendMessage(chatId, text, options);
      try {
        const delaySec = _getAutoClearSecForChat(chatId);
        if (delaySec && res && res.message_id) {
          scheduleAutoDelete(chatId, res.message_id, delaySec * 1000);
        }
      } catch (e) { /* ignore scheduling errors */ }
      return res;
    };
  }

  // Also wrap bot.sendPhoto / sendDocument / sendVideo / sendAudio / sendSticker if present,
  // since some places may call those directly. We patch only if they exist.
  const maybeWrap = (methodName) => {
    if (bot && typeof bot[methodName] === 'function') {
      const orig = bot[methodName].bind(bot);
      bot[methodName] = async function wrapped(...args) {
        // Typical signature starts with chatId, ...; we assume first arg is chatId
        const chatId = args[0];
        const res = await orig(...args);
        try {
          const delaySec = _getAutoClearSecForChat(chatId);
          if (delaySec && res && res.message_id) {
            scheduleAutoDelete(chatId, res.message_id, delaySec * 1000);
          }
        } catch (e) {}
        return res;
      };
    }
  };

  ['sendPhoto', 'sendDocument', 'sendVideo', 'sendAudio', 'sendSticker', 'sendMediaGroup', 'sendAnimation'].forEach(maybeWrap);

})();

// Periodic sweep that deletes any overdue messages persisted in meta.pending_deletes
async function processPendingAutoDeletes() {
  const meta = getMsgMeta();
  if (!meta || !Array.isArray(meta.pending_deletes) || !meta.pending_deletes.length) return;
  const now = Date.now();
  const due = meta.pending_deletes.filter(d => Number(d.deleteAt) <= now);
  if (!due.length) return;

  for (const item of due) {
    try {
      await bot.deleteMessage(item.chatId, item.messageId).catch(()=>{});
    } catch (e) {
      // ignore errors (no permission, message missing, etc.)
    }
    // clear any in-memory timer if present
    const key = `${item.chatId}:${item.messageId}`;
    if (__autoClearTimers.has(key)) {
      clearTimeout(__autoClearTimers.get(key));
      __autoClearTimers.delete(key);
    }
  }

  // remove all processed items from persisted list
  const remaining = (meta.pending_deletes || []).filter(d => Number(d.deleteAt) > now);
  meta.pending_deletes = remaining;
  saveMsgMeta(meta);
}

// At boot: restore timers for future pending deletes
function restorePendingAutoDeleteTimers() {
  const meta = getMsgMeta();
  if (!meta || !Array.isArray(meta.pending_deletes)) return;
  const now = Date.now();
  for (const d of meta.pending_deletes) {
    const chatId = d.chatId; const messageId = d.messageId; const deleteAt = Number(d.deleteAt) || 0;
    const timerKey = `${chatId}:${messageId}`;
    if (__autoClearTimers.has(timerKey)) continue;
    const ms = Math.max(0, deleteAt - now);
    // If overdue, we'll let the periodic sweep handle it quickly; but still schedule a short timer to attempt deletion
    const timeoutMs = ms > 0 ? ms + 250 : 1000;
    const t = setTimeout(async () => {
      __autoClearTimers.delete(timerKey);
      try { await bot.deleteMessage(chatId, messageId).catch(()=>{}); } catch(e) {}
      // cleanup persisted list (safe to call)
      try {
        const m = getMsgMeta();
        m.pending_deletes = (m.pending_deletes || []).filter(item => !(String(item.chatId) === String(chatId) && Number(item.messageId) === Number(messageId)));
        saveMsgMeta(m);
      } catch (e) {}
    }, timeoutMs);
    __autoClearTimers.set(timerKey, t);
  }
}

// Helper: check whether a user is admin in a chat (works for groups / supergroups)
async function isChatAdmin(chatId, userId) {
  if (!chatId || !userId) return false;
  try {
    const cm = await bot.getChatMember(chatId, userId);
    if (!cm || !cm.status) return false;
    return ['creator', 'administrator'].includes(cm.status);
  } catch (e) {
    // fallback: only bot owner can toggle if we cannot query
    return String(userId) === String(ADMIN_ID);
  }
}

// Hook into message receiver to schedule deletes when auto-clear is enabled
bot.on('message', async (msg) => {
  try {
    // existing message processing should continue; we only attach to schedule deletions
    const chatId = msg.chat && (msg.chat.id || msg.chat.username);
    if (!chatId) return;

    const meta = getMsgMeta();
    const cfg = meta && meta.auto_clear ? meta.auto_clear[String(chatId)] : null;
    const durationSec = cfg ? Number(cfg) : 0;
    if (!durationSec) return; // auto-clear not enabled for this chat

    // Optionally ignore service messages (new_chat_members, left, etc.)
    if (msg.new_chat_members || msg.left_chat_member || msg.delete_chat_photo || msg.new_chat_title || msg.pinned_message) {
      return; // skip meta/service messages
    }

    // schedule deletion for this message after durationSec
    const delayMs = durationSec * 1000;
    scheduleAutoDelete(chatId, msg.message_id, delayMs);

    // Also—if this message triggers admin commands (e.g., the /autoclear commands below),
    // those commands will still run as normal in other handlers.

  } catch (e) {
    console.error('auto-clear message hook error', e && (e.stack || e.message));
  }
});

// Periodic sweep every 30s to catch missed or overdue items
setInterval(() => {
  try { processPendingAutoDeletes().catch(()=>{}); } catch (e) {}
}, 30 * 1000);

// Restore pending timers at startup
try { restorePendingAutoDeleteTimers(); } catch (e) {}

// ----- Commands to control auto-clear -----
// Parse text commands; adapt to your command handling style if you already have a text handler
bot.onText(/^\/autoclear(?:\s+(.+))?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const senderId = msg.from && msg.from.id;
  const arg = (match && match[1]) ? match[1].trim() : '';

  // Only allow in private chat for owner or admin in groups
  if (msg.chat.type !== 'private') {
    const ok = await isChatAdmin(chatId, senderId);
    if (!ok) {
      return safeSendMessage(chatId, 'Only chat admins can change auto-clear settings.');
    }
  } else {
    // in private chats allow the bot owner (ADMIN_ID) or the user themself to toggle for their own private chat
    // We'll allow the user in private to toggle their chat
  }

  if (!arg || /^on$/i.test(arg) || /^off$/i.test(arg) === false && /^\d+$/i.test(arg)) {
    // treat "/autoclear" as status display
    const meta = getMsgMeta();
    const cfg = meta && meta.auto_clear ? meta.auto_clear[String(chatId)] : null;
    if (!cfg) {
      return safeSendMessage(chatId, 'Auto-clear is currently *disabled* in this chat.\n\nEnable with `/autoclear on` or `/autoclear on 10` (minutes).', { parse_mode: 'Markdown' });
    }
    return safeSendMessage(chatId, `Auto-clear is enabled: messages will be deleted after *${Math.round(cfg/60)}* minute(s).`, { parse_mode: 'Markdown' });
  }

  // Process arguments: "on", "on 10", "off"
  const parts = arg.split(/\s+/);
  const cmd = parts[0].toLowerCase();

  if (cmd === 'on') {
    let minutes = AUTO_CLEAR_DEFAULT_SEC / 60;
    if (parts[1] && /^\d+$/.test(parts[1])) minutes = Math.max(1, Math.min(60*24, Number(parts[1]))); // limit to 1..1440 min
    const seconds = Math.round(minutes * 60);
    enableAutoClearForChat(chatId, seconds);
    safeSendMessage(chatId, `Auto-clear enabled: messages will be deleted after ${minutes} minute(s).`);
    return;
  }

  if (cmd === 'off') {
    disableAutoClearForChat(chatId);
    safeSendMessage(chatId, 'Auto-clear disabled for this chat.');
    return;
  }

  // If user typed a numeric value directly (e.g., "/autoclear 10")
  if (/^\d+$/.test(cmd)) {
    const minutes = Math.max(1, Math.min(60*24, Number(cmd)));
    enableAutoClearForChat(chatId, minutes * 60);
    safeSendMessage(chatId, `Auto-clear enabled: messages will be deleted after ${minutes} minute(s).`);
    return;
  }

  // fallback: show usage
  safeSendMessage(chatId, 'Usage:\n/autoclear on [minutes]\n/autoclear off\n/autoclear (show status)');
});

// in-memory store
let suggestions = loadSuggestionsFromFile(); // try to load persisted suggestions
const pendingSuggests = {}; // (chatId:userId) -> { user_id, reply_to_message_id, createdAt }

function suggestKey(chatId, userId) {
  return `${chatId}:${userId}`;
}

// helper to load/save
function loadSuggestionsFromFile() {
  try {
    if (fs.existsSync(SUGGESTIONS_FILE)) {
      const raw = fs.readFileSync(SUGGESTIONS_FILE, 'utf8');
      return JSON.parse(raw || '[]');
    }
  } catch (e) {
    console.error('Could not load suggestions file:', e && e.message);
  }
  return [];
}
function saveSuggestionsToFile() {
  try {
    fs.writeFileSync(SUGGESTIONS_FILE, JSON.stringify(suggestions, null, 2), 'utf8');
  } catch (e) {
    console.error('Could not save suggestions file:', e && e.message);
  }
}

// helper to add a suggestion record
async function storeAndForwardSuggestion(chatId, user, text, originalMsg) {
  const rec = {
    id: Date.now().toString(),
    user: {
      id: user.id,
      username: user.username || null,
      first_name: user.first_name || null,
      last_name: user.last_name || null
    },
    text: String(text),
    createdAt: new Date().toISOString()
  };
  suggestions.unshift(rec); // newest first
  // keep a reasonable limit in memory
  if (suggestions.length > 1000) suggestions = suggestions.slice(0, 1000);
  saveSuggestionsToFile();

  // Forward / notify admin
  const adminId = typeof ADMIN_ID !== 'undefined' ? ADMIN_ID : null;
  const forwardedText =
    `<b>New suggestion</b>\n` +
    `From: ${escapeHtml(rec.user.first_name || '')} ${rec.user.username ? '(@' + escapeHtml(rec.user.username) + ')' : ''}\n` +
    `User ID: <code>${rec.user.id}</code>\n\n` +
    `<b>Suggestion:</b>\n${escapeHtml(rec.text)}`;

  if (adminId) {
    try {
      // Try to send the formatted message to admin
      await safeSendMessage(adminId, forwardedText, { parse_mode: 'HTML', disable_web_page_preview: true });
    } catch (e) {
      console.error('Failed to notify admin about suggestion:', e && e.message);
      // fallback: try to forward the original message (if available)
      if (originalMsg && originalMsg.chat && originalMsg.message_id) {
        try { await bot.forwardMessage(adminId, originalMsg.chat.id, originalMsg.message_id); } catch (_) {}
      }
    }
  }

  // Optional: also send a short acknowledgement to the admin chat if same chat
  return rec;
}

const lastAction = {};
function allowThrottle(uid, key='suggest', ms=5000) {
  const k = `${uid}:${key}`;
  const now = Date.now();
  if (lastAction[k] && now - lastAction[k] < ms) return false;
  lastAction[k] = now;
  return true;
}

// --- callback_query handler for the button ---
bot.on('callback_query', async (q) => {
  try {
    if (!q || !q.data) return;
    const data = q.data;

    if (data === 'open_suggest_prompt') {
      const chatId = q.message ? q.message.chat.id : (q.from && q.from.id);
      const userId = q.from && q.from.id;

      if (!chatId || !userId) {
        await safeAnswerCallbackQuery(q.id, { text: 'Unable to open suggest prompt' }).catch(()=>{});
        return;
      }

      // 💡 Throttle button spam per user
      if (!allowThrottle(userId, 'suggest', 5000)) {
        await safeAnswerCallbackQuery(q.id, { text: 'Please wait a few seconds.' }).catch(()=>{});
        return;
      }

      // clear spinner and provide a short notice
      await safeAnswerCallbackQuery(q.id, { text: 'Send your suggestion (reply to the prompt)' }).catch(()=>{});

      // send a force-reply prompt so next message is easy to capture
      const sent = await safeSendMessage(
        chatId,
        'Please type your suggestion and send it — it will be forwarded to the admin and saved.',
        {
          reply_markup: { force_reply: true },
          disable_web_page_preview: true
        }
      );

      const key = suggestKey(chatId, userId);

      pendingSuggests[key] = {
        user_id: userId,
        reply_to_message_id: sent && sent.message_id ? sent.message_id : null,
        createdAt: Date.now()
      };

      // auto-expire this particular (chat,user) entry
      setTimeout(() => {
        const p = pendingSuggests[key];
        if (p && Date.now() - p.createdAt > 6 * 60 * 1000) {
          delete pendingSuggests[key];
        }
      }, 6 * 60 * 1000);

      return;
    }

    // other callback handlers...

  } catch (err) {
    console.error('callback_query (suggest) error', err && err.message);
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error' }); } catch (_) {}
  }
});

// --- Rename batch command ---
// in-memory pending map for interactive renames (keyed by "chat:user")
const pendingRenames = {}; // { "<chatId>:<userId>": { stage, identifier, reply_to_message_id, createdAt } }

function renameKey(chatId, userId) {
  return `${chatId}:${userId}`;
}

/**
 * Try to resolve an identifier (token, filename, or display_name) to a batch filename.
 * Returns filename string or null if not found.
 */
function resolveBatchIdentifier(identifier) {
  if (!identifier) return null;
  identifier = String(identifier).trim();

  try {
    const idx = typeof readIndex === 'function' ? readIndex() : null;

    // 1) token -> filename (via idx.tokens)
    if (idx && idx.tokens && typeof idx.tokens === 'object') {
      if (idx.tokens[identifier]) return idx.tokens[identifier];
      if (idx.tokens[identifier.toLowerCase()]) return idx.tokens[identifier.toLowerCase()];
    }

    // 2) exact filename match
    if (typeof readBatchFile === 'function') {
      const maybe = readBatchFile(identifier);
      if (maybe) return identifier;
    }

    // 3) search by display_name among index.order
    if (idx && Array.isArray(idx.order)) {
      for (const fname of idx.order) {
        try {
          const b = typeof readBatchFile === 'function' ? readBatchFile(fname) : null;
          if (!b) continue;
          const disp = String(b.display_name || b.filename || '').trim();
          if (disp.toLowerCase() === identifier.toLowerCase()) return fname;
        } catch (e) {
          // ignore per-file read errors
        }
      }
    }
  } catch (e) {
    console.error('resolveBatchIdentifier error', e && e.message);
  }

  return null;
}

/**
 * Perform the rename: update batch.display_name and save it.
 * Returns true on success, false otherwise.
 *
 * This lets you go from:
 *   Baaghi 2_NB3WIVTV415G  →  🎬 Movie: Baaghi 2 [2018]
 * (internal filename stays Baaghi 2_NB3WIVTV415G, only display_name changes)
 */
function performRename(filename, newDisplayName) {
  if (!filename || !newDisplayName) return false;
  if (typeof readBatchFile !== 'function') {
    console.error('performRename: readBatchFile not available');
    return false;
  }

  const batch = readBatchFile(filename);
  if (!batch) {
    console.error('performRename: batch not found for', filename);
    return false;
  }

  batch.display_name = String(newDisplayName).trim();

  // we know writeBatchFile exists in your codebase
  if (typeof writeBatchFile === 'function') {
    try {
      writeBatchFile(filename, batch);
      console.log('performRename: updated display_name for', filename, '→', batch.display_name);
      return true;
    } catch (e) {
      console.error('writeBatchFile failed', e && e.message);
      return false;
    }
  } else {
    console.error('performRename: writeBatchFile not available — wire your saver here');
    return false;
  }
}

// ------------- /renamebatch command (one-shot or interactive) -------------
bot.onText(/^\/renamebatch(?:@\w+)?\s+([\s\S]+)$/i, async (msg, match) => {
  try {
    const fromId = msg.from && msg.from.id;
    const chatId = msg.chat && msg.chat.id;
    const adminId = typeof ADMIN_ID !== 'undefined' ? ADMIN_ID : null;

    if (!adminId || String(fromId) !== String(adminId)) {
      return safeSendMessage(chatId, 'Admin only.');
    }

    const raw = (match && match[1]) ? String(match[1]).trim() : '';
    if (!raw) {
      return safeSendMessage(
        chatId,
        'Usage: /renamebatch <identifier> | <New Name>\n' +
        'Or: /renamebatch <identifier> (you will be prompted)'
      );
    }

    // One-shot: "/renamebatch identifier | New Name"
    if (raw.includes('|')) {
      const parts = raw.split('|');
      const identifier = parts[0].trim();
      const newName = parts.slice(1).join('|').trim();
      if (!identifier || !newName) {
        return safeSendMessage(chatId, 'Usage: /renamebatch old_identifier | New Batch Name');
      }

      const filename = resolveBatchIdentifier(identifier);
      if (!filename) {
        return safeSendMessage(
          chatId,
          `Could not find a batch for <code>${escapeHtml(identifier)}</code>.`,
          { parse_mode: 'HTML' }
        );
      }

      const ok = performRename(filename, newName);
      if (ok) {
        return safeSendMessage(
          chatId,
          `✅ Renamed batch <b>${escapeHtml(filename)}</b> to:\n<b>${escapeHtml(newName)}</b>`,
          { parse_mode: 'HTML' }
        );
      } else {
        return safeSendMessage(
          chatId,
          '❌ Failed to rename. Check server logs.'
        );
      }
    }

    // Interactive: "/renamebatch <identifier>" -> ask only for new name
    const identifier = raw;
    const filename = resolveBatchIdentifier(identifier);
    if (!filename) {
      return safeSendMessage(
        chatId,
        `Could not find a batch for <code>${escapeHtml(identifier)}</code>.`,
        { parse_mode: 'HTML' }
      );
    }

    const prompt =
      `You are renaming batch: <b>${escapeHtml(filename)}</b>\n` +
      `Please reply to this message with the <b>new name</b>.\n\n` +
      `Example:\n<code>🎬 Movie: Baaghi 2 [2018]</code>`;
    const sent = await safeSendMessage(chatId, prompt, {
      parse_mode: 'HTML',
      reply_markup: { force_reply: true }
    });

    const key = renameKey(chatId, fromId);
    pendingRenames[key] = {
      stage: 'await_newname',
      identifier: filename,
      reply_to_message_id: sent && sent.message_id ? sent.message_id : null,
      createdAt: Date.now()
    };

    // auto-expire
    setTimeout(() => {
      const p = pendingRenames[key];
      if (p && Date.now() - p.createdAt > 6 * 60 * 1000) delete pendingRenames[key];
    }, 6 * 60 * 1000);

  } catch (err) {
    console.error('/renamebatch error', err && err.message);
    try { await safeSendMessage(msg.chat.id, 'Error processing rename command.'); } catch (_) {}
  }
});

// Finish interactive rename (new name only)
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.chat || !msg.from) return;

    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const key = renameKey(chatId, userId);
    const pending = pendingRenames[key];

    if (!pending) return;

    const isReply =
      msg.reply_to_message &&
      (msg.reply_to_message.message_id === pending.reply_to_message_id);

    if (!isReply) return;

    // stage: await_newname
    if (pending.stage === 'await_newname') {
      const newName = (msg.text || '').trim();
      if (!newName) {
        await safeSendMessage(chatId, '❌ Please send a non-empty new name.');
        delete pendingRenames[key];
        return;
      }

      const filename = pending.identifier;
      const ok = performRename(filename, newName);

      if (ok) {
        await safeSendMessage(
          chatId,
          `✅ Renamed batch <b>${escapeHtml(filename)}</b> to:\n<b>${escapeHtml(newName)}</b>`,
          { parse_mode: 'HTML' }
        );
      } else {
        await safeSendMessage(chatId, '❌ Rename failed. Check logs.');
      }

      delete pendingRenames[key];
    }

  } catch (err) {
    console.error('rename handler error', err && err.message);
  }
});

// --- catch messages that are replies to the prompt (or /suggest command usage below) ---
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.chat) return;
    const chatId = msg.chat.id;
    const userId = msg.from && msg.from.id;
    if (!userId) return;

    const key = suggestKey(chatId, userId);
    const pending = pendingSuggests[key];

    if (pending) {
      const isReplyToPrompt =
        msg.reply_to_message &&
        (msg.reply_to_message.message_id === pending.reply_to_message_id);

      const sameUser =
        pending.user_id &&
        Number(userId) === Number(pending.user_id);

      if (isReplyToPrompt || sameUser) {
        if (!msg.text || !msg.text.trim()) {
          await safeSendMessage(chatId, 'Please send a non-empty text suggestion.');
          return;
        }

        const rec = await storeAndForwardSuggestion(chatId, msg.from, msg.text.trim(), msg);
        await safeSendMessage(chatId, '✅ Thanks — your suggestion has been sent to the admin.');

        delete pendingSuggests[key];
        return;
      }
    }
    // ... other message handlers continue here ...

  } catch (err) {
    console.error('message handler (suggest) error', err && err.message);
  }
});

// --- direct /suggest command handler (users can do: /suggest I want X) ---
bot.onText(/^\/suggest(?:@\w+)?\s+([\s\S]+)/i, async (msg, match) => {
  try {
    const chatId = msg.chat.id;
    const text = (match && match[1]) ? String(match[1]).trim() : '';
    if (!text) return safeSendMessage(chatId, 'Usage: /suggest <your suggestion>');

    const rec = await storeAndForwardSuggestion(chatId, msg.from, text, msg);
    await safeSendMessage(chatId, '✅ Thanks — your suggestion has been sent to the admin.');
  } catch (err) {
    console.error('/suggest handler error', err && err.message);
    try { await safeSendMessage(msg.chat.id, 'Failed to submit suggestion.'); } catch (_) {}
  }
});

// --- admin-only command to list suggestions: /suggestions ---
bot.onText(/^\/suggestions(?:@\w+)?(?:\s+(\d+))?$/i, async (msg, match) => {
  try {
    const fromId = msg.from && msg.from.id;
    const chatId = msg.chat.id;
    const adminId = typeof ADMIN_ID !== 'undefined' ? ADMIN_ID : null;

    // admin guard
    if (!adminId || Number(fromId) !== Number(adminId)) {
      return safeSendMessage(chatId, 'Admin only.');
    }

    const limit = match && match[1]
      ? Math.min(100, Math.max(1, Number(match[1])))
      : 20;

    const list = (suggestions || []).slice(0, limit);

    if (!list.length) {
      return safeSendMessage(chatId, 'No suggestions stored yet.');
    }

    const lines = list.map((s, i) => {
      if (!s) {
        return `${i + 1}. [malformed entry]`;
      }

      const user = s.user || {};
      const uid = user.id || '?';

      const first = user.first_name ? escapeHtml(String(user.first_name)) : '';
      const uname = user.username ? '@' + escapeHtml(String(user.username)) : '';

      let name;
      if (first || uname) {
        name = (first + ' ' + uname).trim();
      } else {
        name = `User ${uid}`;
      }

      const txt = s.text ? shortText(escapeHtml(String(s.text)), 240) : '[no text]';
      const ts = s.createdAt || s.created_at || '';

      return `${i + 1}. ${name} • <code>${uid}</code>\n${txt}\n— ${ts}`;
    });

    const header = `<b>Latest suggestions (showing ${list.length})</b>\n\n`;
    await safeSendMessage(
      chatId,
      header + lines.join('\n\n'),
      { parse_mode: 'HTML', disable_web_page_preview: true }
    );

  } catch (err) {
    console.error('/suggestions handler error', err && err.message);
    try {
      await safeSendMessage(msg.chat.id, 'Error fetching suggestions.');
    } catch (_) {}
  }
});

// small utility to cut long text
function shortText(s, len=120) {
  if (!s) return '';
  return s.length > len ? s.slice(0, len-1) + '…' : s;
}

// ---------- message handler ----------
bot.on('message', async (msg) => {
  try {
    const chatId = msg.chat.id;
    const from = msg.from || {};
    const fromId = from.id;
    const text = msg.text || '';

    if (fromId) {
      const action = { type: (text && text.startsWith('/')) ? 'command' : 'message', text: text ? (text.slice(0, 1000)) : '', chat_type: msg.chat && msg.chat.type ? msg.chat.type : 'private' };
      recordUserAction({ id: fromId, username: from.username || null, first_name: from.first_name || null, last_name: from.last_name || null }, action);
    }
     // --- New command: /exportbatches ---
    if (text && text.startsWith('/exportbatches')) {
      // Only admin
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
        return safeSendMessage(chatId, 'Only admin may use /exportbatches.');
      }

      try {
        const idx = readIndex() || {};
        let order = Array.isArray(idx.order) ? idx.order.slice() : [];
        const tokensMap = idx.tokens || {};

        // fallback: if index has no order, try to read files from batches dir (if you keep one)
        if (!order.length) {
          try {
            const batchFiles = fs.readdirSync(BATCHES_DIR || path.join(__dirname, 'batches'))
              .filter(f => f.endsWith('.json') || f.endsWith('.js') || f.endsWith('.txt'));
            order = batchFiles;
          } catch (e) {
            // ignore - we'll simply produce empty export if nothing available
          }
        }

        // CSV helper to always quote and escape fields
        function csvQuote(val) {
          if (val === null || typeof val === 'undefined') val = '';
          const s = String(val);
          // double-up quotes and wrap in quotes
          return `"${s.replace(/"/g, '""')}"`;
        }

        // header
        const csvRows = [
          [
            'Display Name',
            'Internal Filename',
            'Token',
            'Access Link',
            'File Count',
            'Created At',
            'Admin ID'
          ].map(csvQuote).join(',')
        ];

        for (const filename of order) {
          const batch = readBatchFile(filename);
          if (!batch) continue;

          const token = Object.keys(tokensMap).find(t => tokensMap[t] === filename) || '';
          const displayName = batch.display_name || batch.title || batch.name || filename;
          const internalFilename = filename;
          const accessLink = BOT_USERNAME && token ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : '';
          const fileCount = Array.isArray(batch.files) ? batch.files.length : (batch.fileCount || 0);
          const createdAt = batch.createdAt || batch.date || '';
          const adminId = batch.adminId || '';

          csvRows.push([
            csvQuote(displayName),
            csvQuote(internalFilename),
            csvQuote(token),
            csvQuote(accessLink),
            csvQuote(fileCount),
            csvQuote(createdAt),
            csvQuote(adminId)
          ].join(','));
        }

        const csvContent = csvRows.join('\n');
        const buffer = Buffer.from(csvContent, 'utf8');

        // Send as a document. node-telegram-bot-api supports passing fileOptions as 4th arg.
        await bot.sendDocument(
          chatId,
          buffer,
          { caption: 'Here is your batches export (CSV).' , parse_mode: 'HTML' },
          { filename: 'batches_export.csv', contentType: 'text/csv' }
        );

        return;
      } catch (e) {
        console.error('Error exporting batches:', e && e.stack ? e.stack : e);
        return safeSendMessage(chatId, 'Failed to export batches: ' + (e && e.message ? e.message : String(e)));
      }
    }

    // ---------- admin commands (sendfile/addto/doneadd/doneaddto/edit_caption etc) ----------

    // Usage: /setdeletetimer <seconds>
    if (text && text.startsWith('/setdeletetimer')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /setdeletetimer.');
      const parts = text.trim().split(/\s+/);
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /setdeletetimer <seconds> (e.g. /setdeletetimer 3600)');
      const secs = Number(parts[1]);
      if (!Number.isFinite(secs) || secs <= 0) return safeSendMessage(chatId, 'Please provide a valid positive number of seconds.');
      const meta = readMeta() || {};
      meta.delete_after_seconds = Math.floor(secs);
      try {
        writeMeta(meta);
        const mins = (meta.delete_after_seconds / 60).toFixed(2);
        return safeSendMessage(chatId, `Delete timer set to ${meta.delete_after_seconds} seconds (${mins} mins).`);
      } catch (e) {
        console.error('writeMeta failed', e && e.message);
        return safeSendMessage(chatId, 'Failed to save timer setting.');
      }
    }

    if (text && text.startsWith('/sendfile')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /sendfile.');
      pendingBatches[chatId] = pendingBatches[chatId] || {};
      pendingBatches[chatId].awaitingFilename = true;
      return safeSendMessage(chatId, 'Send filename to save this batch as (no extension) — or just send files and the bot will auto-detect a name from the first file. Example: Surrender 2025\nIf you want auto-detect, just start uploading/forwarding files now and finish with /doneadd');
    }
    if (fromId === ADMIN_ID && pendingBatches[chatId] && pendingBatches[chatId].awaitingFilename && text && !text.startsWith('/')) {
      const filename = text.trim();
      const pending = startPendingBatch(chatId, filename);
      pendingBatches[chatId].awaitingFilename = false;
      return safeSendMessage(chatId, `Batch started as "${filename}" with token: /start_${pending.token}\nNow upload files, send text, or forward messages. When finished, send /doneadd`);
    }

    if (text && text.startsWith('/addto')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /addto.');
      const parts = text.split(/\s+/);
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /addto <TOKEN>');
      const token = parts[1].replace(/^\/start_?/, '').trim();
      const started = startPendingAddTo(chatId, token);
      if (!started) return safeSendMessage(chatId, 'Token not found.');
      return safeSendMessage(chatId, `Now forward or upload files/text to be appended to batch (token: ${token}). Finish with /doneaddto`);
    }
    if (text && text === '/doneaddto') {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /doneaddto.');
      const pending = pendingAddTo[chatId];
      if (!pending) return safeSendMessage(chatId, 'No pending add-to session. Start with /addto <TOKEN>');
      delete pendingAddTo[chatId];
      const batch = readBatchFile(pending.filename);
      if (!batch) return safeSendMessage(chatId, 'Batch not found after add.');
      return safeSendMessage(chatId, `Added ${pending.files.length} items to ${batch.display_name || batch.filename}.`);
    }

    if (text === '/doneadd') {
      if (fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may finish a batch.');
      const pending = pendingBatches[chatId];
      if (!pending) return safeSendMessage(chatId, 'No pending batch found. Start with /sendfile and then name the batch or upload files.');
      delete pendingBatches[chatId];

      const filename = pending.filename;
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(chatId, 'Batch finalized but could not find batch file.');

      const kb = { inline_keyboard: [] };

      // Row 1: Preview & Browse
      const row1 = [];
      if (BOT_USERNAME) {
        const link = `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}`;
        row1.push({ text: 'Preview (open batch)', url: link });
      }
      row1.push({ text: 'Browse preview', callback_data: 'browse_open_from_done' });
      kb.inline_keyboard.push(row1);

      // Row 2: Delete this batch (ADMIN only will be able to confirm)
      kb.inline_keyboard.push([
        { text: '🗑 Delete this batch', callback_data: `channel_delete_${batch.token}` }
      ]);

      // Row 3: Contact
      kb.inline_keyboard.push([
        { text: 'Contact Admin', url: 'https://t.me/aswinlalus' },
        { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
      ]);

      const previewText = batch.display_name ? `Saved ${batch.filename}\n${batch.display_name}` : `Saved ${batch.filename}`;
      await safeSendMessage(chatId, `${previewText}\nPreview link available. For new batch /sendfile.`, { reply_markup: kb });

      // --- Ask the admin what to do next (post / broadcast / both / skip) ---
      const actionKb = {
        inline_keyboard: [
          [
            { text: '📣 Post to channel', callback_data: `admin_post_channel_${batch.token}` },
            { text: '📢 Broadcast to users', callback_data: `admin_broadcast_${batch.token}` }
          ],
          [
            { text: '📣 + 📢 Post & Broadcast', callback_data: `admin_post_broadcast_${batch.token}` }
          ],
          [
            { text: '⏭ Skip', callback_data: `admin_skip_${batch.token}` }
          ]
        ]
      };

      // NOTE: Telegraph generation removed on purpose.
      // Previously you called: await sendAdminTelegraphIndex(batch, ADMIN_ID).catch(()=>{});
      // That call has been intentionally removed to avoid automatic Telegraph creation.

      // then send your existing action prompt (post / broadcast / both / skip)
      await safeSendMessage(chatId, 'Choose what to do with this batch — post to channel, broadcast to users, both, or skip.', { reply_markup: actionKb });

      return;
    }

    // Admin command: build Telegraph index/pages on demand
    bot.onText(/^\/buildtelegraph(?:@\w+)?(?:\s*(force)?)?$/i, async (msg, match) => {
      const chatId = msg.chat.id;
      const fromId = msg.from && msg.from.id;
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');

      const force = !!(match && match[1] && match[1].toLowerCase() === 'force');

      // Optional: show a processing message if you use showProcessing in your file
      let processing = null;
      if (typeof showProcessing === 'function') {
        try { processing = await showProcessing(chatId, '⏳ Building Telegraph index/pages…').catch(()=>null); } catch (_) { processing = null; }
      } else {
        await safeSendMessage(chatId, 'Building Telegraph index/pages… (this may take a moment)');
      }

      try {
        // If you want to only build for flagged batches, you can read meta.telegraph_needs_update
        // and pass a currentBatch or call createTelegraphIndexPages() globally.
        // For simplicity, we'll call global creation (the function itself tries to be incremental/cached).
        const urls = await createTelegraphIndexPages(/* currentBatch */ undefined, { force: !!force });

        if (!Array.isArray(urls) || urls.length === 0) {
          await safeSendMessage(chatId, 'No Telegraph pages were produced (nothing to publish).');
        } else {
          let txt = `Telegraph index/pages created (${urls.length} pages):\n\n`;
          urls.forEach((u, i) => { txt += `Page ${i+1}: ${u}\n`; });
          await safeSendMessage(chatId, txt);
        }

        // Clear the telegraph_needs_update flags (optional — only if creation succeeded)
        try {
          const meta = readMeta() || {};
          if (meta.telegraph_needs_update) {
            meta.telegraph_needs_update = {}; // clear all. if you want more nuanced clearing, adjust here.
            writeMeta(meta);
          }
        } catch (e) { console.warn('Could not clear telegraph_needs_update after build:', e && e.message); }
      } catch (e) {
        console.error('/buildtelegraph failed', e && (e.stack || e.message));
        await safeSendMessage(chatId, 'Failed to build Telegraph pages: ' + (e && e.message ? e.message : 'unknown'));
      } finally {
        if (processing && typeof processing.done === 'function') {
          try { await processing.done(); } catch(_) {}
        }
      }
    });

    // edit caption flow
    if (text && text.startsWith('/edit_caption')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /edit_caption.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /edit_caption <TOKEN>');
      const token = parts[1].replace(/^\/start_?/,'').trim();
      const idx = readIndex();
      const filename = idx.tokens[token];
      if (!filename) return safeSendMessage(chatId, 'Token not found.');
      const batch = readBatchFile(filename);
      if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch has no files.');
      pendingBatches[chatId] = pendingBatches[chatId] || {};
      pendingBatches[chatId].editCaptionFlow = { token, filename, stage: 'await_index' };
      let list = `Editing captions for ${batch.filename} (token: ${token}). Files:\n`;
      batch.files.forEach((f,i)=> { const n = f.file_name || (f.caption ? (f.caption.split(/\r?\n/)[0].slice(0,50)) : 'text'); list += `${i+1}. ${n}\n`; });
      list += '\nReply with the file number to edit (1..' + batch.files.length + ')';
      return safeSendMessage(chatId, list);
    }
    if (pendingBatches[chatId] && pendingBatches[chatId].editCaptionFlow && pendingBatches[chatId].editCaptionFlow.stage === 'await_index' && fromId === ADMIN_ID && text && !text.startsWith('/')) {
      const flow = pendingBatches[chatId].editCaptionFlow;
      const idxNum = Number(text.trim());
      const batch = readBatchFile(flow.filename);
      if (isNaN(idxNum) || idxNum < 1 || idxNum > (batch.files.length||0)) {
        return safeSendMessage(chatId, 'Invalid number. Please send a number between 1 and ' + (batch.files.length||0));
      }
      flow.fileIndex = idxNum - 1;
      flow.stage = 'await_caption';
      pendingBatches[chatId].editCaptionFlow = flow;
      return safeSendMessage(chatId, `Send the new caption for file #${idxNum} (you can include Storyline: etc)`);
    }
    if (pendingBatches[chatId] && pendingBatches[chatId].editCaptionFlow && pendingBatches[chatId].editCaptionFlow.stage === 'await_caption' && fromId === ADMIN_ID && text) {
      const flow = pendingBatches[chatId].editCaptionFlow;
      const batch = readBatchFile(flow.filename);
      batch.files[flow.fileIndex].caption = text;
      writeBatchFile(flow.filename, batch);
      delete pendingBatches[chatId].editCaptionFlow;
      const preview = formatCaptionHtmlForPreview(text);
      await safeSendMessage(chatId, 'Caption updated. Preview (first lines):');
      await safeSendMessage(chatId, preview, { parse_mode: 'HTML' });
      return;
    }

// /listfiles with pagination (e.g., "/listfiles 2")
const PAGE_SIZE = 10;

if (text && text.startsWith('/listfiles')) {
  // Admin check (safer string comparison)
  if (!ADMIN_ID || String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Only admin may use /listfiles.');
  }

  const idx = readIndex();
  const order = (idx && Array.isArray(idx.order)) ? idx.order : [];
  if (order.length === 0) {
    return safeSendMessage(chatId, 'No batches found.');
  }

  // Parse optional page arg: "/listfiles 2"
  const parts = text.trim().split(/\s+/);
  const requestedPage = parts[1] ? parseInt(parts[1], 10) : 1;

  const total = order.length;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const page = Math.min(totalPages, Math.max(1, isNaN(requestedPage) ? 1 : requestedPage));

  const start = (page - 1) * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, total);

  // Tiny inline escaper (scoped to this handler)
  const h = (s) => {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, (ch) =>
      ch === '&' ? '&amp;'
      : ch === '<' ? '&lt;'
      : ch === '>' ? '&gt;'
      : ch === '"' ? '&quot;'
      : '&#39;'
    );
  };

  let out = `Batches (send order) — page ${page}/${totalPages}:\n\n`;

  order.slice(start, end).forEach((fname, i) => {
    const token = (idx && idx.tokens)
      ? Object.keys(idx.tokens).find(t => idx.tokens[t] === fname) || ''
      : '';

    const batch = (typeof readBatchFile === 'function') ? readBatchFile(fname) : null;
    const name = (batch && batch.display_name) ? batch.display_name : fname;
    const n = start + i + 1;

    // Format:
    // 1. DisplayName — filename | token
    //    /start_TOKEN
    //    <code>/deletefile TOKEN</code>
    out += `${n}. ${h(name)} — <code>${h(fname)}</code> | <code>${h(token)}</code>\n`;
    out += `   /start_${h(token)}\n`;
    out += `   <code>/deletefile ${h(token)}</code>\n\n`;
  });

  if (totalPages > 1) {
    out += `Navigate:\n`;
    if (page > 1) out += `← Prev: /listfiles ${page - 1}\n`;
    if (page < totalPages) out += `Next → /listfiles ${page + 1}\n`;
  }

  return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
}

// deletefile
if (text && text.startsWith('/deletefile')) {
  if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may delete.');
  const parts = text.split(' ');
  if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /deletefile <TOKEN>');
  const token = parts[1].trim().replace(/^\/start_?/, '');
  const idx = readIndex();
  const filename = idx.tokens[token];
  if (!filename) return safeSendMessage(chatId, 'Token not found');
  const filePath = filenameToPath(filename);
  try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); delete idx.tokens[token]; idx.order = idx.order.filter(f=>f!==filename); writeIndex(idx); const meta = readMeta(); if (meta.batch_meta) { delete meta.batch_meta[filename]; writeMeta(meta); } return safeSendMessage(chatId, `Deleted ${filename} (token ${token})`); } catch(e){ console.error(e); return safeSendMessage(chatId, 'Delete failed: '+(e && e.message)); }
}

// ----------/start (no token) + /start (with token) + /help (enhanced with tags) ----------
if (text && (text === '/start' || text.trim() === `/start@${BOT_USERNAME}`)) {
    // read saved meta to get telegraph url if published
    const meta = typeof getMsgMeta === 'function' ? getMsgMeta() : (global.meta || {});
    const telegraphUrl = meta && meta.msg_telegraph && meta.msg_telegraph.url ? meta.msg_telegraph.url : null;

    const kb = {
      inline_keyboard: [
        [
          { text: '🧭 Browse', callback_data: 'browse_open' },
          { text: '🔎 Search for Movie/Series', switch_inline_query_current_chat: '' }
        ],
        [
          { text: '📡 Channel Messages', callback_data: 'chls' }
        ],
        [
          { text: '❓ Help', callback_data: 'help' }
        ],
        [
          { text: '📨 Contact Admin', url: ADMIN_CONTACT_URL },
          { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
        ]
      ]
    };

    return safeSendMessage(
      chatId,
      `Use Browse to preview latest uploads or use inline search (type @${BOT_USERNAME} in any chat).`,
      { reply_markup: kb }
    );
}

// Replace the token-missing / token-not-found keyboards to include the new Help callback
const meta = typeof getMsgMeta === 'function' ? getMsgMeta() : (global.meta || {});
const telegraphUrl = meta && meta.msg_telegraph && meta.msg_telegraph.url ? meta.msg_telegraph.url : null;

const commonTokenMissingKb = {
  inline_keyboard: [
    [
      { text: '🧭 Browse', callback_data: 'browse_open' },
      { text: '🔎 Search for Movie/Series', switch_inline_query_current_chat: '' }
    ],
    [
      { text: '🗂️ Index', callback_data: 'open_index' },
     { text: '❓ Help', callback_data: 'help' } // keep Help visible when no telegraph; else Messages link present
    ],
    [
      { text: '📨 Contact Admin', url: ADMIN_CONTACT_URL },
      { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
    ]
  ]
};

// /start with token - show batch files (replacement with scheduled deletion & notice)
if (text && text.startsWith('/start')) {
  const m = text.match(/^\/start(?:@[\w_]+)?(?:[_ ](.+))?$/);
  const payload = (m && m[1]) ? m[1].trim() : '';

  const kb = {
    inline_keyboard: [
      [
        { text: '🧭 Browse', callback_data: 'browse_open' },
        { text: '🔎 Search for Movie/Series', switch_inline_query_current_chat: '' }
      ],
      [
        { text: '🗂️ index', callback_data: 'open_index' }
      ],
      [
        { text: '📨 Contact Admin', url: ADMIN_CONTACT_URL },
        { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
      ]
    ]
  };

  // No payload -> show main menu
  if (!payload) {
    return safeSendMessage(
      chatId,
      'Token missing. Use Browse, Tags, or inline search.',
      { reply_markup: kb }
    );
  }

  // --- NEW: handle channel deep-link payloads: CHMSG_<index> ---
  // Links like: https://t.me/<BOT_USERNAME>?start=CHMSG_3
  if (/^CHMSG[_-]?\d+$/i.test(payload)) {
    // extract index number
    let idx = null;
    const mCh = payload.match(/^CHMSG[_-]?(\d+)$/i);
    if (mCh && mCh[1]) {
      idx = Number(mCh[1]);
    }
    if (!Number.isFinite(idx)) {
      return safeSendMessage(chatId, 'Invalid channel message link.');
    }

    // direct deep-link — force-show hidden items because the user explicitly used the shared link
    return sendChannelForwardByIndex(chatId, idx, { force: true });
  }
  // --- END CHMSG HANDLER ---

  const token = payload;

  // Check if this payload is a message-store token (MSG-*)
  try {
    const meta = getMsgMeta();
    if (meta && meta.msg_tokens && meta.msg_tokens[token]) {
      const mt = meta.msg_tokens[token];
      const arr = meta.saved_texts && meta.saved_texts[mt.key] ? meta.saved_texts[mt.key] : [];
      const item = arr && arr[mt.index] ? arr[mt.index] : null;
      if (!item) {
        return safeSendMessage(chatId, 'Message not found for this token.');
      }
      // increment simple view counter
      try { incrementMsgView(token); } catch (e) {}
      return safeSendMessage(chatId, item, { parse_mode: 'HTML' });
    }
  } catch (e) {
    // ignore and fall through to batch tokens
  }

  const idxFile = readIndex();
  const filename = (idxFile && idxFile.tokens) ? idxFile.tokens[token] : null;
  if (!filename) {
    return safeSendMessage(
      chatId,
      'Token not found. Try Browse, Tags, or inline search.',
      { reply_markup: kb }
    );
  }

  const batch = readBatchFile(filename);
  if (!batch) {
    return safeSendMessage(chatId, 'Batch missing.');
  }

      // send files and capture message ids so we can delete them later
      const sentMessageIds = []; // This will contain file message IDs
        for (let i = 0; i < (batch.files || []).length; i++) {
          try {
            const file = batch.files[i];
            if (!file) continue;
            // Friendly UX: show typing / upload action before sending
            try {
              const action = (file.type === 'photo') ? 'upload_photo' : 'upload_document';
              await bot.sendChatAction(chatId, action).catch(()=>{});
            } catch (_) {}
            // Send the single file (sendBatchItemToChat handles types & fallbacks)
            const sent = await sendBatchItemToChat(chatId, batch, file);
            // Collect any message ids returned by different helpers (send/copy/edit)
            if (sent) {
              if (sent.message_id || sent.message_id === 0) sentMessageIds.push(sent.message_id);
              if (sent.edit && sent.message_id) sentMessageIds.push(sent.message_id);
              if (sent.newMessage && sent.newMessage.message_id) sentMessageIds.push(sent.newMessage.message_id);
            }
          } catch (e) {
            console.warn('send in /start loop failed', e && e.message);
          }
          await sleep(120);
        }

      // rating keyboard
      const row1 = [], row2 = [];
      for (let s = 1; s <= 5; s++) row1.push({ text: `${s}⭐`, callback_data: `rate_${batch.token}_${s}` });
      for (let s = 6; s <= 10; s++) row2.push({ text: `${s}⭐`, callback_data: `rate_${batch.token}_${s}` });

      // send rating prompt AND CAPTURE ITS MESSAGE ID
      let ratingMessage = null;
      try {
          ratingMessage = await safeSendMessage(chatId, 'Rate this batch (1–10):', { reply_markup: { inline_keyboard: [row1, row2] } });
          if (ratingMessage && ratingMessage.message_id) {
              sentMessageIds.push(ratingMessage.message_id); // Add rating message to deletion list
          }
      } catch (e) {
          console.warn('Failed to send rating message:', e.message);
      }


      // schedule deletion for all collected message IDs (files + rating message)
      try {
        const meta = readMeta() || {};
        const deleteSeconds = Number(meta.delete_after_seconds || 3600); // default 1 hour
        
        // Pass the combined list of message IDs, including the rating message
        scheduleDeletionForMessages(chatId, sentMessageIds, deleteSeconds, token);
      } catch (e) {
        console.warn('scheduling deletion failed', e && e.message);
      }

      return;
    };

    if (text && (text === '/help' || text.toLowerCase() === 'help')) {
      const kbUser = {
        inline_keyboard: [
          [
            { text: '👤 User', callback_data: 'help_user' },
            { text: '🛠️ Admin', callback_data: 'help_admin' }
          ],
          [
            { text: '📚 Browse latest', callback_data: 'open_index' }
          ],
          [
            { text: '🔎 Search by token', callback_data: 'open_token_prompt' }
          ],
          [
            { text: '🔎 Try Searching', switch_inline_query_current_chat: '' }
          ],
          [
            { text: '📨 Contact Admin', url: ADMIN_CONTACT_URL },
            { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
          ]
        ]
      };

      const userHelpText = `Need help? Use the buttons to browse batches, search by token, or browse by tags.`;
      return safeSendMessage(chatId, userHelpText, { reply_markup: kbUser });
    }

    // /browse -> preview latest
    if (text && text === '/browse') {
      const idx = readIndex();
      const order = idx.order || [];
      if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches available.');
      const lastIndex = order.length - 1;
      const filename = order[lastIndex];
      const batch = readBatchFile(filename);
      if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Latest batch has no files.');
      const firstFile = batch.files[0];
      const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
      let baseMsg;
      try {
        if (firstFile.type === 'photo' && firstFile.file_id) baseMsg = await bot.sendPhoto(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'document' && firstFile.file_id) baseMsg = await bot.sendDocument(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'video' && firstFile.file_id) baseMsg = await bot.sendVideo(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
        else if (firstFile.type === 'text' && firstFile.text) baseMsg = await bot.sendMessage(chatId, captionHtml ? captionHtml : formatCaptionHtmlForPreview(firstFile.text), { parse_mode: 'HTML' });
        else if (firstFile.type === 'forward' && firstFile.source_chat_id && firstFile.source_message_id) {
          try { baseMsg = await bot.copyMessage(chatId, firstFile.source_chat_id, firstFile.source_message_id); } catch (e) { baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{}); }
        } else baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{});
      } catch (e) { console.warn('browse send failed', e); baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml?{parse_mode:'HTML'}:{}); }
      browseSessions[chatId] = { pos: lastIndex, order: order, messageId: baseMsg.message_id };
      const kb = makeBrowseKeyboardForIndex(lastIndex, order.length, batch.token);
      try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch(_){} }
      return;
    }

// listusers/getuser
    if (text && text.startsWith('/listusers')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /listusers.');
      try {
        const files = fs.readdirSync(USER_DIR).filter(f => f.endsWith('.js'));
        if (!files.length) return safeSendMessage(chatId, 'No users recorded yet.');

        // Parse page number: /listusers 2
        const parts = text.split(/\s+/);
        let page = 1;
        if (parts[1] && /^\d+$/.test(parts[1])) {
          page = parseInt(parts[1], 10);
        }

        const PAGE_SIZE = 25; // Keep message size safe
        const totalUsers = files.length;
        const totalPages = Math.ceil(totalUsers / PAGE_SIZE);

        if (page < 1) page = 1;
        if (page > totalPages) page = totalPages;

        const start = (page - 1) * PAGE_SIZE;
        const end = start + PAGE_SIZE;
        const sliced = files.slice(start, end);

        // small helper to escape HTML locally
        function esc(s) {
          return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }

        let out = `<b>Known users (Page ${page}/${totalPages})</b>\nTotal: ${totalUsers}\n\n`;
        let idx = start;
        
        for (const file of sliced) {
          try {
            const p = path.join(USER_DIR, file);
            delete require.cache[require.resolve(p)];
            const u = require(p);
            idx += 1;
            const display = u.username ? ('@' + esc(u.username)) : (u.first_name ? esc(u.first_name) : 'unknown');
            const idMono = `<code>${esc(u.id)}</code>`;
            const actionsCount = Array.isArray(u.actions) ? u.actions.length : (u.actions ? Number(u.actions) : 0);
            out += `${idx}. ${display} - id: ${idMono} - actions: ${actionsCount}\n`;
          } catch (e) {
            // skip malformed user file
          }
        }

        // Add navigation footer
        if (totalPages > 1) {
          out += `\n<b>Navigate:</b>\n`;
          if (page > 1) out += `⬅️ Prev: /listusers ${page - 1}\n`;
          if (page < totalPages) out += `➡️ Next: /listusers ${page + 1}`;
        }

        // use HTML parse mode so <code> works
        return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
      } catch (e) {
        console.error(e);
        return safeSendMessage(chatId, 'Failed to list users: ' + (e && e.message));
      }
    }

    if (text && text.startsWith('/getuser')) {
      if (ADMIN_ID && fromId !== ADMIN_ID) return safeSendMessage(chatId, 'Only admin may use /getuser.');
      const parts = text.split(' ');
      if (parts.length < 2) return safeSendMessage(chatId, 'Usage: /getuser <userId>');
      const uid = parts[1].trim();
      const p = path.join(USER_DIR, `${uid}.js`);
      try {
        delete require.cache[require.resolve(p)];
        const u = require(p);
        let out = `User ${u.username ? '@'+u.username : ''} id: ${u.id}\nName: ${u.first_name || ''} ${u.last_name || ''}\nActions (${(u.actions||[]).length}):\n`;
        (u.actions||[]).slice(-50).forEach((a,i)=>{ out += `${i+1}. [${a.ts}] ${a.type} ${a.text ? (' — '+ (a.text.length>100? a.text.slice(0,100)+'…':a.text)) : ''}\n`; });
        return safeSendMessage(chatId, out);
      } catch (e) { return safeSendMessage(chatId, 'User not found or read error.'); }
    }

    // Admin uploads/forwards while pending are handled below (in media section)
    const isMedia = !!(
      msg.document ||
      msg.photo ||
      msg.video ||
      msg.audio ||
      msg.voice ||
      msg.caption ||
      msg.forward_from ||
      msg.forward_from_chat ||
      (msg.text && !msg.text.startsWith('/'))
    );

    if (fromId === ADMIN_ID && isMedia) {
      // 1) Highest priority: /addto flow (append to existing batch)
      if (pendingAddTo[chatId]) {
        const pending = pendingAddTo[chatId];
        const fileMeta = {};
        if (msg.caption) fileMeta.caption = msg.caption;
        if (msg.document) {
          fileMeta.type = 'document';
          fileMeta.file_id = msg.document.file_id;
          fileMeta.file_name = msg.document.file_name;
          fileMeta.mime_type = msg.document.mime_type;
          fileMeta.size = msg.document.file_size;
        } else if (msg.photo) {
          const photo = msg.photo[msg.photo.length - 1];
          fileMeta.type = 'photo';
          fileMeta.file_id = photo.file_id;
          fileMeta.mime_type = 'image/jpeg';
          fileMeta.size = photo.file_size;
        } else if (msg.video) {
          fileMeta.type = 'video';
          fileMeta.file_id = msg.video.file_id;
          fileMeta.mime_type = msg.video.mime_type;
          fileMeta.size = msg.video.file_size;
        } else if (msg.audio) {
          fileMeta.type = 'audio';
          fileMeta.file_id = msg.audio.file_id;
          fileMeta.mime_type = msg.audio.mime_type;
          fileMeta.size = msg.audio.file_size;
        } else if (msg.voice) {
          fileMeta.type = 'audio';
          fileMeta.file_id = msg.voice.file_id;
          fileMeta.mime_type = msg.voice.mime_type;
          fileMeta.size = msg.voice.file_size;
        } else if (msg.forward_from || msg.forward_from_chat) {
          fileMeta.type = 'forward';
          fileMeta.source_chat_id =
            (msg.forward_from_chat && msg.forward_from_chat.id) ||
            (msg.forward_from && msg.forward_from.id) ||
            null;
          fileMeta.source_message_id = msg.forward_from_message_id || msg.message_id || null;
          if (msg.caption) fileMeta.caption = msg.caption;
          if (msg.document) {
            fileMeta.file_id = msg.document.file_id;
            fileMeta.file_name = msg.document.file_name;
            fileMeta.mime_type = msg.document.mime_type;
          }
          if (msg.photo) {
            const photo = msg.photo[msg.photo.length - 1];
            fileMeta.file_id = photo.file_id;
          }
        } else if (msg.text && !msg.text.startsWith('/')) {
          fileMeta.type = 'text';
          fileMeta.text = msg.text;
        } else {
          fileMeta.type = 'unknown';
        }

        try {
          const appended = await addFileToExistingBatch(chatId, pending.token, fileMeta);
          pending.files.push(fileMeta);
          await safeSendMessage(
            chatId,
            `Appended item to batch "${appended.display_name || appended.filename}" (now total ${appended.files.length}).`
          );
        } catch (e) {
          console.warn('append failed', e && e.message);
          await safeSendMessage(chatId, 'Failed to append item.');
        }
        return;
      }

      // 2) New batch flow: ONLY if a /sendfile session is active
      let pending = pendingBatches[chatId];

      // ❗ If there is no pending /sendfile session, do NOT auto-create a batch
      if (!pending) {
        // No /sendfile active -> ignore this media for batch logic
        return;
      }

      // If /sendfile was used and we're waiting for a name (auto-detect case)
      // pending.awaitingFilename is set by /sendfile
      if (!pending.token) {
        // We have a /sendfile session but no actual batch yet -> create one now (auto name)
        pending = startPendingBatch(chatId, '');
        pendingBatches[chatId] = pending;
        pending.awaitingFilename = false;
      } else if (pending.awaitingFilename) {
        // We already have a batch, but were "awaiting filename" – first media confirms, so just stop waiting
        pending.awaitingFilename = false;
      }

      // Now we are guaranteed to have a real pending batch with a token/filename
      const fileMeta = {};
      if (msg.caption) fileMeta.caption = msg.caption;
      if (msg.document) {
        fileMeta.type = 'document';
        fileMeta.file_id = msg.document.file_id;
        fileMeta.file_name = msg.document.file_name;
        fileMeta.mime_type = msg.document.mime_type;
        fileMeta.size = msg.document.file_size;
      } else if (msg.photo) {
        const photo = msg.photo[msg.photo.length - 1];
        fileMeta.type = 'photo';
        fileMeta.file_id = photo.file_id;
        fileMeta.mime_type = 'image/jpeg';
        fileMeta.size = photo.file_size;
      } else if (msg.video) {
        fileMeta.type = 'video';
        fileMeta.file_id = msg.video.file_id;
        fileMeta.mime_type = msg.video.mime_type;
        fileMeta.size = msg.video.file_size;
      } else if (msg.audio) {
        fileMeta.type = 'audio';
        fileMeta.file_id = msg.audio.file_id;
        fileMeta.mime_type = msg.audio.mime_type;
        fileMeta.size = msg.audio.file_size;
      } else if (msg.voice) {
        fileMeta.type = 'audio';
        fileMeta.file_id = msg.voice.file_id;
        fileMeta.mime_type = msg.voice.mime_type;
        fileMeta.size = msg.voice.file_size;
      } else if (msg.forward_from || msg.forward_from_chat) {
        fileMeta.type = 'forward';
        fileMeta.source_chat_id =
          (msg.forward_from_chat && msg.forward_from_chat.id) ||
          (msg.forward_from && msg.forward_from.id) ||
          null;
        fileMeta.source_message_id = msg.forward_from_message_id || msg.message_id || null;
        if (msg.caption) fileMeta.caption = msg.caption;
        if (msg.document) {
          fileMeta.file_id = msg.document.file_id;
          fileMeta.file_name = msg.document.file_name;
          fileMeta.mime_type = msg.document.mime_type;
        }
        if (msg.photo) {
          const photo = msg.photo[msg.photo.length - 1];
          fileMeta.file_id = photo.file_id;
        }
      } else if (msg.text && !msg.text.startsWith('/')) {
        fileMeta.type = 'text';
        fileMeta.text = msg.text;
      } else {
        fileMeta.type = 'unknown';
      }

      try {
        const updatedBatch = await addFileToPending(chatId, fileMeta);
        const count = updatedBatch && updatedBatch.files ? updatedBatch.files.length : '?';
        await safeSendMessage(
          chatId,
          `Added item to batch "${updatedBatch.display_name || updatedBatch.filename}" (total items: ${count}). If done /doneadd.\nDelete this file: <code>/deletefile ${updatedBatch.token}</code>`,
          { parse_mode: 'HTML' }
        );
      } catch (e) {
        console.warn('Failed to add file to pending', e && e.message);
        await safeSendMessage(chatId, 'Failed to add file to batch.');
      }
      return;
    }

  } catch (err) { console.error('on message error', err && (err.stack || err.message)); }
});

// keep a small in-memory map for pending token prompts (chatId -> prompt info)
const pendingTokenPrompts = {}; // { [chatId]: { reply_to_message_id, user_id, createdAt } }

// 1) Example: keyboard with the button
const tokenButtonKeyboard = {
  inline_keyboard: [
    [ { text: '🔎 Search by token', callback_data: 'open_token_prompt' } ]
  ]
};

// (example) sending keyboard somewhere
// await safeSendMessage(chatId, 'Choose an action:', { reply_markup: tokenButtonKeyboard });

// 2) callback_query handler (add to your existing callback_query router)
bot.on('callback_query', async (q) => {
  try {
    if (!q || !q.data) return;

    // handle our token search button
    if (q.data === 'open_token_prompt') {
      const chatId = q.message ? q.message.chat.id : (q.from && q.from.id);
      if (!chatId) {
        // best-effort clear spinner
        await safeAnswerCallbackQuery(q.id, { text: 'Unable to open prompt' }).catch(()=>{});
        return;
      }

      // clear spinner and give immediate feedback
      await safeAnswerCallbackQuery(q.id, { text: 'Send token — replying to the prompt' }).catch(()=>{});

      // send the prompt and force a reply so we can catch it easily
      const prompt = 'Please send the token now (or reply to this message).';
      const sent = await safeSendMessage(chatId, prompt, {
        reply_markup: { force_reply: true },
        disable_web_page_preview: true
      });

      // remember the prompt so we can validate the next reply
      pendingTokenPrompts[chatId] = {
        reply_to_message_id: sent && sent.message_id ? sent.message_id : null,
        user_id: q.from && q.from.id,
        createdAt: Date.now()
      };

      // optional: auto-expire after N ms to avoid memory leak
      setTimeout(() => { if (pendingTokenPrompts[chatId] && Date.now() - pendingTokenPrompts[chatId].createdAt > 5 * 60 * 1000) delete pendingTokenPrompts[chatId]; }, 6 * 60 * 1000);

      return;
    }

    // other callback handlers...
  } catch (err) {
    console.error('callback_query handler error', err && err.message);
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error' }); } catch (_) {}
  }
});

// 3) message handler to catch the forced replies / replies to the prompt
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.chat) return;
    const chatId = msg.chat.id;

    const pending = pendingTokenPrompts[chatId];
    if (!pending) return; // nothing pending for this chat

    // if the prompt required a reply_to_message, ensure the user replied to it OR the message is plain text from same user
    const isReplyToPrompt = msg.reply_to_message && (msg.reply_to_message.message_id === pending.reply_to_message_id);
    const isSameUser = msg.from && pending.user_id && Number(msg.from.id) === Number(pending.user_id);

    if (!isReplyToPrompt && !isSameUser) {
      // someone else or not a reply — ignore
      return;
    }

    if (!msg.text || !msg.text.trim()) {
      await safeSendMessage(chatId, 'Please send a non-empty token (text).');
      return;
    }

    const token = msg.text.trim();

    // 🔎 Lookup logic — adapt to your index structure
    // I assume you have a readIndex() function that returns an object with `tokens` mapping token->filename
    const idx = typeof readIndex === 'function' ? readIndex() : null;
    const tokensMap = idx && idx.tokens ? idx.tokens : {};

    const filename = tokensMap[token] || tokensMap[String(token).toLowerCase()] || null;

    if (filename) {
      // found — respond with whatever behaviour you want (link, open, send file)
      // Example: send a link to /start <token> (works in private chats)
      const botUsername = typeof BOT_USERNAME !== 'undefined' ? BOT_USERNAME : '';
      const startUrl = botUsername ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : `https://t.me/?start=${encodeURIComponent(token)}`;

      await safeSendMessage(chatId, `✅ Token found for <code>${escapeHtml(token)}</code>\nFile: <b>${escapeHtml(filename)}</b>\nOpen: ${startUrl}`, {
        parse_mode: 'HTML',
        disable_web_page_preview: true
      });

      // optionally: send the file directly, if you have a helper like sendFileByName(filename)
      // await sendFileByName(chatId, filename);
    } else {
      await safeSendMessage(chatId, `❌ Token not found: <code>${escapeHtml(token)}</code>`, { parse_mode: 'HTML' });
    }

    // clear pending
    delete pendingTokenPrompts[chatId];
  } catch (err) {
    console.error('token search message handler error', err && err.message);
  }
});

// ---------- inline query handler ----------
bot.on('inline_query', async (q) => {
  try {
    const qid = q.id;
    const query = (q.query || '').trim();
    const idxObj = readIndex();
    const results = [];
    const tokens = Object.keys(idxObj.tokens || {});
    let candidates = [];

    // Build candidate list (recent if no query, else fuzzy by name/caption)
    if (!query) {
      const order = idxObj.order || [];
      const recent = order.slice(-24).reverse();
      for (const fname of recent) {
        const token = Object.keys(idxObj.tokens || {}).find(t => idxObj.tokens[t] === fname);
        const batch = readBatchFile(fname); if (!batch) continue;
        candidates.push({ token, batch });
      }
    } else {
      const qLower = query.toLowerCase();
      for (const t of tokens) {
        const fname = idxObj.tokens[t];
        const batch = readBatchFile(fname); if (!batch) continue;
        const name = (batch.display_name || batch.filename || '').toLowerCase();
        const cap0 = ((batch.files && batch.files[0] && (batch.files[0].caption || batch.files[0].text)) || '').toLowerCase();
        if (name.includes(qLower) || cap0.includes(qLower)) {
          candidates.push({ token: t, batch });
        }
      }
    }

    // try to get a usable URL string for thumbnail; return null otherwise
    async function tryThumb(file) {
      if (!file) return null;
      const fid =
        file.file_id ||
        (file.thumb && file.thumb.file_id) ||
        (Array.isArray(file.photo) && file.photo.length ? file.photo[file.photo.length - 1].file_id : null);
      if (!fid) return null;
      try {
        const url = await bot.getFileLink(fid);
        // ensure it's a proper string http/https URL
        if (typeof url === 'string' && /^https?:\/\//i.test(url)) return url;
        return null;
      } catch { return null; }
    }

    function extractMeta(batch) {
      const meta = batch.meta || batch.metadata || batch.info || {};
      let year = meta.year || null;
      if (!year) {
        const m = String(batch.display_name || batch.filename || '').match(/\b(19|20)\d{2}\b/);
        if (m) year = m[0];
      }
      let genres = null;
      if (Array.isArray(meta.genres)) genres = meta.genres.join(', ');
      else if (typeof meta.genre === 'string') genres = meta.genre;
      else if (meta.category) genres = meta.category;
      return { year, genres };
    }

    for (let i = 0; i < Math.min(25, candidates.length); i++) {
      const c = candidates[i];
      const id = `res_${c.token}_${i}`;
      const display = c.batch.display_name || c.batch.filename || 'Untitled';
      const title = display.length > 64 ? display.slice(0, 61) + '…' : display; // Telegram title limit ~64

      const filesCount = Array.isArray(c.batch.files) ? c.batch.files.length : 0;
      const { year, genres } = extractMeta(c.batch);
      const descParts = [];
      if (year) descParts.push(year);
      if (genres) descParts.push(genres);
      if (filesCount) descParts.push(`${filesCount} file${filesCount > 1 ? 's' : ''}`);
      let description = descParts.join(' • ') || 'Open batch';
      if (description.length > 256) description = description.slice(0, 253) + '…'; // Telegram desc limit ~256

      const firstFile = c.batch.files && c.batch.files[0];
      const thumbUrl = await tryThumb(firstFile); // string or null

      const messageTextHtml =
        `${escapeHtml(display)}\n\n` +
        `Open: <a href="https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(c.token)}">Open in bot</a>`;

      const perResultMarkup = {
        inline_keyboard: [
          [
            { text: '🔗 Open in bot', url: `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(c.token)}` }
          ]
        ]
      };

      // Build the result object and only include thumb_url if we have a valid string
      const result = {
        type: 'article',
        id,
        title,
        description,
        input_message_content: { message_text: messageTextHtml, parse_mode: 'HTML' },
        reply_markup: perResultMarkup
      };
      if (thumbUrl) result.thumb_url = thumbUrl; // add conditionally

      results.push(result);
    }

    await bot.answerInlineQuery(qid, results, { cache_time: 0, is_personal: true });
  } catch (e) {
    console.error('inline_query error', e && e.message);
    try { await bot.answerInlineQuery(q.id, [], { cache_time: 0 }); } catch (_) {}
  }
});

// ---------- callback_query handler ----------
bot.on('callback_query', async (q) => {
  try {
  const fromId = q.from?.id;
  const chatId = q.message?.chat?.id ?? fromId;
  const data = q.data || '';
    const msgId = q.message && q.message.message_id;
    if (!data) return safeAnswerCallbackQuery(q.id); // acknowledge empty presses

    // ================= Single HELP button (user/admin aware) =================
if (data === 'help') {
  await safeAnswerCallbackQuery(q.id).catch(() => {});

  const isAdmin = String(fromId) === String(ADMIN_ID);

      // ---------- USER HELP ----------
    if (!isAdmin) {
      const text =
        `👋 <b>Welcome!</b>\n\n` +
        `Here’s how you can use this bot easily:\n\n` +
        `• <b>Search instantly</b> using inline mode — type <code>@${BOT_USERNAME}</code> followed by a keyword in any chat.\n` +
        `• <b>Browse all uploads</b> using the button below.\n` +
        `• <b>Open a batch directly</b> by using its token:\n` +
        `  Example: <code>/start_OBQUMJSSK9YB</code>\n\n` +
        `Stay updated by joining the official channels and feel free to send suggestions anytime.`;

      const replyMarkup = {
        inline_keyboard: [
          [
            { text: '🔎 Search', switch_inline_query_current_chat: '' },
            { text: '🧭 Browse', callback_data: 'browse' }
          ],
          [
            { text: '📢 Updates Channel', url: UPDATES_CHANNEL_URL },
            { text: '💬 Discussion Group', url: DISCUSSION_GROUP_URL }
          ],
          [
            { text: '💡 Suggest an Idea', callback_data: 'open_suggest_prompt' }
          ]
        ]
      };

      await safeSendMessage(chatId, text, {
        parse_mode: 'HTML',
        reply_markup: replyMarkup
      });
      return;
    }

// ---------- ADMIN HELP ----------
const helpText = `
      🛠️ <b>Admin commands</b>:
      • /sendfile — start new batch (finish with /doneadd)
      • /addto <code>&lt;TOKEN&gt;</code> — add files to batch (finish with /doneaddto)
      • /doneadd — close new batch (and broadcast)
      • /doneaddto — close appending
      • /edit_caption <code>&lt;TOKEN&gt;</code> — edit file caption
      • /renamebatch <code>&lt;token&gt;</code> | <code>&lt;New Name&gt;</code> — rename batch
      • /listfiles — list batches + tokens
      • /suggestions — view user suggestions
      • /deletefile <code>&lt;TOKEN&gt;</code> — delete batch
      • /listusers, /getuser <code>&lt;id&gt;</code> — user info
      • /setdeletetimer <code>&lt;seconds&gt;</code> — auto-delete timer
      • /exportbatches — export batch list (CSV)
      • /findid — get user/group/channel/bot ID

      📦 <b>Telegraph & batch tools</b>:
      • /buildtelegraph — build Telegraph pages
      • /buildtelegraph force — full rebuild
      • /showposted <code>&lt;token|filename&gt;</code> — preview batch + actions
      • /resend <code>&lt;token|filename&gt;</code> — send to channel
      • /resend <code>&lt;token|filename&gt;</code> broadcast — channel + users
      • /batchhelp — extra batch help

      📨 <b>Message Store (HTML)</b>:
      • /msgstore <code>&lt;key&gt;</code> — store next HTML message
      • /msgaddto <code>&lt;key&gt;</code> — append HTML message
      • /msgset <code>&lt;key&gt;</code> <code>&lt;index&gt;</code> — replace item (1-based)
      • /msgremove <code>&lt;key&gt;</code> <code>[&lt;index&gt;|all]</code> — remove item / key
      • /msgls — list + manage keys

      📡 <b>Channel browser</b>:
      • /chadd — start collecting channel links
      • /chdone — finish and save links
      • /chls — open browser (Prev/Next/Close)
      • /chmenu — list links, hide/unhide, copy deep-link
      • /chrpls <code>&lt;n&gt;</code> <code>&lt;t.me link&gt;</code> — replace item #n
        — or reply to forwarded msg: <code>/chrpls n</code>

      📁 <b>Index & uploads</b>:
      • /indx <code>[page]</code> — open batch index (paged)
      • /idxls — quick index launcher (like /chls, for batches)
      • /exportindex <code>&lt;chat_id|@username&gt;</code> — export full batch list to another chat
      • /sortuploads <code>[target]</code> — auto-sort batches into Movies / Series / Recommended

      🧹 <b>Auto-clear</b>:
      • /autoclear <code>on</code> — enable (15 min)
      • /autoclear <code>on 10</code> — enable (10 min)
      • /autoclear <code>off</code> — disable
      • /autoclear — show status

      <b>Note:</b> Use valid HTML tags (e.g. &lt;b&gt;, &lt;i&gt;, &lt;a href="..."&gt;, &lt;code&gt;, &lt;blockquote&gt;, &lt;span class="tg-spoiler"&gt;, &lt;u&gt;, &lt;s&gt;).
      `;

  const replyMarkup = {
    inline_keyboard: [
      [
        { text: '📤 Send files', callback_data: 'admin_sendfile' },
        { text: '➕ Add to batch', callback_data: 'admin_addto' }
      ],
      [
        { text: '✅ Finalize', callback_data: 'admin_finalize' },
        { text: '🗂 List files', callback_data: 'admin_listfiles' }
      ],
      [
        { text: '🗂️ View messages (public)', callback_data: 'msgls' }
      ],
      [
        { text: '📝 Edit caption', callback_data: 'admin_edit_caption' },
        { text: '✏️ Rename batch', callback_data: 'admin_renamebatch' },
        { text: '🗑 Delete batch', callback_data: 'admin_deletefile' }
      ],
      [
        { text: '👥 Users', callback_data: 'admin_users' },
        { text: '🔎 Find ID', callback_data: 'admin_findid' }
      ],
      [
        { text: '⏱️ Set delete timer', callback_data: 'admin_set_timer' },
        { text: '📊 Export Batches', callback_data: 'admin_export_batches' }
      ],
      [
        { text: '📦 Msg List', callback_data: 'admin_msg_list' },
        { text: '🧾 Msg Help', callback_data: 'admin_msg_help' }
      ],
      [
        { text: '📝 Store (key)', callback_data: 'admin_msg_store' },
        { text: '➕ AddTo (key)', callback_data: 'admin_msg_addto' }
      ],
      [
        { text: '⬅️ Back', callback_data: 'main_back' }
      ]
    ]
  };

  await safeSendMessage(chatId, helpText, { parse_mode: 'HTML', reply_markup: replyMarkup });
  return;
}

// help user/admin
if (data === 'help_user' || data === 'help_admin') {
  await safeAnswerCallbackQuery(q.id); // acknowledge tap to clear spinner

  // recreate admin check inside callback_query
  const fromId = q.from?.id;
  const chatId = q.message?.chat?.id;
  const isAdmin = ADMIN_ID && String(fromId) === String(ADMIN_ID);

  // ---------- USER HELP ----------
  if (!isAdmin) {
    const text =
      `👋 <b>Welcome!</b>\n\n` +
      `Here’s how you can use this bot easily:\n\n` +
      `• <b>Search instantly</b> using inline mode — type <code>@${BOT_USERNAME}</code> followed by a keyword in any chat.\n` +
      `• <b>Browse all uploads</b> using the button below.\n` +
      `• <b>Open a batch directly</b> by using its token:\n` +
      `  Example: <code>/start_OBQUMJSSK9YB</code>\n\n` +
      `Stay updated by joining the official channels and feel free to send suggestions anytime.`;

    const replyMarkup = {
      inline_keyboard: [
        [
          { text: '🔎 Search', switch_inline_query_current_chat: '' },
          { text: '🧭 Browse', callback_data: 'browse' }
        ],
        [
          { text: '📢 Updates Channel', url: UPDATES_CHANNEL_URL },
          { text: '💬 Discussion Group', url: DISCUSSION_GROUP_URL }
        ],
        [
          { text: '💡 Suggest an Idea', callback_data: 'open_suggest_prompt' }
        ]
      ]
    };

    await safeSendMessage(chatId, text, {
      parse_mode: 'HTML',
      reply_markup: replyMarkup
    });
    return;
  }

// --- Admin help popup ---
      if (data === 'help_admin') {
        // admin-only guard (string compare to be safe)
        if (String(fromId) !== String(ADMIN_ID)) {
          try { await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }); } catch (_) {}
          return;
        }

        // try to clear spinner quickly
        try { await safeAnswerCallbackQuery(q.id).catch(()=>{}); } catch (_) {}

// ---------- ADMIN HELP ----------
const helpText = `
      🛠️ <b>Admin commands</b>:
      • /sendfile — start new batch (finish with /doneadd)
      • /addto <code>&lt;TOKEN&gt;</code> — add files to batch (finish with /doneaddto)
      • /doneadd — close new batch (and broadcast)
      • /doneaddto — close appending
      • /edit_caption <code>&lt;TOKEN&gt;</code> — edit file caption
      • /renamebatch <code>&lt;token&gt;</code> | <code>&lt;New Name&gt;</code> — rename batch
      • /listfiles — list batches + tokens
      • /suggestions — view user suggestions
      • /deletefile <code>&lt;TOKEN&gt;</code> — delete batch
      • /listusers, /getuser <code>&lt;id&gt;</code> — user info
      • /setdeletetimer <code>&lt;seconds&gt;</code> — auto-delete timer
      • /exportbatches — export batch list (CSV)
      • /findid — get user/group/channel/bot ID

      📦 <b>Telegraph & batch tools</b>:
      • /buildtelegraph — build Telegraph pages
      • /buildtelegraph force — full rebuild
      • /showposted <code>&lt;token|filename&gt;</code> — preview batch + actions
      • /resend <code>&lt;token|filename&gt;</code> — send to channel
      • /resend <code>&lt;token|filename&gt;</code> broadcast — channel + users
      • /batchhelp — extra batch help

      📨 <b>Message Store (HTML)</b>:
      • /msgstore <code>&lt;key&gt;</code> — store next HTML message
      • /msgaddto <code>&lt;key&gt;</code> — append HTML message
      • /msgset <code>&lt;key&gt;</code> <code>&lt;index&gt;</code> — replace item (1-based)
      • /msgremove <code>&lt;key&gt;</code> <code>[&lt;index&gt;|all]</code> — remove item / key
      • /msgls — list + manage keys

      📡 <b>Channel browser</b>:
      • /chadd — start collecting channel links
      • /chdone — finish and save links
      • /chls — open browser (Prev/Next/Close)
      • /chmenu — list links, hide/unhide, copy deep-link
      • /chrpls <code>&lt;n&gt;</code> <code>&lt;t.me link&gt;</code> — replace item #n
        — or reply to forwarded msg: <code>/chrpls n</code>'

      📁 <b>Index & uploads</b>:
      • /indx <code>[page]</code> — open batch index (paged)
      • /idxls — quick index launcher (like /chls, for batches)
      • /exportindex <code>&lt;chat_id|@username&gt;</code> — export full batch list to another chat
      • /sortuploads <code>[target]</code> — auto-sort batches into Movies / Series / Recommended

      🧹 <b>Auto-clear</b>:
      • /autoclear <code>on</code> — enable (15 min)
      • /autoclear <code>on 10</code> — enable (10 min)
      • /autoclear <code>off</code> — disable
      • /autoclear — show status

      <b>Note:</b> Use valid HTML tags (e.g. &lt;b&gt;, &lt;i&gt;, &lt;a href="..."&gt;, &lt;code&gt;, &lt;blockquote&gt;, &lt;span class="tg-spoiler"&gt;, &lt;u&gt;, &lt;s&gt;).
      `;

        const replyMarkup = {
          inline_keyboard: [
            [
              { text: '📤 Send files', callback_data: 'admin_sendfile' },
              { text: '➕ Add to batch', callback_data: 'admin_addto' }
            ],
            [
              { text: '✅ Finalize', callback_data: 'admin_finalize' },
              { text: '🗂 List files', callback_data: 'admin_listfiles' }
            ],
            [
              // View the public messages index (uses cached Telegraph index if present)
              { text: '🗂️ View messages (public)', callback_data: 'msgls' },
              // Force rebuild the Telegraph index (admin-only)
              { text: '♻️ Rebuild Telegraph', callback_data: 'msgls_rebuild' },
              { text: '♻️ Rebuild Index', callback_data: 'open_index_rebuild' }
            ],
            [
              { text: '📝 Edit caption', callback_data: 'admin_edit_caption' },
              { text: '✏️ Rename batch', callback_data: 'admin_renamebatch' },
              { text: '🗑 Delete batch', callback_data: 'admin_deletefile' }
            ],
            [
              { text: '👥 Users', callback_data: 'admin_users' },
              { text: '🔎 Find ID', callback_data: 'admin_findid' }
            ],
            [
              { text: '⏱️ Set delete timer', callback_data: 'admin_set_timer' },
              { text: '📊 Export Batches', callback_data: 'admin_export_batches' }
            ],
            // Message Store shortcuts
            [
              { text: '📦 Msg List', callback_data: 'admin_msg_list' },
              { text: '🧾 Msg Help', callback_data: 'admin_msg_help' }
            ],
            [
              { text: '📝 Store (key)', callback_data: 'admin_msg_store' },
              { text: '➕ AddTo (key)', callback_data: 'admin_msg_addto' }
            ],
            [
              { text: '⬅️ Back', callback_data: 'main_back' }
            ]
          ]
        };

        try {
          // Try to send the HTML help once
          await safeSendMessage(chatId, helpText, { parse_mode: 'HTML', reply_markup: replyMarkup, disable_web_page_preview: true });
        } catch (err) {
          console.error('help_admin send failed', err && err.message);

          // Fallback: notify in plain text
          try { await safeSendMessage(chatId, 'Could not send formatted help. Showing plain text instead.'); } catch (err2) { console.error('help_admin fallback send failed', err2 && err2.message); }
        } finally {
          // Always try to clear the callback spinner (if q is present)
          if (q && q.id) {
            try { await safeAnswerCallbackQuery(q.id, { text: 'Admin help' }); } catch (_) { /* ignore - best effort only */ }
          }
        }
        return;
      }
}

    // inside your callback_query handler
    if (data === 'admin_regen_full_index') {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID)
        return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });

      try {
        // call with no current batch to build a pure global index
        const url = await createTelegraphIndexPage(null);
        await safeSendMessage(ADMIN_ID, `Global index created: ${url}`);
      } catch (e) {
        await safeSendMessage(ADMIN_ID, 'Failed to regenerate global index: ' + (e && e.message));
      }
      return;
    }

    // Admin: start rename batch flow
    if (data === 'admin_renamebatch') {
      const chatId = q.message.chat.id;
      const userId = q.from.id;

      // admin-only check
      if (String(userId) !== String(ADMIN_ID)) {
        await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }).catch(()=>{});
        return;
      }

      // clear spinner
      await safeAnswerCallbackQuery(q.id).catch(()=>{});

      // ask the admin for the batch identifier
      const text = 
        '✏️ <b>Rename Batch</b>\n\n' +
        'Send the <b>token</b>, <b>filename</b>, or <b>display name</b> of the batch you want to rename.\n' +
        'Example:\n' +
        '<code>RRR2022</code>\n' +
        '<code>someToken123</code>\n' +
        '<code>KGF Chapter 2</code>';

      const sent = await safeSendMessage(chatId, text, {
        parse_mode: 'HTML',
        reply_markup: { force_reply: true }
      });

      // store state so next message handles identifier
      const key = renameKey(chatId, userId);  // from previous code
      pendingRenames[key] = {
        stage: 'await_identifier',
        reply_to_message_id: sent.message_id,
        createdAt: Date.now()
      };

      // expire after 6 minutes
      setTimeout(() => {
        const p = pendingRenames[key];
        if (p && Date.now() - p.createdAt > 6 * 60 * 1000) delete pendingRenames[key];
      }, 6 * 60 * 1000);

      return;
    }

    // Handle new help actions (examples)
    if (data === 'browse') {
      await safeAnswerCallbackQuery(q.id, { text: 'Browse' });
      await safeSendMessage(chatId, 'Use /browse to explore files and tokens.');
      return;
    }

    // help_tokens — explains tokens and gives quick actions
    if (data === 'help_tokens') {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});

      const botName = BOT_USERNAME ? `@${BOT_USERNAME}` : 'this bot';
      const text =
    `How tokens work (quick):

    • Open a saved batch by sending the bot a start command:
      \`/start_<TOKEN>\`  (example: \`/start_OBQUMJSSK9YB\`)

    • Or open in the app using a link:
      \`https://t.me/${BOT_USERNAME}?start=<TOKEN>\`

    • If you don't know the token for something, use *View index* (Home → View index) to find saved batches and their tokens.

    Important notes:
    • Files served from a batch may be auto-deleted after a configured time — save important files to "Saved Messages" or download them.
    • Tokens reference a stored batch. If the batch was deleted or expired, the token will not work.`;

      const kb = {
        inline_keyboard: [
          [
            { text: '🗂️ View index', callback_data: 'view_index' },
            { text: '🔎 Try inline', switch_inline_query_current_chat: '' }
          ],
          [
            // contact admin (public link) — adjust URL to your admin
            { text: '📨 Contact Admin', url: ADMIN_CONTACT_URL },
            { text: '💡 Suggest', callback_data: 'open_suggest_prompt' }
          ]
        ]
      };

      await safeSendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: kb });
      return;
    }

    // Admin action examples (wire up to your flows)
    if (data === 'admin_sendfile') {
      await safeAnswerCallbackQuery(q.id, { text: 'Send files' });
      await safeSendMessage(chatId, 'Reply with a filename or upload files to begin /sendfile.');
      return;
    }
    // --- paste alongside other admin callback cases (e.g., admin_sendfile) ---
    if (data === 'admin_set_timer') {
      await safeAnswerCallbackQuery(q.id, { text: 'Set delete timer' });
      if (q.from && q.from.id !== ADMIN_ID) return safeSendMessage(chatId, 'Admin only');
      return safeSendMessage(chatId, 'To set the deletion timer use (example):\n/setdeletetimer 3600\n(Seconds; e.g. 3600 = 60 minutes)');
    }

    if (data === 'admin_addto') {
      await safeAnswerCallbackQuery(q.id, { text: 'Add to batch' });
      await safeSendMessage(chatId, 'Use /addto <TOKEN> then upload files to append.');
      return;
    }

    if (data === 'admin_finalize') {
      await safeAnswerCallbackQuery(q.id, { text: 'Finalize' });
      await safeSendMessage(chatId, 'Use /doneadd or /doneaddto to finalize.');
      return;
    }

    if (data === 'admin_listfiles') {
      await safeAnswerCallbackQuery(q.id, { text: 'List files' });
      await safeSendMessage(chatId, 'Run /listfiles to list batches and tokens.');
      return;
    }

    if (data === 'admin_edit_caption') {
      await safeAnswerCallbackQuery(q.id, { text: 'Edit caption' });
      await safeSendMessage(chatId, 'Use /edit_caption <TOKEN> to edit a caption.');
      return;
    }

    if (data === 'admin_deletefile') {
      await safeAnswerCallbackQuery(q.id, { text: 'Delete batch' });
      await safeSendMessage(chatId, 'Use /deletefile <TOKEN> to delete a batch.');
      return;
    }

    if (data === 'admin_users') {
      await safeAnswerCallbackQuery(q.id, { text: 'Users' });
      await safeSendMessage(chatId, 'Use /listusers or /getuser <id> for user tracking.');
      return;
    }

    // Help-style handler for Find ID button in admin help
    if (data === 'admin_findid') {
      await safeAnswerCallbackQuery(q.id, { text: 'Find ID Help' }).catch(()=>{});

      const helpText =
    `🔎 <b>Find ID — Guide</b>

    This tool helps you detect:
    • 👤 User ID
    • 💬 Group / Supergroup ID
    • 📣 Channel ID
    • 🤖 Bot ID

    <b>How to use:</b>
    1) Tap <b>🔎 Find ID</b> in admin menu
    2) Choose method:
      • Send a @username or t.me link
      • Forward a message from target

    <b>Private chats / groups?</b>
    Forwarding a message reveals the source ID.

    <b>Examples:</b>
    <code>@username</code>
    <code>t.me/username</code>
    <code>t.me/c/123456789/55</code> → <code>-100123456789</code>

    <b>Note:</b>
    Invite links (<code>t.me/+abc123</code>) cannot be resolved unless bot is inside chat.`

      await safeSendMessage(chatId, helpText, { parse_mode: 'HTML' }).catch(()=>{});
      return;
    }

    // --- Message Store: Help text ---
    if (data === 'admin_msg_help') {
      if (fromId !== ADMIN_ID) { await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }); return; }
      await safeAnswerCallbackQuery(q.id, { text: 'Message Store help' }).catch(()=>{});
      const t =
    `<b>Message Store — How to use</b>

    • <code>/msgstore &lt;key&gt;</code> → next message (HTML) will be stored as item #1 under <key>.
    • <code>/msgaddto &lt;key&gt;</code> → append another HTML item to <key>.
    • <code>/msgset &lt;key&gt; &lt;index&gt;</code> → replace item at index (1-based).
    • <code>/msgremove &lt;key&gt; [&lt;index&gt;|all]</code> → remove one item or the whole key.
    • <code>/msgls</code> → open listing menu (tap a key, view/add/remove).

    <b>Formatting:</b> send valid HTML (e.g. <b>, <i>, <a href="...">, <code>, <blockquote>, <span class="tg-spoiler">, <u>, <s>).`;
      await safeSendMessage(chatId, t, { parse_mode: 'HTML' });
      return;
    }

    // --- Message Store: open listing menu (same as /msgls) ---
    if (data === 'admin_msg_list') {
      if (String(fromId) !== String(ADMIN_ID)) {
        await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }).catch(()=>{});
        return;
      }

      await safeAnswerCallbackQuery(q.id, { text: 'Opening list' }).catch(()=>{});
      const meta = getMsgMeta(); // ✅ correct function name
      const view = buildKeysMenu(meta);
      await safeSendMessage(chatId, view.text, {
        parse_mode: 'HTML',
        reply_markup: view.reply_markup
      });
      return;
    }

    // --- Message Store: start /msgstore flow via button ---
    if (data === 'admin_msg_store') {
      if (fromId !== ADMIN_ID) { await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }); return; }
      await safeAnswerCallbackQuery(q.id, { text: 'Send key to store' }).catch(()=>{});
      pendingTextOps[chatId] = { mode: 'await_store_key' };
      await safeSendMessage(chatId, 'Reply with the <b>key</b> for storing the next HTML message (e.g. <code>promo1</code>).', { parse_mode: 'HTML' });
      return;
    }

    // --- Message Store: start /msgaddto flow via button ---
    if (data === 'admin_msg_addto') {
      if (fromId !== ADMIN_ID) { await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }); return; }
      await safeAnswerCallbackQuery(q.id, { text: 'Send key to append' }).catch(()=>{});
      pendingTextOps[chatId] = { mode: 'await_addto_key' };
      await safeSendMessage(chatId, 'Reply with the <b>key</b> to append to (e.g. <code>promo1</code>).', { parse_mode: 'HTML' });
      return;
    }

    if (data && data.startsWith('admin_regen_page_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });

      const token = data.slice('admin_regen_page_'.length);
      const idx = readIndex();
      const filename = idx.tokens && idx.tokens[token];
      if (!filename) {
        await safeSendMessage(ADMIN_ID, 'Batch not found for token: ' + token);
        return;
      }
      const batch = readBatchFile(filename); // existing helper in your bot to read saved batch file
      if (!batch) {
        await safeSendMessage(ADMIN_ID, 'Batch file missing for token: ' + token);
        return;
      }

      // create page & send admin the new link + keyboard
      try {
        const url = await sendAdminTelegraphIndex(batch, ADMIN_ID);
        // optionally edit the original prompt message to show the new URL (best-effort)
        try {
          if (q.message && q.message.chat && q.message.message_id) {
            await bot.editMessageText(`Index page recreated: ${url}`, { chat_id: q.message.chat.id, message_id: q.message.message_id });
          }
        } catch (e) { /* ignore edit errors */ }
      } catch (e) {
        // already messaged admin in sendAdminTelegraphIndex
      }
      return;
    }

    // near your other command handlers, create a function:
    if (data === 'admin_export_batches') {
      // Acknowledge the button press (don't let this throw)
      await safeAnswerCallbackQuery(q.id, { text: 'Exporting batches...' }).catch(()=>{});

      // Admin-only guard (compare as strings to avoid type mismatch)
      if (!q.from || String(q.from.id) !== String(ADMIN_ID)) {
        return safeSendMessage(chatId, 'Admin only');
      }

      try {
        // 1) Preferred: call the exported function directly if available
        if (module.exports && typeof module.exports.runExportBatchesCommand === 'function') {
          return await module.exports.runExportBatchesCommand({
            chat: { id: chatId, type: q.message.chat.type },
            from: q.from
          });
        }

        // 2) Fallback: synthesize a message update and hand it to the bot
        const update = {
          update_id: Date.now(),
          message: {
            message_id: Date.now() + 1,
            from: q.from,
            chat: { id: chatId, type: q.message.chat.type },
            date: Math.floor(Date.now() / 1000),
            text: '/exportbatches'
          }
        };

        if (typeof bot.processUpdate === 'function') {
          // node-telegram-bot-api (or similar) — use processUpdate if available
          return await bot.processUpdate(update);
        } else if (typeof bot.emit === 'function') {
          // fallback: emit a 'message' event so your existing message handlers run
          bot.emit('message', update.message);
          return;
        }

        // If we reach here, we couldn't call the handler
        return safeSendMessage(chatId, 'Export handler not found (cannot trigger command).');

      } catch (err) {
        console.error('export via inline button failed:', err && err.stack ? err.stack : err);
        return safeSendMessage(chatId, 'Export failed — check logs.');
      }
    }
   
    // inside your bot.on('callback_query', async (q) => { ... }) handler
    try {
      if (!q || !q.data) return;
      const data = q.data;
      const fromId = q.from && q.from.id;
      const chatId = q.message ? q.message.chat.id : fromId;

      // ---------------- open_index: show published Telegraph index to users; admin can rebuild ----------------
      // open_index branch: send plain-text Telegraph URL(s); show admin-only "Rebuild" button
      if (data === 'open_index' || data === 'open_index_rebuild') {
        const isRebuildRequest = (data === 'open_index_rebuild');
        const fromId = q.from && q.from.id;
        const isAdmin = (typeof ADMIN_ID !== 'undefined' && ADMIN_ID && String(fromId) === String(ADMIN_ID));

        // If someone attempted to call rebuild but is not admin, politely block it
        if (isRebuildRequest && !isAdmin) {
          await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }).catch(()=>{});
          return;
        }

        await safeAnswerCallbackQuery(q.id).catch(()=>{});
        const chatId = q.message ? q.message.chat.id : fromId;

        let processing = null;
        try {
          processing = await showProcessing(chatId, '⏳ Building Telegraph index…').catch(()=>null);

          // If admin pressed rebuild, force regeneration; otherwise let createTelegraphIndexPages use its default/cached behavior
          const urls = await createTelegraphIndexPages({ force: !!(isRebuildRequest && isAdmin) });

          if (!Array.isArray(urls) || urls.length === 0) {
            // If there are no pages, let users know. Include admin hint if requester is admin.
            const adminHint = isAdmin ? '\n(As admin you can rebuild using the button below.)' : '';
            await safeSendMessage(chatId, 'No Telegraph index pages were produced (yet).' + adminHint);
            return;
          }

          // Build plain-text message body with URL(s)
          let messageBody;
          if (urls.length === 1) {
            messageBody = `Telegraph index created:\n${urls[0]}`;
          } else {
            messageBody = 'Telegraph index pages created:\n\n' + urls.map((u, i) => `${i+1}. ${u}`).join('\n');
          }

          // If the requester is admin, attach a single inline button to let them rebuild quickly.
          // Non-admins receive just the plain-text message (no inline keyboard).
          if (isAdmin) {
            const keyboard = { inline_keyboard: [[{ text: '♻️ Rebuild Telegraph', callback_data: 'open_index_rebuild' }]] };
            try {
              await safeSendMessage(chatId, messageBody, { reply_markup: keyboard });
            } catch (e) {
              // fallback to simple send
              await safeSendMessage(chatId, messageBody);
            }
          } else {
            await safeSendMessage(chatId, messageBody);
          }
        } catch (err) {
          console.error('open_index -> createTelegraphIndexPages error', err && (err.stack || err.message));
          await safeSendMessage(chatId, 'Failed to build Telegraph index pages: ' + (err?.message || 'unknown error'));
        } finally {
          if (processing && typeof processing.done === 'function') {
            try { await processing.done(); } catch (_) {}
          }
        }

        return;
      }
    } catch (err) {
      console.error('callback_query handler error', err);
    }

    // index & browse callbacks
    if (data === 'view_index' || data.startsWith('index_') || data.startsWith('browse_') || data.startsWith('file_') || data.startsWith('browse_file_')) {
      await safeAnswerCallbackQuery(q.id);

      // view index
      if (data === 'view_index') {
        const idxPayload = buildIndexTextAndKeyboardQuick(0, (q.from && q.from.id === ADMIN_ID));
        try { await bot.sendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(chatId, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index pagination handlers
      if (data.startsWith('index_prev_') || data.startsWith('index_next_') || data.startsWith('index_page_') || data.startsWith('index_refresh_')) {
        const parts = data.split('_');
        const page = Number(parts[2]) || 0;
        const idxPayload = buildIndexTextAndKeyboardQuick(page, (q.from && q.from.id === ADMIN_ID));
        try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
        return;
      }

      // index actions with token
      if (data.startsWith('index_view_') || data.startsWith('index_up_') || data.startsWith('index_down_')) {
        const parts = data.split('_');
        const action = parts[1];
        const token = parts[2];
        const page = Number(parts[3]) || 0;
        const idx = readIndex();
        const filename = idx.tokens && idx.tokens[token];
        if (!filename) return safeSendMessage(q.message.chat.id, 'Batch not found.');
        if (action === 'view') {
          const batch = readBatchFile(filename);
          if (!batch) return safeSendMessage(q.message.chat.id, 'Batch missing.');
          const asAdmin = (q.from && q.from.id === ADMIN_ID);
          const listView = buildListViewForBatch(token, batch, asAdmin);
          try { await bot.editMessageText(listView.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, listView.text, { parse_mode: 'HTML', reply_markup: listView.keyboard }); }
          return;
        }
        // reorder batch in index — admin only
        if ((action === 'up' || action === 'down') && q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        const order = idx.order || [];
        const pos = order.indexOf(filename);
        if (pos === -1) return safeSendMessage(q.message.chat.id, 'Batch not in index order.');
        if (action === 'up') {
          if (pos <= 0) return safeAnswerCallbackQuery(q.id, { text: 'Already top' });
          [order[pos-1], order[pos]] = [order[pos], order[pos-1]];
          idx.order = order; writeIndex(idx);
          const idxPayload = buildIndexTextAndKeyboardQuick(page, true);
          try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          return;
        }
        if (action === 'down') {
          if (pos >= order.length - 1) return safeAnswerCallbackQuery(q.id, { text: 'Already bottom' });
          [order[pos+1], order[pos]] = [order[pos], order[pos+1]];
          idx.order = order; writeIndex(idx);
          const idxPayload = buildIndexTextAndKeyboardQuick(page, true);
          try { await bot.editMessageText(idxPayload.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          catch (e) { await safeSendMessage(q.message.chat.id, idxPayload.text, { parse_mode:'HTML', reply_markup: idxPayload.keyboard }); }
          return;
        }
      }

      // browse open (same as /browse)
      if (data === 'browse_open' || data === 'browse_open_from_done') {
        const idx = readIndex();
        const order = idx.order || [];
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches available.');
        const pos = order.length - 1; const filename = order[pos];
        const batch = readBatchFile(filename);
        if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Latest batch has no files.');
        const firstFile = batch.files[0];
        const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        let baseMsg;
        try {
          if (firstFile.type === 'photo' && firstFile.file_id) baseMsg = await bot.sendPhoto(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else if (firstFile.type === 'document' && firstFile.file_id) baseMsg = await bot.sendDocument(chatId, firstFile.file_id, captionHtml ? { caption: captionHtml, parse_mode: 'HTML' } : {});
          else baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {});
        } catch (e) { baseMsg = await bot.sendMessage(chatId, captionHtml || filename, captionHtml ? { parse_mode: 'HTML' } : {}); }
        browseSessions[chatId] = { pos, order, messageId: baseMsg.message_id };
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: baseMsg.message_id }); } catch (e) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} }
        return;
      }

      // session checks
      const session = browseSessions[chatId];
      if (!session) return safeSendMessage(chatId, 'No active browse session. Use /browse.');

      const order = session.order || []; let pos = session.pos || 0;

      if (data === 'browse_left') {
        // left shows previous upload -> older -> increase pos
        pos = Math.min(order.length - 1, pos + 1); session.pos = pos;
        const fname = order[pos]; const batch = readBatchFile(fname); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_right') {
        pos = Math.max(0, pos - 1); session.pos = pos;
        const fname = order[pos]; const batch = readBatchFile(fname); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(pos, order.length, batch.token);
        try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      if (data === 'browse_random') {
        if (!order || order.length === 0) return safeSendMessage(chatId, 'No batches.');
        const r = Math.floor(Math.random() * order.length); session.pos = r;
        const batch = readBatchFile(order[r]); if (!batch || !batch.files || batch.files.length === 0) return safeSendMessage(chatId, 'Random batch empty.');
        const fileObj = batch.files[0]; const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(r, order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

if (data === 'browse_view') {
        const fname = order[session.pos];
        const batch = readBatchFile(fname);
        if (!batch) return safeSendMessage(chatId, 'Batch missing.');

        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname) || batch.token || '';
        const asAdmin = (q.from && q.from.id === ADMIN_ID);

        const filesKb = buildFilesKeyboardForBatch(token, batch, asAdmin);
        try {
          await bot.editMessageReplyMarkup({ inline_keyboard: filesKb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId });
        } catch (e) {
          await safeSendMessage(chatId, 'Files:', { reply_markup: filesKb });
        }

        // --- NEW: send all files in the batch to the user (same as /start with token) ---
        const sentMessageIdsForBrowse = []; // This will contain file message IDs
        try {
          if (!Array.isArray(batch.files) || batch.files.length === 0) {
            return safeSendMessage(chatId, 'No files in this batch.');
          }

          for (let i = 0; i < batch.files.length; i++) {
            const item = batch.files[i];
            const sent = await sendBatchItemToChat(chatId, batch, item);
            if (sent && (sent.message_id || sent.message_id === 0)) sentMessageIdsForBrowse.push(sent.message_id);
            if (sent && sent.edit && sent.message_id) sentMessageIdsForBrowse.push(sent.message_id);
            if (sent && sent.newMessage && sent.newMessage.message_id) sentMessageIdsForBrowse.push(sent.newMessage.message_id);
            await sleep(120);
          }

          // rating keyboard
          const row1 = [], row2 = [];
          for (let s = 1; s <= 5; s++) row1.push({ text: `${s}⭐`, callback_data: `rate_${token}_${s}` });
          for (let s = 6; s <= 10; s++) row2.push({ text: `${s}⭐`, callback_data: `rate_${token}_${s}` });

          // Send the rating prompt AND CAPTURE ITS MESSAGE ID
          let ratingMessage = null;
          try {
              ratingMessage = await safeSendMessage(chatId, 'Rate this batch (1–10):', { reply_markup: { inline_keyboard: [row1, row2] } });
              if (ratingMessage && ratingMessage.message_id) {
                  sentMessageIdsForBrowse.push(ratingMessage.message_id); // Add rating message to deletion list
              }
          } catch (e) {
              console.warn('Failed to send rating message (browse_view):', e.message);
          }
          
          // Schedule deletion for all collected message IDs (files + rating message)
          const meta = readMeta() || {};
          const deleteSeconds = Number(meta.delete_after_seconds || 3600); // default 1 hour
          
          // Call the shared deletion function, passing the collected message IDs and the token
          scheduleDeletionForMessages(chatId, sentMessageIdsForBrowse, deleteSeconds, token);

          return;

        } catch (err) {
          console.error('Error sending batch files:', err);
          return safeSendMessage(chatId, 'Failed to send batch files. Try again later.');
        }
      }

      // --- Go-to page flow for browse controls ---
      if (data === 'browse_goto') {
        await safeAnswerCallbackQuery(q.id).catch(()=>{});
        const session = browseSessions[chatId];
        if (!session) {
          // No active browse session: show a small instruction instead of an error
          return safeSendMessage(chatId, 'No active browse session. Use /browse or Browse to start one.');
        }
        const meta = readMeta() || {};
        const pageSize = Number(meta.index_page_size || 8);
        const totalItems = (session.order && session.order.length) ? session.order.length : 0;
        const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
        const rows = [];
        // build up to 8 page buttons per keyboard (you can adjust)
        const maxShow = Math.min(totalPages, 16);
        const pageButtons = [];
        for (let p = 1; p <= maxShow; p++) {
          pageButtons.push({ text: String(p), callback_data: `browse_goto_page_${p}` });
          if (pageButtons.length === 4) { rows.push(pageButtons.slice()); pageButtons.length = 0; }
        }
        if (pageButtons.length) rows.push(pageButtons.slice());
        rows.push([ { text: 'Cancel', callback_data: 'noop' } ]);
        return bot.sendMessage(chatId, `Choose a page (1–${totalPages}):`, { reply_markup: { inline_keyboard: rows } });
      }

      if (data.startsWith('browse_goto_page_')) {
        await safeAnswerCallbackQuery(q.id).catch(()=>{});
        const m = data.match(/^browse_goto_page_(\d+)$/);
        if (!m) return;
        const pageNum = Number(m[1]);
        const session = browseSessions[chatId];
        if (!session) return safeSendMessage(chatId, 'No active browse session.');
        const meta = readMeta() || {};
        const pageSize = Number(meta.index_page_size || 8);
        // Convert page number to a position (jump to first item of that page)
        const pos = Math.min(session.order.length - 1, Math.max(0, (pageNum - 1) * pageSize));
        session.pos = pos;
        // show the batch at that position (like browse_left / browse_right)
        const fname = session.order[session.pos];
        const batch = readBatchFile(fname);
        if (!batch || !batch.files || !batch.files.length) return safeSendMessage(chatId, 'Batch empty.');
        const firstFile = batch.files[0];
        const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        try {
          const res = await replaceBrowseMessage(chatId, session.messageId, firstFile, captionHtml);
          if (res && res.newMessage) session.messageId = res.newMessage.message_id;
          const kb = makeBrowseKeyboardForIndex(session.pos, session.order.length, batch.token);
          try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        } catch (e) {
          try { await safeSendMessage(chatId, captionHtml || fname, { reply_markup: makeBrowseKeyboardForIndex(session.pos, session.order.length, batch.token) }); } catch(_) {}
        }
        return;
      }

      if (data === 'browse_list') {
        const fname = order[session.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const token = Object.keys(readIndex().tokens || {}).find(t => readIndex().tokens[t] === fname);
        const asAdmin = (q.from && q.from.id === ADMIN_ID);
        const listView = buildListViewForBatch(token, batch, asAdmin);
        try { await bot.editMessageText(listView.text, { chat_id: chatId, message_id: session.messageId, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard }); } catch (e) { await safeSendMessage(chatId, listView.text, { parse_mode: 'HTML', reply_markup: listView.keyboard }); }
        return;
      }

      if (data === 'browse_back_to_preview') {
        const s = browseSessions[chatId]; if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing.');
        const firstFile = batch.files[0]; const captionHtml = firstFile.caption ? formatCaptionHtmlForPreview(firstFile.caption) : '';
        try { const res = await replaceBrowseMessage(chatId, s.messageId, firstFile, captionHtml); if (res && res.newMessage) s.messageId = res.newMessage.message_id; const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) {} } catch (e) { const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) { try { await bot.sendMessage(chatId, 'Browse controls:', { reply_markup: kb }); } catch (_) {} } }
        return;
      }

      // show a specific file from files list
      if (data.startsWith('browse_file_')) {
        const parts = data.split('_'); const token = parts[2]; const indexStr = parts[3]; const fileIdx = Number(indexStr);
        if (isNaN(fileIdx)) return safeSendMessage(chatId, 'Invalid file index');
        const idxObj = readIndex(); const fname = idxObj.tokens[token]; if (!fname) return safeSendMessage(chatId, 'Batch not found for that token');
        const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const fileObj = batch.files[fileIdx]; if (!fileObj) return safeSendMessage(chatId, 'File not found in batch');
        const captionHtml = fileObj.caption ? formatCaptionHtmlForPreview(fileObj.caption) : '';
        const res = await replaceBrowseMessage(chatId, session.messageId, fileObj, captionHtml);
        if (res && res.newMessage) session.messageId = res.newMessage.message_id;
        const kb = makeBrowseKeyboardForIndex(session.pos, session.order.length, token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: session.messageId }); } catch (_) {}
        return;
      }

      // close files view
      if (data === 'browse_files_close') {
        const s = browseSessions[chatId]; if (!s) return safeSendMessage(chatId, 'No active browse session.');
        const fname = s.order[s.pos]; const batch = readBatchFile(fname); if (!batch) return safeSendMessage(chatId, 'Batch missing');
        const kb = makeBrowseKeyboardForIndex(s.pos, s.order.length, batch.token); try { await bot.editMessageReplyMarkup({ inline_keyboard: kb.inline_keyboard }, { chat_id: chatId, message_id: s.messageId }); } catch (_) {}
        return;
      }

      // file-level admin actions: file_up_, file_down_, file_edit_
      if (data.startsWith('file_up_') || data.startsWith('file_down_') || data.startsWith('file_edit_')) {
        const parts = data.split('_');
        const action = parts[1];
        const token = parts[2];
        const idxNum = Number(parts[3]);
        if ((action === 'up' || action === 'down') && q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
        const idxObj = readIndex();
        const filename = idxObj.tokens[token];
        if (!filename) return safeSendMessage(chatId, 'Batch not found');
        const batch = readBatchFile(filename);
        if (!batch) return safeSendMessage(chatId, 'Batch missing');
        if (isNaN(idxNum) || idxNum < 0 || idxNum >= (batch.files||[]).length) return safeSendMessage(chatId, 'Invalid file index');
        if (action === 'up') {
          if (idxNum <= 0) return safeAnswerCallbackQuery(q.id, { text: 'Already at top' });
          [batch.files[idxNum-1], batch.files[idxNum]] = [batch.files[idxNum], batch.files[idxNum-1]];
          writeBatchFile(filename, batch);
          await safeAnswerCallbackQuery(q.id, { text: 'Moved up' });
        } else if (action === 'down') {
          if (idxNum >= batch.files.length - 1) return safeAnswerCallbackQuery(q.id, { text: 'Already at bottom' });
          [batch.files[idxNum+1], batch.files[idxNum]] = [batch.files[idxNum], batch.files[idxNum+1]];
          writeBatchFile(filename, batch);
          await safeAnswerCallbackQuery(q.id, { text: 'Moved down' });
        } else if (action === 'edit') {
          if (q.from && q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only' });
          pendingBatches[chatId] = pendingBatches[chatId] || {};
          pendingBatches[chatId].editCaptionFlow = { token, filename: filename, stage: 'await_caption', fileIndex: idxNum };
          return safeSendMessage(chatId, `Send a new caption for file #${idxNum+1} in that batch.`);
        }
        // re-render the list view if message belongs to the bot
        try {
          const asAdmin = true;
          const listView = buildListViewForBatch(token, batch, asAdmin);
          await bot.editMessageText(listView.text, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode: 'HTML', reply_markup: listView.keyboard.inline_keyboard });
        } catch (e) { /* ignore */ }
        return;
      }

      return;
    }

    // rating
    if (data && data.startsWith('rate_')) {
      const parts = data.split('_'); const token = parts[1]; const score = Number(parts[2] || 0);
      const idx = readIndex(); const filename = idx.tokens[token]; if (!filename) return safeAnswerCallbackQuery(q.id, { text: 'Batch not found' });
      const batch = readBatchFile(filename); if (!batch) return safeAnswerCallbackQuery(q.id, { text: 'Batch missing' });
      batch.ratings = batch.ratings || {}; batch.ratings[q.from.id] = { score, ts: new Date().toISOString() }; writeBatchFile(filename, batch);
      return safeAnswerCallbackQuery(q.id, { text: `Thanks — you rated ${score}⭐` });
    }
    // --- tolerant no-op / close handlers for inline UIs ---
    // Put this before the final 'Unknown action' default in the callback_query handler
    if (data === 'noop') {
      // Simple acknowledgement for a "Close" or noop button.
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      try {
        // Try to remove inline keyboard on the message that had the noop button.
        await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id }).catch(()=>{});
      } catch (e) { /* ignore */ }
      return;
    }

    if (data === 'browse_files_close') {
      // Close the files-list view. If we have an active browse session, use it.
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      const s = browseSessions[q.message.chat.id];
      if (!s) {
        // Just clear the inline keyboard of the message where the user pressed Close.
        try {
          await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id });
        } catch (e) {
          // If edit fails (old message), try deleting the message quietly (if bot has rights)
          try { await bot.deleteMessage(q.message.chat.id, q.message.message_id); } catch (_) {}
        }
        return;
      }
      // If a real session exists, remove session and try to tidy the preview message
      try {
        const fname = s.order[s.pos];
        delete browseSessions[q.message.chat.id];
        // attempt to clear keyboard on the messageId stored for the session
        try { await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: s.messageId }); } catch (_) {}
      } catch (e) { /* ignore */ }
      return;
    }

    // Favorites helpers (uses readMeta/writeMeta)
    function favAdd(userId, token) {
      const meta = (typeof readMeta === 'function') ? (readMeta() || {}) : {};
      meta.favorites = meta.favorites || {};
      meta.favorites[userId] = Array.isArray(meta.favorites[userId]) ? meta.favorites[userId] : [];
      if (!meta.favorites[userId].includes(token)) meta.favorites[userId].push(token);
      if (typeof writeMeta === 'function') writeMeta(meta);
      return meta.favorites[userId];
    }
    function favRemove(userId, token) {
      const meta = (typeof readMeta === 'function') ? (readMeta() || {}) : {};
      meta.favorites = meta.favorites || {};
      meta.favorites[userId] = Array.isArray(meta.favorites[userId]) ? meta.favorites[userId] : [];
      meta.favorites[userId] = meta.favorites[userId].filter(t => t !== token);
      if (typeof writeMeta === 'function') writeMeta(meta);
      return meta.favorites[userId];
    }

    // Inside bot.on('callback_query', async (q) => { ... })
    if (data && data.startsWith('fav_add_')) {
      const token = data.slice('fav_add_'.length);
      const userId = q.from && q.from.id;
      favAdd(String(userId), token);
      await safeAnswerCallbackQuery(q.id, { text: 'Saved to favorites ❤️' }).catch(()=>{});
      // Toggle the button to "Remove" if possible
      try {
        const msg = q.message;
        const kb = (msg && msg.reply_markup && msg.reply_markup.inline_keyboard) ? msg.reply_markup : null;
        if (kb && kb.inline_keyboard && kb.inline_keyboard[0]) {
          kb.inline_keyboard[0][1] = { text: '❌ Remove', callback_data: `fav_remove_${token}` };
          await bot.editMessageReplyMarkup(kb, { chat_id: msg.chat.id, message_id: msg.message_id });
        }
      } catch (_) {}
      return;
    }

    if (data && data.startsWith('fav_remove_')) {
      const token = data.slice('fav_remove_'.length);
      const userId = q.from && q.from.id;
      favRemove(String(userId), token);
      await safeAnswerCallbackQuery(q.id, { text: 'Removed from favorites' }).catch(()=>{});
      // Toggle back to "Save"
      try {
        const msg = q.message;
        const kb = (msg && msg.reply_markup && msg.reply_markup.inline_keyboard) ? msg.reply_markup : null;
        if (kb && kb.inline_keyboard && kb.inline_keyboard[0]) {
          kb.inline_keyboard[0][1] = { text: '❤️ Save', callback_data: `fav_add_${token}` };
          await bot.editMessageReplyMarkup(kb, { chat_id: msg.chat.id, message_id: msg.message_id });
        }
      } catch (_) {}
      return;
    }

    // --- Admin actions for doneadd prompt ---
    if (data && data.startsWith('admin_post_channel_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });

      const token = data.slice('admin_post_channel_'.length);
      const idx = readIndex(); const filename = idx.tokens && idx.tokens[token];
      if (!filename) return safeSendMessage(ADMIN_ID, 'Batch not found for token: ' + token);
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(ADMIN_ID, 'Batch file missing.');

      try {
        const sent = await postBatchToPrivateChannel(batch).catch(()=>null);
        const reply = sent && sent.message_id ? `Posted to channel (message_id: ${sent.message_id}).` : 'Posting failed or no private channel configured.';
        await safeSendMessage(ADMIN_ID, reply);
      } catch (e) {
        console.warn('admin_post_channel error', e && e.message);
        await safeSendMessage(ADMIN_ID, 'Posting failed: ' + (e && e.message));
      }

      // tidy up: clear the original inline keyboard (best-effort)
      try { await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id }).catch(()=>{}); } catch(_) {}
      return;
    }

    if (data && data.startsWith('admin_broadcast_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });

      const token = data.slice('admin_broadcast_'.length);
      const idx = readIndex(); const filename = idx.tokens && idx.tokens[token];
      if (!filename) return safeSendMessage(ADMIN_ID, 'Batch not found for token: ' + token);
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(ADMIN_ID, 'Batch file missing.');

      try {
        await broadcastNewBatchToAllUsers(batch);
        await safeSendMessage(ADMIN_ID, 'Broadcast attempted (see log).');
      } catch (e) {
        console.warn('admin_broadcast error', e && e.message);
        await safeSendMessage(ADMIN_ID, 'Broadcast failed: ' + (e && e.message));
      }

      try { await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id }).catch(()=>{}); } catch(_) {}
      return;
    }

    if (data && data.startsWith('admin_post_broadcast_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });

      const token = data.slice('admin_post_broadcast_'.length);
      const idx = readIndex(); const filename = idx.tokens && idx.tokens[token];
      if (!filename) return safeSendMessage(ADMIN_ID, 'Batch not found for token: ' + token);
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(ADMIN_ID, 'Batch file missing.');

      try {
        const sent = await postBatchToPrivateChannel(batch).catch(()=>null);
        if (sent && sent.message_id) {
          await safeSendMessage(ADMIN_ID, `Posted to channel (message_id: ${sent.message_id}). Starting broadcast...`);
        } else {
          await safeSendMessage(ADMIN_ID, 'Posting to channel failed or channel not configured. Proceeding to broadcast only.');
        }
        await broadcastNewBatchToAllUsers(batch);
        await safeSendMessage(ADMIN_ID, 'Broadcast attempted (see log).');
      } catch (e) {
        console.warn('admin_post_broadcast error', e && e.message);
        await safeSendMessage(ADMIN_ID, 'Post & broadcast failed: ' + (e && e.message));
      }

      try { await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id }).catch(()=>{}); } catch(_) {}
      return;
    }

    if (data && data.startsWith('admin_skip_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });
      // Clear the inline keyboard for cleanliness
      try { await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: q.message.chat.id, message_id: q.message.message_id }); } catch(_) {}
      await safeSendMessage(ADMIN_ID, 'No action taken for this batch.');
      return;
    }

    return safeAnswerCallbackQuery(q.id, { text: 'Unknown action' });

  } catch (e) {
    console.error('callback_query handler error', e && (e.stack || e.message));
    try { await safeAnswerCallbackQuery(q.id, { text: 'Error handling action' }); } catch (_) {}
  }
});

bot.on('callback_query', async (callbackQuery) => {
  try {
    const data = callbackQuery.data || '';
    const parts = data.split('|');
    const cmd = parts[0];
    const key = parts[1];

    if (cmd === 'copytoken') {
      const rec = key ? __callbackTokenMap[key] : null;
      if (!rec || !rec.token) {
        return bot.answerCallbackQuery(callbackQuery.id, { text: 'Token expired or unavailable', show_alert: true });
      }
      await bot.answerCallbackQuery(callbackQuery.id); // remove spinner
      const safeText = `Token for "${rec.display || ''}":\n\`${rec.token}\``;
      try {
        await bot.sendMessage(callbackQuery.from.id, safeText, { parse_mode: 'MarkdownV2' });
      } catch (e) {
        // fallback if DM fails
        await bot.answerCallbackQuery(callbackQuery.id, { text: `Token: ${rec.token}`, show_alert: true });
      }
      return;
    }

    if (cmd === 'indexview') {
      const rec = key ? __callbackTokenMap[key] : null;
      const token = rec ? rec.token : null;
      const page = parts[2] ? Number(parts[2]) : 0;

      // stop spinner
      await bot.answerCallbackQuery(callbackQuery.id);

      if (!token) {
        return bot.sendMessage(callbackQuery.message.chat.id, 'Preview token expired or invalid. Try the index again.');
      }

      // 1) Prefer your existing preview function(s) if present
      if (typeof handleIndexView === 'function') {
        return handleIndexView(callbackQuery.message.chat.id, token, page, callbackQuery);
      }
      if (typeof showBatchPreview === 'function') {
        return showBatchPreview(callbackQuery.message.chat.id, token, page, callbackQuery);
      }
      if (typeof handleBrowse === 'function') {
        return handleBrowse(callbackQuery.message.chat.id, token, { fromCallback: callbackQuery });
      }

      // 2) Fallback: try to read the batch and send a first-file caption like /browse would
      try {
        const idx = readIndex();
        const filename = (idx && idx.tokens && idx.tokens[token]) ? idx.tokens[token] : null;
        let batch = null;
        if (filename) batch = readBatchFile(filename);
        // fallback: maybe token maps to display name; attempt to find matching filename
        if (!batch && idx && idx.order) {
          const candidate = idx.order.find(fn => {
            const b = readBatchFile(fn);
            const name = (b && b.display_name) ? String(b.display_name) : fn;
            return name === token || name === rec?.display;
          });
          if (candidate) batch = readBatchFile(candidate);
        }

        if (!batch) {
          // we can't construct a full preview — provide the deep link + caption fallback
          const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : `https://t.me/?start=${encodeURIComponent(token)}`;
          return bot.sendMessage(callbackQuery.message.chat.id, `Preview not available here. Open using the link below:\n${openUrl}`);
        }

        // try to locate first file and its caption
        const first = (batch.files && batch.files[0]) || (batch.items && batch.items[0]) || null;
        const caption = first && (first.caption || first.title || first.text) ? String(first.caption || first.title || first.text) : (batch.caption || `Preview: ${batch.display_name || batch.filename || 'item'}`);

        // best-effort preview: if it's just a caption/text, send it
        if (!first || (!first.file_id && !first.fileId && !first.id)) {
          // no file id — send the caption + deep link
          const openUrl = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : `https://t.me/?start=${encodeURIComponent(token)}`;
          return bot.sendMessage(callbackQuery.message.chat.id, `${caption}\n\nOpen: ${openUrl}`);
        }

        // If we have a file id, attempt to send the file. We try common senders in order:
        const fileId = first.file_id || first.fileId || first.id;
        // try sendPhoto, sendVideo, sendDocument — choose by declared type if available
        if (first.mime_type && first.mime_type.startsWith('image')) {
          return bot.sendPhoto(callbackQuery.message.chat.id, fileId, { caption });
        }
        if (first.mime_type && first.mime_type.startsWith('video')) {
          return bot.sendVideo(callbackQuery.message.chat.id, fileId, { caption });
        }
        // default to document send
        return bot.sendDocument(callbackQuery.message.chat.id, fileId, {}, { caption });
      } catch (e) {
        console.error('indexview fallback error', e);
        return bot.sendMessage(callbackQuery.message.chat.id, 'Unable to show preview. Try opening the item using the Open link.');
      }
    }

    // keep other callback handlers (prev/next/page etc) in place
  } catch (err) {
    console.error('callback_query error', err);
    try { await bot.answerCallbackQuery(callbackQuery.id, { text: 'Error', show_alert: false }); } catch (e) {}
  }
});

// ---------- PRIVATE CHANNEL POSTING (drop into bot.js) ----------
const PRIVATE_CHANNEL_ID = process.env.PRIVATE_CHANNEL_ID ? (String(process.env.PRIVATE_CHANNEL_ID).startsWith('@') ? String(process.env.PRIVATE_CHANNEL_ID) : Number(process.env.PRIVATE_CHANNEL_ID)) : null;

// Post the batch's first file into the configured private channel with controls
async function postBatchToPrivateChannel(batch) {
  if (!PRIVATE_CHANNEL_ID) return null;
  const channelId = PRIVATE_CHANNEL_ID;
  const firstFile = (batch.files && batch.files[0]) || null;

  const accessLink = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}` : `https://t.me/?start=${encodeURIComponent(batch.token)}`;

  // Build caption and keyboard
  const captionParts = [];
  captionParts.push(`<b>${escapeHtml(batch.display_name || batch.filename)}</b>`);
  if (firstFile && firstFile.file_name) captionParts.push(`<b>File:</b> ${escapeHtml(firstFile.file_name)}`);
  captionParts.push(`<b>Token:</b> <code>${escapeHtml(batch.token)}</code>`);
  captionParts.push(`<b>Access:</b> <a href="${accessLink}">Open in bot</a>`);
  const captionHtml = captionParts.join('\n');

  const kb = {
    inline_keyboard: [
      [
        { text: 'Open', url: accessLink },
        { text: 'Edit caption', callback_data: `channel_edit_${batch.token}` },
        { text: 'Delete batch', callback_data: `channel_delete_${batch.token}` }
      ]
    ]
  };

  try {
    // Prefer sending the actual file with our caption so the "first saved message" is the one with the info
    if (firstFile && firstFile.type === 'photo' && firstFile.file_id) {
      return await bot.sendPhoto(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else if (firstFile && firstFile.type === 'video' && firstFile.file_id) {
      return await bot.sendVideo(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else if (firstFile && firstFile.type === 'document' && firstFile.file_id) {
      return await bot.sendDocument(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else {
      // fallback: send a plain message with the caption and keyboard
      return await bot.sendMessage(channelId, captionHtml, { parse_mode: 'HTML', reply_markup: kb });
    }
  } catch (e) {
    console.warn('postBatchToPrivateChannel failed', e && e.message);
    return null;
  }
}

// new: pending store-message flow (admin)
const pendingStoreMessages = {}; // chatId -> key awaiting HTML to be stored

// Broadcast newly added batch to all recorded users
// MODIFIED: Logs to file instead of console, sends report to Admin
async function broadcastNewBatchToAllUsers(batch) {
  const logBuffer = [];
  const log = (msg) => logBuffer.push(`[${new Date().toISOString()}] ${msg}`);
  
  try {
    const users = (fs.readdirSync(USER_DIR) || []).filter(f => f.endsWith('.js'));
    
    log(`Starting broadcast for batch: ${batch.token}`);
    log(`Display Name: ${batch.display_name || batch.filename}`);
    log(`Total User Files Found: ${users.length}`);
    log('------------------------------------------------');

    if (!users.length) {
      if (ADMIN_ID) await safeSendMessage(ADMIN_ID, 'Broadcast: no users found to notify.');
      return;
    }

    // prepare first file + caption similar to channel post
    const firstFile = (batch.files && batch.files[0]) || null;
    const display = batch.display_name || batch.filename || 'New upload';
    const accessLink = BOT_USERNAME
      ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}`
      : `https://t.me/?start=${encodeURIComponent(batch.token)}`;

    const captionParts = [];
    captionParts.push(`<b>New upload:</b> ${escapeHtml(display)}`);
    if (firstFile && firstFile.file_name) {
      captionParts.push(`<b>File:</b> ${escapeHtml(firstFile.file_name)}`);
    }
    captionParts.push(`<b>Token:</b> <code>${escapeHtml(batch.token)}</code>`);
    captionParts.push(`<b>Access:</b> <a href="${accessLink}">Open in bot</a>`);
    const captionHtml = captionParts.join('\n');

    let sent = 0;
    let failed = 0;

    for (const f of users) {
      let u = { id: 'unknown' };
      try {
        const p = path.join(USER_DIR, f);
        delete require.cache[require.resolve(p)];
        u = require(p);
        if (!u || !u.id) {
            log(`[SKIP] Invalid user file: ${f}`);
            continue;
        }

        // Send logic
        if (firstFile && firstFile.type === 'photo' && firstFile.file_id) {
          await bot.sendPhoto(u.id, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML' });
        } else if (firstFile && firstFile.type === 'video' && firstFile.file_id) {
          await bot.sendVideo(u.id, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML' });
        } else if (firstFile && firstFile.type === 'document' && firstFile.file_id) {
          await bot.sendDocument(u.id, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML' });
        } else if (firstFile && firstFile.type === 'text' && firstFile.text) {
          await safeSendMessage(u.id, captionHtml || firstFile.text, { parse_mode: 'HTML' });
        } else {
          await safeSendMessage(u.id, captionHtml, { parse_mode: 'HTML' });
        }

        sent++;
        // Optional: uncomment next line if you want a visual progress dot in terminal
        // process.stdout.write('.'); 
        await sleep(80); // small pause to reduce flood errors

      } catch (e) {
        failed++;
        const errorMsg = e.response && e.response.body && e.response.body.description 
          ? e.response.body.description 
          : (e.message || String(e));
        log(`[FAIL] User: ${u.id || f} | Error: ${errorMsg}`);
      }
    }

    log('------------------------------------------------');
    log(`COMPLETED. Sent: ${sent}, Failed: ${failed}, Total: ${users.length}`);

    // Send log file to Admin
    if (ADMIN_ID) {
      try {
        const buffer = Buffer.from(logBuffer.join('\n'), 'utf8');
        await bot.sendDocument(
          ADMIN_ID, 
          buffer, 
          { 
            caption: `📊 <b>Broadcast Report</b>\n\n✅ <b>Sent:</b> ${sent}\n❌ <b>Failed:</b> ${failed}\n📂 <b>Batch:</b> ${batch.token}`,
            parse_mode: 'HTML'
          },
          {
            filename: `broadcast_log_${batch.token}_${Date.now()}.txt`,
            contentType: 'text/plain'
          }
        );
      } catch (err) {
        console.error('Failed to send broadcast log file to admin:', err.message);
        await safeSendMessage(ADMIN_ID, `Broadcast complete.\nSent: ${sent}\nFailed: ${failed}\n(Log file could not be sent).`);
      }
    }

  } catch (e) {
    console.error('Broadcast Critical Fail:', e);
    if (ADMIN_ID) {
      try {
        await safeSendMessage(ADMIN_ID, '⚠️ Broadcast Critical Failure: ' + (e.message || String(e)));
      } catch (_) {}
    }
  }
}

// Ensure this returns the { chat_id, message_id } or null
async function updateOrPostBatchToChannel(batch, filename) {
  if (!PRIVATE_CHANNEL_ID) return null; // config not set
  const meta = readMeta() || {};
  meta.batch_meta = meta.batch_meta || {};

  // check existing saved channel message
  const saved = meta.batch_meta[filename] && meta.batch_meta[filename].channel_message;
  const accessLink = (typeof BOT_USERNAME !== 'undefined' && BOT_USERNAME) ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}` : `https://t.me/?start=${encodeURIComponent(batch.token)}`;

  // build caption
  const firstFile = (batch.files && batch.files[0]) || {};
  const captionParts = [];
  captionParts.push(`<b>${escapeHtml(batch.display_name || batch.filename || filename)}</b>`);
  if (firstFile && firstFile.file_name) captionParts.push(`<b>File:</b> ${escapeHtml(firstFile.file_name)}`);
  captionParts.push(`<b>Token:</b> <code>${escapeHtml(batch.token)}</code>`);
  captionParts.push(`<b>Access:</b> <a href="${accessLink}">Open in bot</a>`);
  const captionHtml = captionParts.join('\n');

  const kb = {
    inline_keyboard: [
      [
        { text: 'Open', url: accessLink },
        { text: 'Edit caption', callback_data: `channel_edit_${batch.token}` },
        { text: 'Delete batch', callback_data: `channel_delete_${batch.token}` }
      ]
    ]
  };

  try {
    if (saved && saved.chat_id && saved.message_id) {
      // try to edit existing message's caption. Use editMessageCaption for media messages; fallback to editMessageText
      try {
        await bot.editMessageCaption(captionHtml, { chat_id: saved.chat_id, message_id: saved.message_id, parse_mode: 'HTML', reply_markup: kb });
        meta.batch_meta[filename] = meta.batch_meta[filename] || {};
        meta.batch_meta[filename].channel_message = { chat_id: saved.chat_id, message_id: saved.message_id };
        writeMeta(meta);
        return { chat_id: saved.chat_id, message_id: saved.message_id };
      } catch (err) {
        // fallback: edit text
        try {
          await bot.editMessageText(captionHtml, { chat_id: saved.chat_id, message_id: saved.message_id, parse_mode: 'HTML', reply_markup: kb });
          meta.batch_meta[filename] = meta.batch_meta[filename] || {};
          meta.batch_meta[filename].channel_message = { chat_id: saved.chat_id, message_id: saved.message_id };
          writeMeta(meta);
          return { chat_id: saved.chat_id, message_id: saved.message_id };
        } catch (e2) {
          console.warn('Failed to edit existing channel message:', e2 && e2.message);
          // fall through and try to send a fresh message
        }
      }
    }

    // No saved channel message or edit failed — send a new channel message.
    // Prefer to attach the first file so the channel post is the "real" saved file.
    let sent = null;
    const channelId = PRIVATE_CHANNEL_ID;
    if (firstFile && firstFile.type === 'photo' && firstFile.file_id) {
      sent = await bot.sendPhoto(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else if (firstFile && firstFile.type === 'video' && firstFile.file_id) {
      sent = await bot.sendVideo(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else if (firstFile && firstFile.type === 'document' && firstFile.file_id) {
      sent = await bot.sendDocument(channelId, firstFile.file_id, { caption: captionHtml, parse_mode: 'HTML', reply_markup: kb });
    } else {
      sent = await bot.sendMessage(channelId, captionHtml, { parse_mode: 'HTML', reply_markup: kb });
    }

    if (sent && sent.chat && typeof sent.message_id !== 'undefined') {
      meta.batch_meta[filename] = meta.batch_meta[filename] || {};
      meta.batch_meta[filename].channel_message = { chat_id: sent.chat.id, message_id: sent.message_id };
      writeMeta(meta);
      return { chat_id: sent.chat.id, message_id: sent.message_id };
    }
  } catch (e) {
    console.error('updateOrPostBatchToChannel error', e && (e.stack || e.message));
    throw e;
  }

  return null;
}

// Hook this into your /doneadd flow: after you finalize and send the preview to the admin
// (Find your /doneadd handler where you already create `kb` and `previewText` — after sending that reply add:)
async function _doneadd_post_to_channel_if_configured(batch) {
  try {
    const sent = await postBatchToPrivateChannel(batch);
    if (sent && ADMIN_ID) {
      await safeSendMessage(ADMIN_ID, `Batch "${batch.display_name || batch.filename}" posted to private channel (message_id: ${sent.message_id}).`);
    }
  } catch (e) {
    console.warn('channel post after doneadd failed', e && e.message);
  }
}
// >>> Call `_doneadd_post_to_channel_if_configured(batch)` from your existing /doneadd flow
// (I recommend adding `await _doneadd_post_to_channel_if_configured(batch);` right after you send the preview message.)

// ---------- Channel callback handlers ----------
// Add into your existing callback_query handler (near the other `if (data === '...')` blocks)
bot.on('callback_query', async (q) => {
  try {
    const data = q.data || '';
    // handle only our special channel buttons here
    if (data && data.startsWith('channel_edit_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      const token = data.slice('channel_edit_'.length);
      // restrict to admin
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });
      const idx = readIndex();
      const filename = idx.tokens && idx.tokens[token];
      if (!filename) return safeSendMessage(ADMIN_ID, 'Batch not found for token: ' + token);
      const batch = readBatchFile(filename);
      if (!batch) return safeSendMessage(ADMIN_ID, 'Batch file missing.');

      // Start the edit-caption flow for admin programmatically (same as /edit_caption)
      pendingBatches[ADMIN_ID] = pendingBatches[ADMIN_ID] || {};
      pendingBatches[ADMIN_ID].editCaptionFlow = { token, filename, stage: 'await_index' };

      let list = `Editing captions for ${batch.filename} (token: ${token}). Files:\n`;
      batch.files.forEach((f, i) => {
        const n = f.file_name || (f.caption ? (String(f.caption).split(/\r?\n/)[0].slice(0,50)) : 'text');
        list += `${i+1}. ${n}\n`;
      });
      list += '\nReply with the file number to edit (1..' + (batch.files.length || 0) + ')';
      await safeSendMessage(ADMIN_ID, list);
      return;
    }

    if (data && data.startsWith('channel_delete_')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      // restrict to admin
      if (!q.from || q.from.id !== ADMIN_ID) return safeAnswerCallbackQuery(q.id, { text: 'Admin only', show_alert: true });
      const token = data.slice('channel_delete_'.length);
      const idx = readIndex();
      const filename = idx.tokens && idx.tokens[token];
      if (!filename) return safeSendMessage(ADMIN_ID, 'Token not found');

      // Delete batch file & remove from index (reuse your existing /deletefile logic)
      try {
        const filePath = filenameToPath(filename);
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
        delete idx.tokens[token];
        idx.order = (idx.order || []).filter(f => f !== filename);
        writeIndex(idx);
        const meta = readMeta();
        if (meta && meta.batch_meta && meta.batch_meta[filename]) { delete meta.batch_meta[filename]; writeMeta(meta); }

        await safeSendMessage(ADMIN_ID, `Deleted ${filename} (token ${token})`);

        // Update the channel message to show it was deleted
        try {
          const newCaption = `<b>DELETED:</b> ${escapeHtml(filename)}\nToken: <code>${escapeHtml(token)}</code>`;
          await bot.editMessageCaption(newCaption, { chat_id: q.message.chat.id, message_id: q.message.message_id, parse_mode: 'HTML' });
        } catch (e) {
          try { await bot.editMessageText(`DELETED: ${filename}\nToken: ${token}`, { chat_id: q.message.chat.id, message_id: q.message.message_id }); } catch (_){}
        }

      } catch (e) {
        console.error('channel delete failed', e && e.stack ? e.stack : e);
        await safeSendMessage(ADMIN_ID, 'Delete failed: ' + (e && e.message ? e.message : String(e)));
      }
      return;
    }

    // --- other callback handlers keep working below (do not block) ---
  } catch (err) {
    console.error('callback_query (channel handlers) error', err && (err.stack || err.message));
  }
});

// ---------- /addto and /doneaddto integration ----------

// Usage:
// /addto <TOKEN>     -> start appending files to existing batch (admin only)
// /doneaddto         -> finish appending; update/create channel message for that batch

// Start add-to session
bot.onText(/^\/addto(?:@?"+BOT_USERNAME+"?)?\s*(\S+)?/, async (msg, match) => {
  const chatId = msg.chat.id;
  if (!msg.from || msg.from.id !== ADMIN_ID) return safeSendMessage(chatId, 'Admin only.');

  const token = (match && match[1]) ? String(match[1]).trim() : null;
  if (!token) return safeSendMessage(chatId, 'Usage: /addto <TOKEN>');

  const idx = readIndex();
  const filename = idx.tokens && idx.tokens[token];
  if (!filename) return safeSendMessage(chatId, `Token not found: ${token}`);

  const batch = readBatchFile(filename);
  if (!batch) return safeSendMessage(chatId, `Batch file missing: ${filename}`);

  pendingBatches[chatId] = pendingBatches[chatId] || {};
  pendingBatches[chatId].mode = 'addto';
  pendingBatches[chatId].token = token;
  pendingBatches[chatId].filename = filename;

  await safeSendMessage(chatId, `Add mode started for token <code>${token}</code> (batch: ${filename}).\nSend files now. When finished send /doneaddto or /cancel.`, { parse_mode: 'HTML' });
});

bot.onText(/^\/favorites(?:@\w+)?(?:\s+(\d+))?$/i, async (msg, match) => {
  const fromId = msg?.from?.id;
  const chatId = msg?.chat?.id;
  const page = match && match[1] ? parseInt(match[1], 10) : 1;

  const view = buildFavoritesView(fromId, page);
  await safeSendMessage(chatId, view.text, {
    parse_mode: 'HTML',
    reply_markup: view.reply_markup
  });
});

// Finish add-to session
bot.onText(/^\/doneaddto(?:@?"+BOT_USERNAME+"?)?/, async (msg) => {
  const chatId = msg.chat.id;
  if (!msg.from || msg.from.id !== ADMIN_ID) return safeSendMessage(chatId, 'Admin only.');
  const pending = pendingBatches[chatId];
  if (!pending || pending.mode !== 'addto') return safeSendMessage(chatId, 'No active /addto session.');

  const filename = pending.filename;
  delete pendingBatches[chatId];

  const batch = readBatchFile(filename);
  if (!batch) return safeSendMessage(chatId, 'Batch not found on disk.');

  // Save batch (if your file-append already wrote per-file, this is extra safety)
  writeBatchFile(filename, batch);

  // Update or create the channel post and save the channel message info in meta
  try {
    const channelMsg = await updateOrPostBatchToChannel(batch, filename);
    if (channelMsg) {
      await safeSendMessage(chatId, `Done. Channel message updated/created (chat: ${channelMsg.chat_id}, message: ${channelMsg.message_id}).`);
    } else {
      await safeSendMessage(chatId, 'Done. (private channel not configured or bot lacks permissions).');
    }
  } catch (e) {
    console.error('doneaddto channel update error', e && e.stack ? e.stack : e);
    await safeSendMessage(chatId, 'Done, but failed to update private channel: ' + (e.message || String(e)));
  }
});

// === FIND ID feature ===
// Add near other pending-request maps at top of file
const pendingFindRequests = {}; // chatId -> { mode: 'link'|'forward', userId, ts }

// /findid command - shows options
bot.onText(/^\/findid(?:@?${BOT_USERNAME})?(?:\s+(.+))?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const who = msg.from && msg.from.id;
  // interactive keyboard: share a link or forward a message
  const kb = {
    inline_keyboard: [
      [{ text: '🔗 Share link', callback_data: 'findid_link' }, { text: '📤 Forward msg', callback_data: 'findid_forward' }],
      [{ text: '❓ How it works', callback_data: 'findid_help' }]
    ]
  };
  await safeSendMessage(chatId, 'Choose method to find ID — share a link (t.me/username, t.me/c/...) or forward a message from the user/channel/group/bot you want the ID for.', { reply_markup: kb });
});

// callback handlers
bot.on('callback_query', async (q) => {
    
  try {
    const data = q.data;
    const chatId = q.message ? q.message.chat.id : (q.from && q.from.id);

    // --- Channel message browser navigation (skip hidden entries) ---
    if (data && data.startsWith('chmsg_')) {
      // acknowledge the callback quickly
      await safeAnswerCallbackQuery(q.id).catch(()=>{});

      const chatId = q.message && q.message.chat ? q.message.chat.id : (q.message ? q.message.chat.id : null);
      const [actionRaw, idxStr] = (data || '').split('|');
      const action = actionRaw;  // 'chmsg_prev' / 'chmsg_next' / 'chmsg_close' / 'chmsg_nop'
      const idx = Number(idxStr || '0');

      const meta = getMsgMeta();
      const items = meta && meta.channel_forwards ? meta.channel_forwards : [];
      const total = items.length;

      if (!total) {
        await safeSendMessage(chatId, 'No channel messages configured yet.');
        return;
      }

      // Close = delete current message only
      if (action === 'chmsg_close') {
        try { await bot.deleteMessage(chatId, q.message.message_id); } catch (e) {}
        return;
      }

      // No-op (page indicator)
      if (action === 'chmsg_nop') {
        return;
      }

      // helper: find next visible index from `start`, direction dir (+1 or -1), skipping hidden items
      function findAdjacentVisibleIndex(start, dir) {
        let i = start + dir;
        while (i >= 0 && i < items.length) {
          if (!items[i] || !items[i].hidden) return i;
          i += dir;
        }
        return -1;
      }

      // compute direction
      let dir = 0;
      if (action === 'chmsg_prev') dir = -1;
      if (action === 'chmsg_next') dir = 1;
      if (dir === 0) return;

      // find the next visible index in that direction
      const nextIdx = findAdjacentVisibleIndex(idx, dir);

      if (nextIdx === -1) {
        const txt = dir === -1 ? 'No previous visible messages.' : 'No more visible messages.';
        await safeAnswerCallbackQuery(q.id, { text: txt, show_alert: false }).catch(()=>{});
        return;
      }

            // navigate: delete current inline message and show the next visible one
      try {
        // delete current inline message (clean UI)
        try { await bot.deleteMessage(chatId, q.message.message_id); } catch (e) {}

        // copy the next visible channel message (no force, so hidden items remain skipped)
        await sendChannelForwardByIndex(chatId, nextIdx, { force: false });
        // done — no need to answer the callback again (we accepted it at the top)
      } catch (err) {
        console.error('chmsg navigation error', err && (err.stack || err));
        await safeAnswerCallbackQuery(q.id, { text: 'Failed to open message.' }).catch(()=>{});
      }

      return;
    }

    if (!chatId) return safeAnswerCallbackQuery(q.id, { text: 'No chat' });

    if (data === 'findid_link') {
      pendingFindRequests[chatId] = { mode: 'link', ts: Date.now() };
      await safeAnswerCallbackQuery(q.id, { text: 'Send the link or username now (e.g. https://t.me/username or @username or https://t.me/c/194xxx987/23).' });
      await safeSendMessage(chatId, 'Paste the link or username here. Examples:\n• https://t.me/username\n• @username\n• https://t.me/c/194xxxx987/11\n• or paste a numeric chat id (if you have it).');
      return;
    }

    if (data === 'findid_forward') {
      pendingFindRequests[chatId] = { mode: 'forward', ts: Date.now() };
      await safeAnswerCallbackQuery(q.id, { text: 'Now forward a message from the target (user / channel / group / bot).' });
      await safeSendMessage(chatId, 'Forward a message from the target into this chat — the bot will try to extract the origin id from the forwarded message.');
      return;
    }

    if (data === 'findid_help') {
      await safeAnswerCallbackQuery(q.id, { text: 'Explained' });
      const help = `How /findid works:
        • Public username (@username or t.me/username): bot calls getChat and returns the chat id.
        • Internal group link (t.me/c/NNNNNNNNN/...): bot can compute -100NNNNNNNNN which works as chat_id in API.
        • Forwarded message: most forwarded messages contain origin info (forward_from or forward_from_chat) which reveals id.
        • Invite/join links (t.me/+ABC...): bots cannot always resolve these to numeric ids unless bot is admin or the chat owner cooperates.`;
      await safeSendMessage(chatId, help);
      return;
    }

    // Pagination: go to a page
    if (data && data.startsWith('fav_page_')) {
      const targetPage = parseInt(data.replace('fav_page_', ''), 10) || 1;
      const view = buildFavoritesView(fromId, targetPage);
      try {
        await bot.editMessageText(view.text, {
          chat_id: chatId, message_id: q.message.message_id,
          parse_mode: 'HTML', reply_markup: view.reply_markup
        });
      } catch (e) {
        // If edit fails (e.g., old message), just send a new one
        await safeSendMessage(chatId, view.text, { parse_mode: 'HTML', reply_markup: view.reply_markup });
      }
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      return;
    }

    // Remove one favorite (includes current page in callback)
    if (data && data.startsWith('fav_rm_')) {
      // format: fav_rm_<page>_<token>
      const parts = data.split('_');
      const currentPage = parseInt(parts[2], 10) || 1;
      const token = parts.slice(3).join('_'); // token may contain underscores
      favRemove(fromId, token);

      const view = buildFavoritesView(fromId, currentPage);
      try {
        await bot.editMessageText(view.text, {
          chat_id: chatId, message_id: q.message.message_id,
          parse_mode: 'HTML', reply_markup: view.reply_markup
        });
      } catch (e) {
        await safeSendMessage(chatId, view.text, { parse_mode: 'HTML', reply_markup: view.reply_markup });
      }
      await safeAnswerCallbackQuery(q.id, { text: 'Removed from favorites' }).catch(()=>{});
      return;
    }

    // Inside callback_query handler, alongside other cases
    if (data.startsWith('fav_add_')) {
      const token = data.slice('fav_add_'.length);
      favAdd(fromId, token);
      await safeAnswerCallbackQuery(q.id, { text: 'Saved to favorites ❤️' }).catch(()=>{});

      try {
        const kb = q.message.reply_markup;
        // replace second button in first row to "Remove"
        if (kb?.inline_keyboard?.[0]?.[1]) {
          kb.inline_keyboard[0][1] = { text: '❌ Remove', callback_data: `fav_remove_${token}` };
          await bot.editMessageReplyMarkup(kb, { chat_id: chatId, message_id: q.message.message_id });
        }
      } catch(_) {}
      return;
    }

    if (data.startsWith('fav_remove_')) {
      const token = data.slice('fav_remove_'.length);
      favRemove(fromId, token);
      await safeAnswerCallbackQuery(q.id, { text: 'Removed from favorites' }).catch(()=>{});

      try {
        const kb = q.message.reply_markup;
        if (kb?.inline_keyboard?.[0]?.[1]) {
          kb.inline_keyboard[0][1] = { text: '❤️ Save', callback_data: `fav_add_${token}` };
          await bot.editMessageReplyMarkup(kb, { chat_id: chatId, message_id: q.message.message_id });
        }
      } catch(_) {}
      return;
    }

  } catch (e) {
    console.error('callback_query /findid error', e && e.stack || e);
  }
});

// main message handler: pick up link or forwarded messages when pending
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.chat) return;
    const chatId = msg.chat.id;
    const pending = pendingFindRequests[chatId];
    if (!pending) return; // not in a find flow

    // consume request after handling
    delete pendingFindRequests[chatId];

    // ---------- MODE: link ----------
    if (pending.mode === 'link') {
      const text = (msg.text || '').trim();
      if (!text) return safeSendMessage(chatId, 'No text detected. Please paste a link or username (e.g. https://t.me/username or @username).');

      // helper replies
      const replyWithError = async (t) => {
        await safeSendMessage(chatId, `Could not resolve link: ${t}\nTip: try forwarding a message from the target or use a public username (t.me/username).`);
      };

      // 1) t.me/c/<num>/... pattern -> derive supergroup id with -100 prefix
      const tmeC = text.match(/t\.me\/c\/(\d+)/i) || text.match(/https?:\/\/t\.me\/c\/(\d+)/i);
      if (tmeC) {
        const raw = tmeC[1];
        // API uses -100 prefix for supergroup internal id
        const chat_id = `-100${raw}`;
        return safeSendMessage(chatId, `Derived chat id: <code>${chat_id}</code>\n(Use this id with bot API calls — note: the bot must be a member of the chat to perform many actions.)`, { parse_mode: 'HTML' });
      }

      // 2) username or @username or plain username
      const usernameMatch = text.match(/(?:https?:\/\/)?t\.me\/(@?[A-Za-z0-9_]{5,})/i) || text.match(/^@?([A-Za-z0-9_]{5,})$/i);
      if (usernameMatch) {
        const uname = usernameMatch[1].startsWith('@') ? usernameMatch[1] : `@${usernameMatch[1]}`;
        try {
          const chat = await bot.getChat(uname);
          // format chat info
          let out = `Chat info for ${uname}:\n`;
          out += `• id: <code>${chat.id}</code>\n`;
          out += `• type: ${chat.type || 'unknown'}\n`;
          if (chat.title) out += `• title: ${escapeHtml(chat.title)}\n`;
          if (chat.username) out += `• username: @${chat.username}\n`;
          if (chat.invite_link) out += `• invite_link: ${escapeHtml(chat.invite_link)}\n`;
          if (chat.first_name || chat.last_name) out += `• name: ${escapeHtml(((chat.first_name||'') + ' ' + (chat.last_name||'')).trim())}\n`;
          if (chat.is_forum) out += `• forum: yes\n`;
          return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
        } catch (err) {
          // getChat failed — likely bot not in the chat or username invalid
          console.warn('getChat failed for', uname, err && err.message);
          return replyWithError(`getChat(@${usernameMatch[1]}) failed: ${err && err.message ? err.message : 'API error or bot lacks access'}`);
        }
      }

      // 3) numeric id supplied directly
      if (/^-?\d+$/.test(text)) {
        try {
          const chat = await bot.getChat(Number(text));
          const out = `Chat info for numeric id:\n• id: <code>${chat.id}</code>\n• type: ${chat.type || 'unknown'}\n${chat.title ? `• title: ${escapeHtml(chat.title)}\n` : ''}`;
          return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
        } catch (err) {
          console.warn('getChat by numeric id failed', err && err.message);
          return replyWithError('getChat by numeric id failed: ' + (err && err.message));
        }
      }

      // 4) invite / join links (t.me/+... or t.me/joinchat/...)
      if (text.match(/t\.me\/\+|t\.me\/joinchat/i)) {
        // Bot API limitation: bots cannot always convert an arbitrary join link to the numeric id unless the bot is admin or the link was created by a bot admin with rights.
        return safeSendMessage(chatId,
          'This looks like an invite/join link (t.me/+... or t.me/joinchat/...).\n' +
          'Bots cannot reliably convert an arbitrary invite link into a numeric chat id unless the bot is an administrator in that chat or the chat owner cooperates.\n\n' +
          'Options:\n• Add the bot to the chat (then use /findid again or forward a message from that chat).\n• Ask an admin of that group/channel to forward a message from the chat to this bot.\n• If the chat has a public username, paste the username (t.me/username).'
        );
      }

      // fallback
      return replyWithError('Unrecognized link format. Try a public username, a t.me/c/... link, or forward a message from the target.');

    } // end pending.mode === 'link'

    // ---------- MODE: forward ----------
    if (pending.mode === 'forward') {
      // forwarded user?
      if (msg.forward_from) {
        const u = msg.forward_from;
        const isBot = !!u.is_bot;
        let out = `Forwarded-from user detected:\n• id: <code>${u.id}</code>\n• name: ${escapeHtml(((u.first_name||'') + ' ' + (u.last_name||'')).trim())}\n`;
        if (u.username) out += `• username: @${u.username}\n`;
        out += `• is_bot: ${isBot}\n`;
        return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
      }

      // forwarded chat (channel / anonymous admin)
      if (msg.forward_from_chat) {
        const c = msg.forward_from_chat;
        let out = `Forwarded-from chat detected:\n• id: <code>${c.id}</code>\n• type: ${c.type}\n`;
        if (c.title) out += `• title: ${escapeHtml(c.title)}\n`;
        if (c.username) out += `• username: @${c.username}\n`;
        if (c.invite_link) out += `• invite_link: ${escapeHtml(c.invite_link)}\n`;
        return safeSendMessage(chatId, out, { parse_mode: 'HTML' });
      }

      // forwarded channel post info (forward_from_message_id)
      if (msg.forward_from_message_id && msg.forward_from_chat === undefined) {
        // sometimes bots receive limited forwarded info; still show what we have
        return safeSendMessage(chatId, `Message appears forwarded but lacked origin metadata the bot can use.\nTry asking an admin to forward a channel post directly or invite the bot to the chat and resend.`);
      }

      // nothing detected
      return safeSendMessage(chatId, 'This forwarded message did not include origin information usable by bots (it may be protected). Try forwarding a different message or ask an admin to add/forward for you.');
    }

  } catch (e) {
    console.error('findid message handler error', e && e.stack || e);
  }
});

// Example snippet: append incoming files when admin sends them (you probably already have general file handlers)
async function handleIncomingFileMessage(msg, fileMeta) {
  // fileMeta must include { type: 'photo'|'video'|'document'|..., file_id, file_name, caption }
  const chatId = msg.chat.id;
  const pending = pendingBatches[chatId];
  if (pending && pending.mode === 'addto') {
    const filename = pending.filename;
    const batch = readBatchFile(filename);
    if (!batch) {
      await safeSendMessage(chatId, 'Batch missing (stopped).');
      delete pendingBatches[chatId];
      return;
    }

    batch.files = batch.files || [];
    // push minimal metadata needed for later sending (file_id, file_name, caption, type, etc.)
    batch.files.push({
      file_id: fileMeta.file_id,
      file_name: fileMeta.file_name || ('file_' + (batch.files.length + 1)),
      type: fileMeta.type,
      caption: fileMeta.caption || ''
    });

    writeBatchFile(filename, batch);
    await safeSendMessage(chatId, `Appended: ${fileMeta.file_name || 'file'} (now ${batch.files.length} files)`);
    return true;
  }

  // not handled here — return false to let other handlers process
  return false;
}

// -------------------- Message Store with MSG tokens --------------------
// Requires: readMeta(), writeMeta(), generateToken(), safeSendMessage(), safeAnswerCallbackQuery(), bot, ADMIN_ID
// Safe global-backed pending map (prevents "before initialization" and keeps data across reloads)
global.__pendingTextOps = global.__pendingTextOps || {};
const pendingTextOps = global.__pendingTextOps;  // chatId -> { mode:'store'|'addto'|'set'|'await_store_key'|'await_addto_key', key, index }

// NEW: pending map for collecting channel links
global.__pendingChannelLinks = global.__pendingChannelLinks || {};
const pendingChannelLinks = global.__pendingChannelLinks; // chatId -> { mode:'collect', added }

function getMsgMeta() {
  const meta = (typeof readMeta === 'function') ? (readMeta() || {}) : {};
  meta.saved_texts = meta.saved_texts || {};  // { key: [ html1, html2, ... ] }
  meta.msg_tokens = meta.msg_tokens || {};    // { MSG-XXXX: { key, index } }
  // NEW: list of channel forward entries
  meta.channel_forwards = meta.channel_forwards || []; // [ { source_chat_id, source_message_id, link } ]
  return meta;
}
function saveMsgMeta(meta) { if (typeof writeMeta === 'function') writeMeta(meta); }

// generate message token (distinct prefix from batch tokens)
function makeMsgToken() {
  // use existing generateToken but shorter & prefix MSG-
  const t = (typeof generateToken === 'function') ? generateToken(8) : (Math.random().toString(36).slice(2,10).toUpperCase());
  return `MSG-${String(t).toUpperCase()}`;
}

// helper to register token -> key/index mapping
function registerMsgToken(token, key, index) {
  const meta = getMsgMeta();
  meta.msg_tokens = meta.msg_tokens || {};
  meta.msg_tokens[token] = { key, index, createdAt: new Date().toISOString() };
  saveMsgMeta(meta);
}

// ---- msg view counters (for simple unread/view tracking) ----
function incrementMsgView(token) {
  if (!token) return;
  const meta = getMsgMeta();
  meta.msg_views = meta.msg_views || {};
  meta.msg_views[token] = (meta.msg_views[token] || 0) + 1;
  if (typeof saveMsgMeta === 'function') saveMsgMeta(meta);
}

function getMsgViewCount(token) {
  const meta = getMsgMeta();
  return (meta.msg_views && meta.msg_views[token]) ? meta.msg_views[token] : 0;
}

// build message-keys keyboard (shows admin-only buttons only when asAdmin=true)
function buildMsgKeysKeyboard(asAdmin = false) {
  const meta = getMsgMeta();
  const keys = Object.keys(meta.saved_texts || {});
  if (!keys.length) return {
    text: 'No saved message keys yet. Use /msgstore <key> to store a message.',
    reply_markup: { inline_keyboard: [] },
    parse_mode: 'HTML'
  };
  const rows = keys.map(k => [{ text: k, callback_data: `msg_key_open|${k}` }]);

  // admin shortcuts row (only show to admin)
  if (asAdmin) {
    rows.push([
      { text: '➕ New key (store)', callback_data: 'admin_msg_store' },
      { text: '🧾 Help', callback_data: 'admin_msg_help' }
    ]);
  }

  return { text: '<b>Saved message keys</b>\nTap a key to view its items.', reply_markup: { inline_keyboard: rows }, parse_mode: 'HTML' };
}

// Build admin key-list menu used by "Admin: list keys"
function buildKeysMenu(meta) {
  meta = meta || getMsgMeta();
  const saved = meta.saved_texts || {};
  const keys = Object.keys(saved).sort();

  // Header text
  let text = `<b>Message Store — Keys</b>\n\n`;
  if (!keys.length) {
    text += 'No keys yet. Use "➕ Add" to create one.';
  } else {
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      const count = Array.isArray(saved[k]) ? saved[k].length : 0;
      // shorten preview safely
      const short = (String(k).length > 64) ? (String(k).slice(0, 60) + '…') : String(k);
      text += `${i + 1}. ${escapeHtml(short)} — ${count} item(s)\n`;
    }
  }

  // Build inline keyboard: one row per key (Open | Publish) + footer admin actions
  const rows = [];

  for (const k of keys) {
    // callback_data must be URL-safe and within Telegram limit — encodeURIComponent helps
    const encoded = encodeURIComponent(k);
    // Open key (inline preview) and Open public page (if exists)
    const openBtn = { text: '🔎 Open', callback_data: `msg_key_open|${encoded}` };
    const pubBtn = { text: '🌐 Page', callback_data: `msg_key_page|${encoded}` }; // handler should show cached page or ask admin
    rows.push([ openBtn, pubBtn ]);
  }

  // footer actions
  rows.push([{ text: '➕ Add key', callback_data: 'admin_msg_add' }]);
  rows.push([{ text: '♻️ Rebuild Index', callback_data: 'msgls_rebuild' }, { text: '⬅️ Back', callback_data: 'msg_back' }]);

  return {
    text,
    reply_markup: { inline_keyboard: rows },
    parse_mode: 'HTML'
  };
}

// view key -> item list (asAdmin controls destructive/actions)
function buildMsgKeyView(key, asAdmin = false) {
  const meta = getMsgMeta();
  const arr = meta.saved_texts && meta.saved_texts[key] ? meta.saved_texts[key] : [];
  let text = `<b>${escapeHtml(String(key))}</b> — ${arr.length} item(s)\n\n`;
  const rows = [];

  if (arr.length === 0) {
    text += asAdmin
      ? 'No items. Use "➕ Add to this key" to append a new HTML message.'
      : 'No items yet.';
  } else {
    arr.forEach((html, i) => {
      const preview = (String(html).replace(/<[^>]*>/g, '') || '').slice(0, 60);
      text += `${i + 1}. ${escapeHtml(preview)}${preview.length === 60 ? '…' : ''}\n`;

      // find msg-token(s) that map to this key/index (there may be none)
      const metaTokens = meta.msg_tokens || {};
      const tokenList = [];
      for (const tk of Object.keys(metaTokens)) {
        const v = metaTokens[tk];
        if (v && v.key === key && Number(v.index) === Number(i)) tokenList.push(tk);
      }

      const viewBtn = { text: `👁 View ${i + 1}`, callback_data: `msg_item_view|${key}|${i}` };

      if (asAdmin) {
        const rmBtn = { text: `❌ Remove ${i + 1}`, callback_data: `msg_item_rm|${key}|${i}` };

        // Primary token button (show first token + its view count)
        let tokBtn;
        if (tokenList.length) {
          const primary = tokenList[0];
          // get view count if helper exists
          let views = 0;
          try { views = (typeof getMsgViewCount === 'function') ? getMsgViewCount(primary) || 0 : 0; } catch (e) { views = 0; }
          tokBtn = {
            text: `🔐 ${primary}${views ? ` (${views})` : ''}`,
            callback_data: `msg_token_copy|${primary}`
          };
        } else {
          tokBtn = { text: '🔐 Create token', callback_data: `msg_token_create|${key}|${i}` };
        }

        // If multiple tokens exist, add a small extra button indicating there are more
        if (tokenList.length > 1) {
          const moreBtn = { text: `🔐 +${tokenList.length - 1} more`, callback_data: `msg_token_list|${key}|${i}` };
          rows.push([viewBtn, rmBtn, tokBtn, moreBtn]);
        } else {
          rows.push([viewBtn, rmBtn, tokBtn]);
        }
      } else {
        rows.push([viewBtn]); // users: pure view
      }
    });
  }

  if (asAdmin) {
    rows.push([{ text: '➕ Add to this key', callback_data: `msg_item_add|${key}` }]);
    rows.push([{ text: '🗑 Delete key', callback_data: `msg_key_rm|${key}` }, { text: '⬅️ Back', callback_data: 'msg_back' }]);
  } else {
    rows.push([{ text: '⬅️ Back', callback_data: 'msg_back' }]);
  }

  return { text, reply_markup: { inline_keyboard: rows }, parse_mode: 'HTML' };
}

// ---------------- New index commands: /indx, /idxls, /exportindex, /sortuploads ----------------

// /indx <page?>  — show index pages (like existing index UI, admin gets extra controls)
bot.onText(/^\/indx(?:@\w+)?(?:\s+(\d+))?$/i, async (msg, match) => {
  try {
    const chatId = msg.chat.id;
    const fromId = msg.from && msg.from.id;
    const page = match && match[1] ? Math.max(1, parseInt(match[1], 10)) : 1;
    const isAdmin = Boolean(ADMIN_ID && String(fromId) === String(ADMIN_ID));
    const idxPayload = buildIndexTextAndKeyboardQuick(Math.max(0, page - 1), isAdmin);
    await safeSendMessage(chatId, idxPayload.text, { parse_mode: 'HTML', reply_markup: idxPayload.keyboard });
  } catch (e) {
    console.error('/indx error', e && e.message);
    await safeSendMessage(msg.chat.id, 'Failed to show index.');
  }
});

// /idxls — open index in the same way /chls opens channel messages (quick launcher + immediate open)
bot.onText(/^\/idxls(?:@\w+)?$/i, async (msg) => {
  try {
    const chatId = msg.chat.id;
    const idx = readIndex() || {};
    const order = Array.isArray(idx.order) ? idx.order : [];
    if (!order.length) return safeSendMessage(chatId, 'No batches available.');

    // Build first-page payload and send it (the payload keyboard already includes pagination)
    const idxPayload = buildIndexTextAndKeyboardQuick(0, (msg.from && msg.from.id === ADMIN_ID));
    const sent = await bot.sendMessage(chatId, idxPayload.text, { parse_mode: 'HTML', reply_markup: idxPayload.keyboard });
    // No further action needed — pagination callbacks are already wired.
  } catch (e) {
    console.error('/idxls error', e && e.message);
    await safeSendMessage(msg.chat.id, 'Failed to open index list.');
  }
});

// Admin-only: /exportindex <target>  -> send the full /listfiles-style text to another chat (id or @username)
bot.onText(/^\/exportindex(?:@\w+)?\s+(.+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const targetRaw = (match && match[1]) ? match[1].trim() : null;
  if (!targetRaw) {
    return safeSendMessage(chatId, 'Usage: /exportindex <chat_id|@username|-100...id>');
  }

  // Resolve target chat id/username
  let target = null;
  try {
    if (/^-?\d+$/.test(targetRaw)) {
      // numeric id (group/channel/user)
      target = Number(targetRaw);
    } else {
      // assume @username or plain username
      const lookup = targetRaw.startsWith('@') ? targetRaw : ('@' + targetRaw);
      const info = await bot.getChat(lookup).catch(() => null);
      if (info && info.id) target = info.id;
    }
  } catch (_) {
    target = null;
  }

  if (!target) {
    return safeSendMessage(
      chatId,
      'Could not resolve target chat. Use numeric id or @username and ensure the bot is a member of the target.'
    );
  }

  // Reuse /listfiles logic to render full export
  try {
    const idx = readIndex() || {};
    const order = (idx && Array.isArray(idx.order)) ? idx.order : [];
    if (!order.length) {
      return safeSendMessage(chatId, 'No batches to export.');
    }

    const h = (s) => {
      if (s == null) return '';
      return String(s).replace(/[&<>"']/g, (ch) =>
        ch === '&' ? '&amp;'
        : ch === '<' ? '&lt;'
        : ch === '>' ? '&gt;'
        : ch === '"' ? '&quot;'
        : '&#39;'
      );
    };

    // Build the full listing (no pagination) — similar to /listfiles page 1 but includes all
    let out = `Batches (send order) — total ${order.length}:\n\n`;
    for (let i = 0; i < order.length; i++) {
      const fname = order[i];
      const token = (idx && idx.tokens)
        ? Object.keys(idx.tokens).find(t => idx.tokens[t] === fname) || ''
        : '';
      const batch = readBatchFile(fname) || null;
      const name = (batch && batch.display_name) ? batch.display_name : fname;
      const n = i + 1;

      out += `${n}. ${h(name)} — <code>${h(fname)}</code> | <code>${h(token)}</code>\n`;
      if (token) {
        out += `   /start_${h(token)}\n`;
        out += `   <code>/deletefile ${h(token)}</code>\n`;
      }
      out += '\n';
    }

    // ✅ Send in safe chunks instead of one huge message
    await sendLongHtmlMessage(
      target,
      out,
      { disable_web_page_preview: true } // parse_mode is set inside helper
    );

    await safeSendMessage(
      chatId,
      `Exported ${order.length} batches to ${target}.`
    );
  } catch (e) {
    console.error('/exportindex failed', e && e.stack ? e.stack : e);
    await safeSendMessage(
      chatId,
      'Export failed: ' + (e && e.message ? e.message : 'unknown error')
    );
  }
});

// /sortuploads [<target>] — quick categorization of all batches into Movie / Series / Recommended
// admin-only. If <target> provided, post result there; otherwise reply in current chat.
bot.onText(/^\/sortuploads(?:@\w+)?(?:\s+(.+))?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');

  const postTargetRaw = (match && match[1]) ? match[1].trim() : null;
  let postTarget = chatId;
  if (postTargetRaw) {
    try {
      if (/^-?\d+$/.test(postTargetRaw)) postTarget = Number(postTargetRaw);
      else {
        const info = await bot.getChat(postTargetRaw.startsWith('@') ? postTargetRaw : ('@' + postTargetRaw)).catch(()=>null);
        if (info && info.id) postTarget = info.id;
      }
    } catch (_) { postTarget = null; }
    if (!postTarget) return safeSendMessage(chatId, 'Could not resolve target chat for posting the report.');
  }

  try {
    const idx = readIndex() || {};
    const order = Array.isArray(idx.order) ? idx.order.slice() : [];
    if (!order.length) return safeSendMessage(chatId, 'No batches found.');

    const movies = [], series = [], recommended = [], others = [];

    function classifyBatch(b) {
      const name = (b && (b.display_name || b.filename || '')).toString().toLowerCase();
      const caption = (b && Array.isArray(b.files) && b.files.length && (b.files[0].caption || b.files[0].text || '')) ? (b.files[0].caption || b.files[0].text || '') : '';
      const sample = (name + ' ' + String(caption)).toLowerCase();

      if (sample.includes('recommended') || sample.includes('recommend') || sample.includes('⭐') || sample.includes('top picks') || sample.includes('recommended movies')) return 'recommended';
      if (sample.includes('movie') || sample.includes('🎬') || sample.match(/\bfilm\b/)) return 'movie';
      if (sample.includes('series') || sample.includes('season') || sample.match(/\bepisod(e|es)\b/) || sample.match(/\bs\d{1,2}e\d{1,2}\b/)) return 'series';
      return 'other';
    }

    for (let i = 0; i < order.length; i++) {
      const fname = order[i];
      const batch = readBatchFile(fname) || { filename: fname, files: [] };
      const kind = classifyBatch(batch);
      const token = Object.keys(idx.tokens || {}).find(t => idx.tokens[t] === fname) || '';
      const entry = { index: i+1, filename: fname, display: batch.display_name || fname, token, items: Array.isArray(batch.files) ? batch.files.length : 0 };
      if (kind === 'movie') movies.push(entry);
      else if (kind === 'series') series.push(entry);
      else if (kind === 'recommended') recommended.push(entry);
      else others.push(entry);
    }

    const makeListText = (arr, title) => {
      if (!arr.length) return `<b>${title}:</b> (none)\n\n`;
      const lines = arr.slice(0, 200).map(a => `${a.index}. ${escapeHtml(a.display)} — <code>${escapeHtml(a.filename)}</code> | <code>${escapeHtml(a.token)}</code> (items:${a.items})`);
      return `<b>${title} (${arr.length}):</b>\n` + lines.join('\n') + '\n\n';
    };

    let report = '';
    report += makeListText(recommended, 'Recommended');
    report += makeListText(movies, 'Movies');
    report += makeListText(series, 'Series');
    report += makeListText(others, 'Others / Uncategorized');

    // If lists were very long, indicate how to view full list with /exportindex
    report += `\nUse <code>/exportindex &lt;chat_id|@username&gt;</code> to export full list to another chat.\n`;

    await safeSendMessage(postTarget, report, { parse_mode: 'HTML', disable_web_page_preview: true });
    if (postTarget !== chatId) await safeSendMessage(chatId, `Posted report to ${postTarget}.`);
  } catch (e) {
    console.error('/sortuploads failed', e && e.stack ? e.stack : e);
    await safeSendMessage(chatId, 'Failed to sort uploads: ' + (e && e.message ? e.message : 'unknown error'));
  }
});

// ---------------- Commands ----------------

// /msgstore <key>
bot.onText(/^\/msgstore(?:@\w+)?\s+(\S+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const key = (match && match[1]) ? String(match[1]).trim() : null;
  if (!key) return safeSendMessage(chatId, 'Usage: /msgstore <key>');

  pendingTextOps[chatId] = { mode: 'store', key };

  const renderedExample =
    `<b>Baaghi [2016]</b><a href="https://t.me/Cloudmakerbot?start=ZQ3JQ2727CDN">Click here👈</a>`;
  const escapedExample = escapeForPre(renderedExample);

  const text =
    `Send the <b>HTML-formatted</b> message to store under ` +
    `<code>${escapeHtml(key)}</code> as item #1.\n\n` +
    `Preview (rendered):\n` +
    `${renderedExample}\n\n` +
    `Markup (copy/paste the HTML below):\n` +
    `<pre>${escapedExample}</pre>`;

  return safeSendMessage(chatId, text, {
    parse_mode: 'HTML',
    disable_web_page_preview: true
  });
});

// /msgaddto <key>
bot.onText(/^\/msgaddto(?:@\w+)?\s+(\S+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const key = (match && match[1]) ? String(match[1]).trim() : null;
  if (!key) return safeSendMessage(chatId, 'Usage: /msgaddto <key>');

  pendingTextOps[chatId] = { mode: 'addto', key };

  const renderedExample =
    `<b>Baaghi [2016]</b><a href="https://t.me/Cloudmakerbot?start=ZQ3JQ2727CDN">Click here👈</a>`;
  const escapedExample = escapeForPre(renderedExample);

  const text =
    `Send the <b>HTML-formatted</b> message to <b>append</b> to ` +
    `<code>${escapeHtml(key)}</code>.\n\n` +
    `Preview (rendered):\n` +
    `${renderedExample}\n\n` +
    `Markup (copy/paste the HTML below):\n` +
    `<pre>${escapedExample}</pre>`;

  return safeSendMessage(chatId, text, {
    parse_mode: 'HTML',
    disable_web_page_preview: true
  });
});

// /msgset <key> <index>
bot.onText(/^\/msgset(?:@\w+)?\s+(\S+)\s+(\d+)$/i, async (msg, match) => {
  const chatId = msg.chat.id; const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');
  const key = match[1].trim(); const index = Math.max(1, parseInt(match[2], 10)) - 1;
  const meta = getMsgMeta();
  if (!meta.saved_texts[key] || index < 0 || index >= meta.saved_texts[key].length) return safeSendMessage(chatId, `Invalid key/index. Use /msgls to check items.`);
  pendingTextOps[chatId] = { mode: 'set', key, index };
  return safeSendMessage(chatId, `Send the <b>HTML-formatted</b> message to replace item #${index+1} in <code>${escapeHtml(key)}</code>.`, { parse_mode: 'HTML' });
});

// /msgremove <key> [index|all]
bot.onText(/^\/msgremove(?:@\w+)?\s+(\S+)(?:\s+(\S+))?$/i, async (msg, match) => {
  const chatId = msg.chat.id; const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');
  const key = match[1].trim();
  const which = (match[2] || '').trim().toLowerCase();

  const meta = getMsgMeta();
  if (!meta.saved_texts[key]) return safeSendMessage(chatId, `Key <code>${escapeHtml(key)}</code> not found.`, { parse_mode: 'HTML' });

  if (!which || which === 'all') {
    // delete all and remove tokens for that key
    for (let i = 0; i < (meta.saved_texts[key] || []).length; i++) removeMsgTokensForKeyIndex(key, i);
    delete meta.saved_texts[key];
    saveMsgMeta(meta);
    function markMsgKeysDirty() {
    const meta = getMsgMeta();
    if (!meta) return;

    meta.msg_keys_telegraph = meta.msg_keys_telegraph || {};
    // drop cached index and per-key pages so they’ll be rebuilt next time
    meta.msg_keys_telegraph.indexUrl = null;
    meta.msg_keys_telegraph.key_pages = {};
    meta.msg_keys_telegraph.updated_at = new Date().toISOString();

    saveMsgMeta(meta);
  }
    markMsgKeysDirty();
    return safeSendMessage(chatId, `Deleted key <code>${escapeHtml(key)}</code>.`, { parse_mode: 'HTML' });
  }

  const idx = parseInt(which, 10);
  if (!Number.isFinite(idx) || idx < 1 || idx > meta.saved_texts[key].length) {
    return safeSendMessage(chatId, `Invalid index. Use /msgremove ${key} <index|all>`);
  }
  const removedIndex = idx - 1;
  // remove tokens referencing that index
  const metaTokens = meta.msg_tokens || {};
  for (const tk of Object.keys(metaTokens)) {
    const v = metaTokens[tk];
    if (v && v.key === key && v.index === removedIndex) delete meta.msg_tokens[tk];
    // if token points to indices after removedIndex, shift them left by 1
    else if (v && v.key === key && v.index > removedIndex) meta.msg_tokens[tk].index = v.index - 1;
  }
  meta.saved_texts[key].splice(removedIndex, 1);
  if (meta.saved_texts[key].length === 0) delete meta.saved_texts[key];
  saveMsgMeta(meta);
  function markMsgKeysDirty() {
  const meta = getMsgMeta();
  if (!meta) return;

  meta.msg_keys_telegraph = meta.msg_keys_telegraph || {};
  // drop cached index and per-key pages so they’ll be rebuilt next time
  meta.msg_keys_telegraph.indexUrl = null;
  meta.msg_keys_telegraph.key_pages = {};
  meta.msg_keys_telegraph.updated_at = new Date().toISOString();

  saveMsgMeta(meta);
}
  return safeSendMessage(chatId, `Removed item #${idx} from <code>${escapeHtml(key)}</code>.`, { parse_mode: 'HTML' });
});

// /msgls  — open message store keys (now Telegraph index for users)
bot.onText(/^\/msgls(?:@\w+)?$/i, async (msg) => {
  const chatId = msg.chat.id;
  const processing = await showProcessing(chatId, '⏳ Building Telegraph index…');
  try {
    const { indexUrl } = await createTelegraphIndexForMsgKeys();
    const kb = { inline_keyboard: [[{ text: '🗂️ Open Messages Index', url: indexUrl }]] };
    await safeSendMessage(chatId, 'Here you go:', { reply_markup: kb });
  } catch (_) {
    await safeSendMessage(chatId, 'Failed to build the public messages index. Please try again.');
  } finally {
    await processing.done();
  }
});

// /getmsg <MSG_TOKEN>
bot.onText(/^\/getmsg(?:@\w+)?\s+(\S+)$/i, async (msg, match) => {
  const chatId = msg.chat.id; const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');
  const token = match[1].trim().toUpperCase();
  const meta = getMsgMeta();
  const mapping = meta.msg_tokens && meta.msg_tokens[token];
  if (!mapping) return safeSendMessage(chatId, `Token not found: ${escapeHtml(token)}`, { parse_mode: 'HTML' });
  const arr = meta.saved_texts && meta.saved_texts[mapping.key];
  if (!arr || !arr[mapping.index]) return safeSendMessage(chatId, 'Message missing (maybe deleted).');
  // send the HTML as-is (admin supplied)
  return safeSendMessage(chatId, arr[mapping.index], { parse_mode: 'HTML' });
});

// ------------ REPLACE resolveBatchById with this improved implementation -------------
async function resolveBatchById(id) {
  if (!id) return null;
  const ident = String(id).trim();

  // 1) Prefer the index.js token -> filename mapping (this is what /listfiles uses)
  try {
    if (typeof readIndex === 'function') {
      const idx = readIndex() || {};
      const tokensMap = idx.tokens || {};
      // direct match (most common)
      if (tokensMap[ident]) {
        const filename = tokensMap[ident];
        const batch = readBatchFile(filename);
        if (batch) return { batch, filename };
      }
      // case-insensitive key match
      const lc = ident.toLowerCase();
      const foundKey = Object.keys(tokensMap || {}).find(k => String(k).toLowerCase() === lc);
      if (foundKey) {
        const filename = tokensMap[foundKey];
        const batch = readBatchFile(filename);
        if (batch) return { batch, filename };
      }
    }
  } catch (e) {
    // don't fail hard on index read errors
    console.warn('resolveBatchById: index lookup failed', e && e.message);
  }

  // 2) Try treating the identifier as a filename directly
  try {
    const maybe = readBatchFile(ident);
    if (maybe) return { batch: maybe, filename: ident };
  } catch (e) { /* ignore */ }

  // 3) Finally, fall back to legacy meta.* maps (preserves previous behavior)
  try {
    const meta = (typeof readMeta === 'function') ? (readMeta() || {}) : {};
    const maps = [meta.batches, meta.batchIndex, meta.token_map, meta.tokens, meta.byToken];
    for (const m of maps) {
      if (!m || typeof m !== 'object') continue;

      // token -> filename style mapping
      if (m[ident]) {
        const filename = (typeof m[ident] === 'string') ? m[ident] : (m[ident].filename || m[ident].file || null);
        if (filename) {
          try {
            const batch = readBatchFile(filename);
            if (batch) return { batch, filename };
          } catch (_) {}
        }
      }

      // scan values for token/filename fields
      for (const k of Object.keys(m)) {
        const val = m[k];
        if (!val) continue;
        const tokenMatch = (val.token && String(val.token) === String(ident));
        const filenameMatch = (val.filename && String(val.filename) === String(ident));
        const keyMatch = (String(k) === String(ident));
        if (tokenMatch || filenameMatch || keyMatch) {
          const filename = val.filename || k;
          try {
            const batch = readBatchFile(filename);
            if (batch) return { batch, filename };
          } catch (_) {}
        }
      }
    }
  } catch (e) {
    console.warn('resolveBatchById: meta map scan failed', e && e.message);
  }

  return null;
}

function adminOnly(msgChatId, fromId) {
  if (!ADMIN_ID) return false;
  return String(fromId) === String(ADMIN_ID);
}

// ------------ /showposted <token|filename> -------------
bot.onText(/^\/showposted(?:@\w+)?\s+(\S+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (!ADMIN_ID || String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const id = match[1];
  const resolved = await resolveBatchById(id);
  if (!resolved) {
    return safeSendMessage(chatId, `Could not find batch for "${id}". Provide exact filename or token.`);
  }

  const { batch, filename } = resolved;

  // --- Build "first file" style preview (same idea as broadcast) ---
  const firstFile = (batch.files && batch.files[0]) || null;
  const display = batch.display_name || batch.filename || filename || 'Batch';
  const accessLink = BOT_USERNAME
    ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(batch.token)}`
    : null;

  // small HTML escaper in case you don't have one already
  const esc = (s) => String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const captionParts = [];
  captionParts.push(`<b>${esc(display)}</b>`);
  if (firstFile && firstFile.file_name) {
    captionParts.push(`<b>File:</b> ${esc(firstFile.file_name)}`);
  }
  if (batch.token) {
    captionParts.push(`<b>Token:</b> <code>${esc(batch.token)}</code>`);
  }
  if (accessLink) {
    captionParts.push(`<b>Access:</b> <a href="${accessLink}">Open in bot</a>`);
  }
  const captionHtml = captionParts.join('\n');

  // --- Inline keyboard: resend actions ---
  const kb = {
    inline_keyboard: [
      [
        { text: '📣 Resend to Channel', callback_data: `admin_resend_channel_${batch.token}` },
        { text: '👥 Send to Users', callback_data: `admin_send_users_${batch.token}` }
      ],
      [
        { text: '📣+👥 Channel + Users', callback_data: `admin_resend_both_${batch.token}` }
      ]
    ]
  };

  // --- Send first file (photo/video/document) or text-only fallback ---
  try {
    if (firstFile && firstFile.type === 'photo' && firstFile.file_id) {
      await bot.sendPhoto(chatId, firstFile.file_id, {
        caption: captionHtml,
        parse_mode: 'HTML',
        reply_markup: kb
      });
    } else if (firstFile && firstFile.type === 'video' && firstFile.file_id) {
      await bot.sendVideo(chatId, firstFile.file_id, {
        caption: captionHtml,
        parse_mode: 'HTML',
        reply_markup: kb
      });
    } else if (firstFile && firstFile.type === 'document' && firstFile.file_id) {
      await bot.sendDocument(chatId, firstFile.file_id, {
        caption: captionHtml,
        parse_mode: 'HTML',
        reply_markup: kb
      });
    } else {
      // no usable file -> send caption only
      await safeSendMessage(chatId, captionHtml, {
        parse_mode: 'HTML',
        reply_markup: kb
      });
    }
  } catch (e) {
    console.error('/showposted preview send failed', e && (e.stack || e.message));
    await safeSendMessage(chatId, 'Failed to send preview: ' + (e && e.message ? e.message : String(e)));
  }
});

// ------------ /resend <token|filename> [broadcast] -------------
bot.onText(/^\/resend(?:@\w+)?\s+(\S+)(?:\s+(broadcast))?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (!adminOnly(chatId, fromId)) return safeSendMessage(chatId, 'Admin only.');

  const id = match[1];
  const doBroadcast = !!match[2];

  const resolved = await resolveBatchById(id);
  if (!resolved) return safeSendMessage(chatId, `Could not find batch for "${id}". Provide exact filename or token.`);

  const { batch, filename } = resolved;

  // Post to channel (use your existing updateOrPostBatchToChannel if present)
  try {
    if (typeof updateOrPostBatchToChannel === 'function') {
      const channelMsg = await updateOrPostBatchToChannel(batch, filename, { force: true });
      // channelMsg may contain chat_id and message_id or other useful info
      await safeSendMessage(chatId, `Posted to channel. ${channelMsg && channelMsg.message_id ? `message_id=${channelMsg.message_id}` : ''}`);
    } else if (typeof postBatchToChannel === 'function') {
      await postBatchToChannel(batch); // fallback name
      await safeSendMessage(chatId, 'Posted to channel (used fallback postBatchToChannel).');
    } else {
      return safeSendMessage(chatId, 'No channel-post function found (expected updateOrPostBatchToChannel). Please add it or tell me how you post batches to channel.');
    }
  } catch (e) {
    console.error('/resend channel post failed', e && (e.stack || e.message));
    await safeSendMessage(chatId, 'Failed to post to channel: ' + (e && e.message ? e.message : String(e)));
  }

  // Optionally broadcast to users
  if (doBroadcast) {
    try {
      if (typeof broadcastNewBatchToAllUsers === 'function') {
        await broadcastNewBatchToAllUsers(batch); // keep existing behavior
        await safeSendMessage(chatId, 'Broadcast started to users.');
      } else if (typeof broadcastBatch === 'function') {
        await broadcastBatch(batch);
        await safeSendMessage(chatId, 'Broadcast started (used fallback broadcastBatch).');
      } else {
        await safeSendMessage(chatId, 'No broadcast function found (expected broadcastNewBatchToAllUsers).');
      }
    } catch (e) {
      console.error('/resend broadcast failed', e && (e.stack || e.message));
      await safeSendMessage(chatId, 'Broadcast failed: ' + (e && e.message ? e.message : String(e)));
    }
  }
});

// ---------- Helper: build chmenu message (text + keyboard) ----------
function buildChmenuMessage() {
  const meta = getMsgMeta() || {};
  const items = meta.channel_forwards || [];
  if (!items.length) {
    return {
      text: 'No channel messages configured yet.',
      reply_markup: null
    };
  }

  // Build text listing (HTML)
  let out = '<b>Channel messages (all entries)</b>\n\n';
  for (let i = 0; i < items.length; i++) {
    const it = items[i] || {};
    const status = it.hidden ? '🔒 Hidden' : '✅ Visible';
    const link = getChannelStartLinkForIndex(i);
    // include replacedAt note if any
    const repl = it.replacedAt ? ` — replaced ${it.replacedAt.split('T')[0]}` : '';
    out += `#${i + 1} — ${status}${repl} — <a href="${link}">Open</a>\n`;
  }

  // Build inline keyboard: one row per item (Open URL + Toggle + Replace)
  const kb = { inline_keyboard: [] };
  for (let i = 0; i < items.length; i++) {
    const it = items[i] || {};
    const url = getChannelStartLinkForIndex(i);
    const toggleText = it.hidden ? 'Unhide' : 'Hide';
    kb.inline_keyboard.push([
      { text: `Open #${i + 1}`, url },
      { text: toggleText, callback_data: `ch_toggle|${i}` },
      { text: 'Replace', callback_data: `ch_request_replace|${i}` }
    ]);
  }

  // Add a final row with Quick actions
  kb.inline_keyboard.push([
    { text: 'Show /chls (visible only)', callback_data: 'ch_show_list' },
    { text: 'Refresh', callback_data: 'ch_menu_refresh' }
  ]);

  return { text: out, reply_markup: kb };
}

// ---------- /chmenu (admin only) ----------
bot.onText(/^\/chmenu(?:@\w+)?$/i, async (msg) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const m = buildChmenuMessage();
  if (!m.reply_markup) {
    return safeSendMessage(chatId, m.text);
  }

  return safeSendMessage(chatId, m.text, { parse_mode: 'HTML', reply_markup: m.reply_markup, disable_web_page_preview: true });
});

// ---------- callback handler: toggle hidden/unhidden (must be inside your callback_query handler) ----------
bot.on('callback_query', async (q) => {
  try {
    const data = q.data || '';
    // existing callback handlers go above this block in your file;
    // place this toggle block before generic fallthrough callbacks if possible.

    // Toggle hide/unhide
    if (data.startsWith('ch_toggle|')) {
      const fromId = q.from && q.from.id;
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
        await bot.answerCallbackQuery(q.id, { text: 'Admin only.', show_alert: true }).catch(()=>{});
        return;
      }

      const parts = data.split('|');
      const idx = Number(parts[1]);
      const meta = getMsgMeta() || {};
      meta.channel_forwards = meta.channel_forwards || [];

      if (!Number.isFinite(idx) || idx < 0 || idx >= meta.channel_forwards.length) {
        await bot.answerCallbackQuery(q.id, { text: 'Index out of range.' }).catch(()=>{});
        return;
      }

      meta.channel_forwards[idx].hidden = !meta.channel_forwards[idx].hidden;
      saveMsgMeta(meta);

      const statusText = meta.channel_forwards[idx].hidden ? 'Hidden' : 'Visible';
      await bot.answerCallbackQuery(q.id, { text: `Item #${idx + 1} is now ${statusText}.` }).catch(()=>{});

      // Update the original menu message in-place (if possible)
      try {
        const chatId = q.message.chat.id;
        const messageId = q.message.message_id;
        const m = buildChmenuMessage();
        await bot.editMessageText(m.text, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'HTML',
          reply_markup: m.reply_markup,
          disable_web_page_preview: true
        });
      } catch (e) {
        // If edit fails (permissions etc), fallback: send a confirmation message
        await safeSendMessage(q.message.chat.id, `Updated item #${idx + 1} — ${statusText}`);
      }
      return;
    }

    // inside your existing bot.on('callback_query', ...) handler, add:
    if (data && data.startsWith('ch_request_replace|')) {
      const fromId = q.from && q.from.id;
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
        await bot.answerCallbackQuery(q.id, { text: 'Admin only.', show_alert: true }).catch(()=>{});
        return;
      }

      const parts = data.split('|');
      const idx = Number(parts[1]);
      const meta = getMsgMeta() || {};
      if (!Number.isFinite(idx) || idx < 0 || idx >= (meta.channel_forwards || []).length) {
        await bot.answerCallbackQuery(q.id, { text: 'Index out of range.' }).catch(()=>{});
        return;
      }

      // Prepare help text for the admin with examples
      const sampleLink = getChannelStartLinkForIndex(idx);
      const helpText = 
        `Replace item #${idx + 1}\n\n` +
        `You can either:\n` +
        `1) Reply to a forwarded channel message (in this chat) and run:\n` +
        `/chrpls ${idx + 1}\n\n` +
        `2) Or pass a t.me link directly:\n` +
        `/chrpls ${idx + 1} https://t.me/c/123456789/55\n` +
        `or\n` +
        `/chrpls ${idx + 1} https://t.me/ChannelUsername/12\n\n` +
        `Shareable deep-link for this item: ${sampleLink}\n\n` +
        `Note: replacing updates source_chat_id/source_message_id (if the link was parseable).`;

      // Send ephemeral alert and also send a message in chat with instructions
      await bot.answerCallbackQuery(q.id, { text: 'See replacement instructions (sent).' }).catch(()=>{});
      try {
        const chatId = q.message.chat.id;
        await safeSendMessage(chatId, helpText, { parse_mode: 'HTML' });
      } catch (e) {
        // fallback to answerCallbackQuery only
      }
      return;
}

    // Refresh the menu
    if (data === 'ch_menu_refresh') {
      const fromId = q.from && q.from.id;
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
        await bot.answerCallbackQuery(q.id, { text: 'Admin only.' }).catch(()=>{});
        return;
      }
      try {
        const chatId = q.message.chat.id;
        const messageId = q.message.message_id;
        const m = buildChmenuMessage();
        await bot.editMessageText(m.text, {
          chat_id: chatId,
          message_id: messageId,
          parse_mode: 'HTML',
          reply_markup: m.reply_markup,
          disable_web_page_preview: true
        });
        await bot.answerCallbackQuery(q.id, { text: 'Refreshed.' }).catch(()=>{});
      } catch (e) {
        await bot.answerCallbackQuery(q.id, { text: 'Could not refresh.' }).catch(()=>{});
      }
      return;
    }

    // Shortcut to show /chls (visible only)
    if (data === 'ch_show_list') {
      const fromId = q.from && q.from.id;
      if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
        await bot.answerCallbackQuery(q.id, { text: 'Admin only.' }).catch(()=>{});
        return;
      }
      // call your existing /chls behaviour if available; otherwise replicate visible-only keyboard
      try {
        const chatId = q.message.chat.id;
        // Reuse the /chls handler logic: send visible-only keyboard
        // (If you created sendChannelListVisible(chatId) helper earlier, call it; otherwise replicate)
        const meta = getMsgMeta();
        const items = meta.channel_forwards || [];
        const visibleIndices = [];
        for (let i = 0; i < items.length; i++) if (!items[i].hidden) visibleIndices.push(i);
        if (!visibleIndices.length) {
          await bot.answerCallbackQuery(q.id, { text: 'No visible items.' }).catch(()=>{});
          return;
        }
        const kb = { inline_keyboard: [] };
        const perRow = 3;
        for (let i = 0; i < visibleIndices.length; i += perRow) {
          const row = [];
          for (let j = i; j < Math.min(i + perRow, visibleIndices.length); j++) {
            const origIdx = visibleIndices[j];
            const url = getChannelStartLinkForIndex(origIdx);
            row.push({ text: `Open #${origIdx + 1}`, url });
          }
          kb.inline_keyboard.push(row);
        }
        await bot.sendMessage(chatId, `Channel messages (${visibleIndices.length} visible of ${items.length}). Tap to open:`, { reply_markup: kb });
        await bot.answerCallbackQuery(q.id, { text: 'Sent visible list.' }).catch(()=>{});
      } catch (e) {
        await bot.answerCallbackQuery(q.id, { text: 'Failed.' }).catch(()=>{});
      }
      return;
    }

  } catch (err) {
    console.error('chmenu callback error', err && err.stack ? err.stack : err);
  }

  // allow other callback handlers below...
});

// /chadd — start collecting private channel message links (admin only)
bot.onText(/^\/chadd(?:@\w+)?$/i, async (msg) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  pendingChannelLinks[chatId] = { mode: 'collect', added: 0 };

  const exampleLink = getChannelStartLinkForIndex(1);
  const t =
    '🔗 <b>Channel link collector</b>\n\n' +
    'Now send me one or more message links from your <b>private channel</b>.\n' +
    'Format (example):\n' +
    '<code>https://t.me/c/123456789/55</code>\n\n' +
    'You can send multiple links in one message.\n' +
    'When you are done, send <code>/chdone</code>.\n\n' +
    '<b>Example embed (after setup):</b>\n' +
    `<a href="${exampleLink}">Open channel message #1</a>\n\n` +
    'You can paste such an <code>&lt;a href="...">&lt;/a></code> link inside any saved HTML message to let users open the message in their private chat with the bot.';

  return safeSendMessage(chatId, t, { parse_mode: 'HTML' });
});

// /chdone — stop collecting links (updated to show deep-links)
bot.onText(/^\/chdone(?:@\w+)?$/i, async (msg) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const state = pendingChannelLinks[chatId];
  const count = state && state.added ? state.added : 0;
  // read meta BEFORE deleting state so we can compute indices
  const meta = getMsgMeta();
  const items = meta.channel_forwards || [];
  const total = items.length;

  // compute the indices that were added in this session (last `count` entries)
  const startIndex = Math.max(0, total - count);
  const addedLinks = [];
  for (let i = startIndex; i < total; i++) {
    addedLinks.push({
      index: i,
      url: getChannelStartLinkForIndex(i)
    });
  }

  delete pendingChannelLinks[chatId];

  // first, confirmation message (keeps your existing phrasing)
  await safeSendMessage(
    chatId,
    `✅ Stopped collecting channel links.\nAdded in this session: <b>${count}</b> item(s).`,
    { parse_mode: 'HTML' }
  );

  // if we have new links, show them (HTML list)
  if (addedLinks.length) {
    let out = '<b>Deep-links for items added in this session</b>\n\n';
    for (const it of addedLinks) {
      out += `#${it.index + 1} — <a href="${it.url}">Open (bot)</a>\n`;
    }
    await safeSendMessage(chatId, out, { parse_mode: 'HTML', disable_web_page_preview: true });
  }

  return;
});

// /chls — browse saved channel messages (only shows non-hidden entries)
bot.onText(/^\/chls(?:@\w+)?$/i, async (msg) => {
  const chatId = msg.chat.id;
  const meta = getMsgMeta();
  const items = meta.channel_forwards || [];
  if (!items.length) {
    return safeSendMessage(chatId, 'No channel messages configured yet.');
  }

  // Build list of original indices that are visible (not hidden)
  const visibleIndices = [];
  for (let i = 0; i < items.length; i++) {
    if (!items[i] || items[i].hidden) continue;
    visibleIndices.push(i);
  }

  if (!visibleIndices.length) {
    return safeSendMessage(chatId, `No visible channel messages (0 of ${items.length} visible).`);
  }

  // Build keyboard rows (3 per row) using the original index in the deep link
  const kb = { inline_keyboard: [] };
  const perRow = 3;
  for (let i = 0; i < visibleIndices.length; i += perRow) {
    const row = [];
    for (let j = i; j < Math.min(i + perRow, visibleIndices.length); j++) {
      const origIdx = visibleIndices[j];
      const url = getChannelStartLinkForIndex(origIdx); // uses original index
      row.push({ text: `Open #${origIdx + 1}`, url });
    }
    kb.inline_keyboard.push(row);
  }

  if (items.length > visibleIndices.length) {
    kb.inline_keyboard.push([{ text: `…showing ${visibleIndices.length} of ${items.length}`, callback_data: 'chmsg_more' }]);
  }

  await safeSendMessage(chatId, `Channel messages (${visibleIndices.length} visible of ${items.length}). Tap to open:`, { reply_markup: kb });

  // open the first visible page (preserve previous UX)
  return sendChannelForwardByIndex(chatId, visibleIndices[0]);
});

// /chhide <n> — hide item n (1-based index) from /chls (admin only)
bot.onText(/^\/chhide(?:@\w+)?\s+(\d+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');

  const idx = Number(match[1]) - 1;
  const meta = getMsgMeta();
  meta.channel_forwards = meta.channel_forwards || [];
  if (idx < 0 || idx >= meta.channel_forwards.length) {
    return safeSendMessage(chatId, 'Index out of range.');
  }
  meta.channel_forwards[idx].hidden = true;
  saveMsgMeta(meta);
  return safeSendMessage(chatId, `🔒 Hidden CHMSG_${idx} (item #${idx + 1}).`, { parse_mode: 'HTML' });
});

// /chunhide <n> — unhide item n (1-based index) (admin only)
bot.onText(/^\/chunhide(?:@\w+)?\s+(\d+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return safeSendMessage(chatId, 'Admin only.');

  const idx = Number(match[1]) - 1;
  const meta = getMsgMeta();
  meta.channel_forwards = meta.channel_forwards || [];
  if (idx < 0 || idx >= meta.channel_forwards.length) {
    return safeSendMessage(chatId, 'Index out of range.');
  }
  meta.channel_forwards[idx].hidden = false;
  saveMsgMeta(meta);
  return safeSendMessage(chatId, `🔓 Unhidden CHMSG_${idx} (item #${idx + 1}).`, { parse_mode: 'HTML' });
});

// /chrpls <n> <link>  OR reply to a forwarded channel message with /chrpls <n>
// Replace stored channel entry at 1-based index <n> with the provided link (or reply target).
bot.onText(/^\/chrpls(?:@\w+)?\s+(\d+)(?:\s+(.+))?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const fromId = msg.from && msg.from.id;
  if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) {
    return safeSendMessage(chatId, 'Admin only.');
  }

  const idx1 = Number(match[1]);
  const idx = idx1 - 1; // convert to 0-based
  if (!Number.isFinite(idx) || idx < 0) return safeSendMessage(chatId, 'Invalid index.');

  const meta = getMsgMeta() || {};
  meta.channel_forwards = meta.channel_forwards || [];
  if (idx < 0 || idx >= meta.channel_forwards.length) {
    return safeSendMessage(chatId, 'Index out of range.');
  }

  // Determine replacement source_chat_id & source_message_id & link
  let newSourceChat = null;
  let newSourceMsgId = null;
  let newLinkRaw = null;

  // 1) If user replied to a forwarded channel message, try to extract forward info
  if (msg.reply_to_message && (msg.reply_to_message.forward_from_chat || msg.reply_to_message.forward_from)) {
    const f = msg.reply_to_message.forward_from_chat || msg.reply_to_message.forward_from;
    // forward_from_chat.id exists for channels, or forward_from.username for public channels
    newSourceChat = f.id ? f.id : (f.username ? ('@' + f.username) : null);
    newSourceMsgId = msg.reply_to_message.forward_from_message_id || msg.reply_to_message.message_id;
    newLinkRaw = `(replaced from forwarded message)`;
  }

  // 2) Else if a link was provided as argument, parse it
  const argLink = (match[2] || '').trim();
  if (!newSourceChat && argLink) {
    // Try to parse forms:
    // https://t.me/c/<numericId>/<msgId>
    // https://t.me/<username>/<msgId>
    // t.me/c/<numericId>/<msgId>
    // @username/<msgId>
    const m1 = argLink.match(/t\.me\/c\/(\d+)\/(\d+)/i) || argLink.match(/\/c\/(\d+)\/(\d+)/i);
    const m2 = argLink.match(/t\.me\/@?([\w_]+)\/(\d+)/i) || argLink.match(/@?([\w_]+)\/(\d+)/i);
    if (m1) {
      // numeric channel id (t.me/c/12345/678)
      // build internal chat id by prefixing -100
      const rawNum = m1[1];
      newSourceChat = Number('-100' + rawNum); // e.g. -100123456789
      newSourceMsgId = Number(m1[2]);
      newLinkRaw = `https://t.me/c/${rawNum}/${newSourceMsgId}`;
    } else if (m2) {
      const uname = m2[1];
      newSourceChat = '@' + uname;
      newSourceMsgId = Number(m2[2]);
      newLinkRaw = `https://t.me/${uname}/${newSourceMsgId}`;
    } else {
      // fallback: treat argument as a raw link string and store it, but warn admin that copy may fail unless it's a recognized t.me link or reply
      newLinkRaw = argLink;
    }
  }

  // If still nothing, tell admin how to use the command.
  if (!newSourceChat && !newLinkRaw) {
    const usage = 'Usage:\n' +
      '/chrpls <n> <t.me link>  — replace item #n with the given link\n' +
      'OR reply to a forwarded channel message with: /chrpls <n>\n\n' +
      'Examples:\n/chrpls 3 https://t.me/c/123456789/55\n/chrpls 2 https://t.me/ChannelUsername/12\n(Or reply to a channel message and run /chrpls 4)';
    return safeSendMessage(chatId, usage);
  }

  // Save previous state as history
  const prev = Object.assign({}, meta.channel_forwards[idx]);

  // Update entry (preserve allowed fields)
  const newEntry = Object.assign({}, prev);
  if (newSourceChat) newEntry.source_chat_id = newSourceChat;
  if (newSourceMsgId) newEntry.source_message_id = newSourceMsgId;
  if (newLinkRaw) newEntry.link = newLinkRaw;

  // keep a small history
  newEntry.prevLink = prev.link || null;
  newEntry.replacedAt = new Date().toISOString();
  newEntry.replacedBy = msg.from && (msg.from.username ? ('@' + msg.from.username) : (msg.from.first_name || String(msg.from.id)));

  // replace in meta
  meta.channel_forwards[idx] = newEntry;
  saveMsgMeta(meta);

  await safeSendMessage(chatId, `Replaced item #${idx + 1}.\nPrevious link: ${newEntry.prevLink || '(none)'}\nNew link: ${newEntry.link || '(provided as raw string)'}`);
});

// -------------- capture channel links for /chadd --------------
bot.on('message', async (msg) => {
  try {
    if (!msg || !msg.chat || !msg.from) return;

    const chatId = msg.chat.id;
    const fromId = msg.from.id;

    // admin only
    if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return;

    const state = pendingChannelLinks[chatId];
    if (!state || state.mode !== 'collect') return;

    const text = msg.text || '';
    // ignore commands while collecting
    if (!text || text.startsWith('/')) return;

    // Accept both with and without protocol
    const links = [];
    const re = /(?:https?:\/\/)?t\.me\/c\/(\d+)\/(\d+)/gi;
    let m;
    while ((m = re.exec(text)) !== null) {
      links.push({ raw: m[0], channelPart: m[1], msgId: m[2] });
    }

    if (!links.length) {
      await safeSendMessage(chatId, 'No valid t.me/c/... links found in that message.');
      return;
    }

    const meta = getMsgMeta();
    meta.channel_forwards = meta.channel_forwards || [];

    let added = 0;
    for (const link of links) {
      const channelPart = String(link.channelPart);
      const msgId = Number(link.msgId);
      if (!Number.isFinite(msgId)) continue;

      // For t.me/c/123456789/55 → internal chat_id is -100123456789
      const chatIdStr = `-100${channelPart}`;
      const source_chat_id = Number(chatIdStr);

      if (!Number.isFinite(source_chat_id)) continue;

      meta.channel_forwards.push({
        source_chat_id,
        source_message_id: msgId,
        link: link.raw,
        hidden: false,                // visible by default
        addedAt: new Date().toISOString()
      });
      added++;
    }

    saveMsgMeta(meta);
    state.added = (state.added || 0) + added;

    if (added) {
      await safeSendMessage(
        chatId,
        `✅ Added <b>${added}</b> message link(s).\nTotal in this session: <b>${state.added}</b>.`,
        { parse_mode: 'HTML' }
      );
    } else {
      await safeSendMessage(chatId, 'No usable channel links were added from that message.');
    }
  } catch (e) {
    console.error('channel links collector error', e && (e.stack || e.message || e));
  }
});

// -------------- capture the next message (store/addto/set) --------------
bot.on('message', async (msg) => {
  try {
    const chatId = msg.chat && msg.chat.id;
    const fromId = msg.from && msg.from.id;
    const text = msg.text || '';
    if (!chatId || !fromId) return;
    if (ADMIN_ID && String(fromId) !== String(ADMIN_ID)) return; // only admin can store

    const pending = pendingTextOps[chatId];
    if (!pending) return;
    // Do not trigger on commands
    if (text && text.startsWith('/')) return;

    // ensure meta structure
    const meta = getMsgMeta();
    meta.saved_texts = meta.saved_texts || {};
    meta.msg_tokens = meta.msg_tokens || {};

    // STORE (set item #1)
    if (pending.mode === 'store') {
      const key = pending.key;
      meta.saved_texts[key] = Array.isArray(meta.saved_texts[key]) ? meta.saved_texts[key] : [];
      if (meta.saved_texts[key].length === 0) meta.saved_texts[key].push(String(text));
      else meta.saved_texts[key][0] = String(text); // replace first
      saveMsgMeta(meta);
      markMsgKeysDirty();

      // create token for that item (message token)
      const token = makeMsgToken();
      registerMsgToken(token, key, 0);

      // create a batch for this stored message and register batch token
      const batchRes = await createBatchForStoredMessage(key, 0, text, fromId);

      delete pendingTextOps[chatId];
      await safeSendMessage(chatId,
        `✅ Stored under <code>${escapeHtml(key)}</code> as item #1.\nMessage token: <code>${token}</code>` +
        (batchRes && batchRes.token ? `\nBatch token: <code>${batchRes.token}</code>` : ''),
        { parse_mode: 'HTML' }
      );
      return;
    }

    // ADDTO (append)
    if (pending.mode === 'addto') {
      const key = pending.key;
      meta.saved_texts[key] = Array.isArray(meta.saved_texts[key]) ? meta.saved_texts[key] : [];
      meta.saved_texts[key].push(String(text));
      const idx = meta.saved_texts[key].length - 1;

      // create token for added item
      const token = makeMsgToken();
      registerMsgToken(token, key, idx);

      saveMsgMeta(meta);
      markMsgKeysDirty();

      // create a batch for this appended message
      const batchRes = await createBatchForStoredMessage(key, idx, text, fromId);

      delete pendingTextOps[chatId];
      await safeSendMessage(chatId,
        `➕ Appended to <code>${escapeHtml(key)}</code> as item #${idx+1}.\nMessage token: <code>${token}</code>` +
        (batchRes && batchRes.token ? `\nBatch token: <code>${batchRes.token}</code>` : ''),
        { parse_mode: 'HTML' }
      );
      return;
    }

    // SET (replace a specific index)
    if (pending.mode === 'set') {
      const key = pending.key;
      const idx = Number(pending.index || 0);
      if (!meta.saved_texts[key] || idx < 0 || idx >= meta.saved_texts[key].length) {
        delete pendingTextOps[chatId];
        return safeSendMessage(chatId, `Index out of range for <code>${escapeHtml(key)}</code>.`, { parse_mode: 'HTML' });
      }

      meta.saved_texts[key][idx] = String(text);

      // remove any message-tokens pointing to this item (they become stale)
      for (const tk of Object.keys(meta.msg_tokens || {})) {
        const v = meta.msg_tokens[tk];
        if (v && v.key === key && v.index === idx) delete meta.msg_tokens[tk];
      }

      // create a fresh message token for the replaced item
      const token = makeMsgToken();
      registerMsgToken(token, key, idx);

      saveMsgMeta(meta);
      markMsgKeysDirty();

      // create a batch for the updated message
      const batchRes = await createBatchForStoredMessage(key, idx, text, fromId);

      delete pendingTextOps[chatId];
      await safeSendMessage(chatId,
        `✏️ Updated <code>${escapeHtml(key)}</code> item #${idx+1}.\nNew message token: <code>${token}</code>` +
        (batchRes && batchRes.token ? `\nBatch token: <code>${batchRes.token}</code>` : ''),
        { parse_mode: 'HTML' }
      );
      return;
    }

    // support the 'await_store_key' and 'await_addto_key' flows (if you integrated buttons)
    if (pending.mode === 'await_store_key') {
      const key = text.trim().split(/\s+/)[0];
      pendingTextOps[chatId] = { mode: 'store', key };
      return safeSendMessage(chatId, `Key set to <code>${escapeHtml(key)}</code>. Now send the HTML message to store as item #1.`, { parse_mode: 'HTML' });
    }
    if (pending.mode === 'await_addto_key') {
      const key = text.trim().split(/\s+/)[0];
      pendingTextOps[chatId] = { mode: 'addto', key };
      return safeSendMessage(chatId, `Key set to <code>${escapeHtml(key)}</code>. Now send the HTML message to append.`, { parse_mode: 'HTML' });
    }

    // Handle key prompt flows (duplicate safe-guards kept for compatibility)
    if (fromId === ADMIN_ID && pendingTextOps[chatId]?.mode === 'await_store_key' && text && !text.startsWith('/')) {
      const key = text.trim().split(/\s+/)[0];
      pendingTextOps[chatId] = { mode: 'store', key };
      await safeSendMessage(chatId,
        `Key set to <code>${escapeHtml(key)}</code>.\nNow send the <b>HTML-formatted</b> message to store as item #1.`,
        { parse_mode: 'HTML' }
      );
      return;
    }

    if (fromId === ADMIN_ID && pendingTextOps[chatId]?.mode === 'await_addto_key' && text && !text.startsWith('/')) {
      const key = text.trim().split(/\s+/)[0];
      pendingTextOps[chatId] = { mode: 'addto', key };
      await safeSendMessage(chatId,
        `Key set to <code>${escapeHtml(key)}</code>.\nNow send the <b>HTML-formatted</b> message to append.`,
        { parse_mode: 'HTML' }
      );
      return;
    }

  } catch (e) {
    console.error('msgstore message flow error', e && (e.stack || e.message));
  }
});

// -------------- inline callbacks for listing/menu --------------
bot.on('callback_query', async (q) => {
  try {
    const data = q.data || '';
    const fromId = q.from && q.from.id;
    const chatId = q.message && q.message.chat && q.message.chat.id;
    const isAdmin = (ADMIN_ID && String(fromId) === String(ADMIN_ID));

    // back to keys list
    if (data === 'msg_back') {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      const view = buildMsgKeysKeyboard(isAdmin);
      try {
        await bot.editMessageText(view.text, {
          chat_id: chatId,
          message_id: q.message.message_id,
          parse_mode: view.parse_mode,
          reply_markup: view.reply_markup
        });
      } catch (e) {
        await safeSendMessage(chatId, view.text, { parse_mode: view.parse_mode, reply_markup: view.reply_markup });
      }
      return;
    }

    // open key -> show preview UI, build Telegraph page with a temporary "processing..." message
    // open key -> show preview UI, and handle telegraph-per-key logic (admin can generate page; users get cached page)
    if (data.startsWith('msg_key_open|')) {
      try {
        await safeAnswerCallbackQuery(q.id).catch(()=>{});
        const chatId = q.message.chat.id;
        // safer key extraction for callbacks like "msg_key_open|<encoded key>"
        const parts = data.split('|');
        const keyEncoded = parts.slice(1).join('|'); // join in case key had '|' before encoding
        const key = decodeURIComponent(keyEncoded || '');
        const asAdmin = isAdmin;

        // show preview view first (same code as before)
        const view = buildMsgKeyView(key, asAdmin);
        try {
          await bot.editMessageText(view.text, { chat_id: chatId, message_id: q.message.message_id, parse_mode: view.parse_mode, reply_markup: view.reply_markup });
        } catch (e) {
          await safeSendMessage(chatId, view.text, { parse_mode: view.parse_mode, reply_markup: view.reply_markup });
        }

        // Now: for admin, optionally create/cached telegraph page on-demand
        const meta = getMsgMeta();
        meta.msg_keys_telegraph = meta.msg_keys_telegraph || { indexUrl: null, key_pages: {}, updated_at: null };

        // If user pressed to open the telegraph page (via another button), they will hit separate callback; here we simply ensure admin can create
        if (asAdmin) {
          // create per-key telegraph page if not present (force create)
          let url = meta.msg_keys_telegraph.key_pages && meta.msg_keys_telegraph.key_pages[key];
          if (!url) {
            // create page and cache it
            try {
              url = await createTelegraphPageForMsgKey(key, { force: true });
              // notify admin
              await safeSendMessage(chatId, `Telegraph page created for <code>${escapeHtml(key)}</code>:\n${url}`, { parse_mode: 'HTML' });
            } catch (e) {
              console.warn('admin create page error', e && e.message);
              await safeSendMessage(chatId, `Failed to create Telegraph page for key: ${escapeHtml(key)}`);
            }
          } else {
            await safeSendMessage(chatId, `Telegraph page (cached) for <code>${escapeHtml(key)}</code>:\n${url}`, { parse_mode: 'HTML' });
          }
        } else {
          // for users: if page cached, show URL; else ask them to ask admin to rebuild
          const url = meta.msg_keys_telegraph.key_pages && meta.msg_keys_telegraph.key_pages[key];
          if (url) {
            const kb = { inline_keyboard: [[{ text: 'Open key page', url }]] };
            await safeSendMessage(chatId, 'Open the public page for this key:', { reply_markup: kb });
          } else {
            await safeSendMessage(chatId, 'This key is not published yet. Please ask an admin to rebuild/publish the public messages index.');
          }
        }
      } catch (e) {
        console.error('msg_key_open error', e && e.stack || e);
      }
      return;
    }

    // view item content (send final stored HTML/text)
    if (data.startsWith('msg_item_view|')) {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      const parts = data.split('|'); // msg_item_view|key|index
      const key = parts[1]; const idx = Number(parts[2] || 0);
      const meta = getMsgMeta();
      const arr = meta.saved_texts && meta.saved_texts[key] ? meta.saved_texts[key] : [];
      if (!arr[idx]) { await safeAnswerCallbackQuery(q.id, { text: 'Item missing' }).catch(()=>{}); return; }
      // increment view count for any registered message tokens that point to this key/index
      try {
        const metaTokens = meta.msg_tokens || {};
        for (const tk of Object.keys(metaTokens)) {
          const v = metaTokens[tk];
          if (v && v.key === key && Number(v.index) === Number(idx)) {
            try { incrementMsgView(tk); } catch(e) {}
          }
        }
      } catch(e) {}

      return safeSendMessage(chatId, arr[idx], { parse_mode: 'HTML' });
    }

    // ---------- ADMIN-ONLY enforcement (mutating/token actions) ----------
    const adminOnlyPrefixes = [
      'admin_',             // admin helper buttons
      'msg_item_rm|',       // remove item
      'msg_key_rm|',        // delete key
      'msg_token_create|',  // create token
      'msg_item_add|',      // add-to via inline (should be admin)
      'msg_token_copy|'     // copying a token should be admin-only
    ];

    if (!isAdmin) {
      for (const p of adminOnlyPrefixes) {
        if (data.startsWith(p)) {
          await safeAnswerCallbackQuery(q.id, { text: 'Admin only' }).catch(()=>{});
          return;
        }
      }
    }

    // ---------- Now the admin-only handlers (these run only if isAdmin) ----------
    // remove item via inline
    if (data.startsWith('msg_item_rm|')) {
      const parts = data.split('|'); const key = parts[1]; const idx = Number(parts[2] || 0);
      const meta = getMsgMeta();
      if (!meta.saved_texts[key] || idx < 0 || idx >= meta.saved_texts[key].length) { await safeAnswerCallbackQuery(q.id, { text: 'Item missing' }).catch(()=>{}); return; }
      // delete tokens referencing and shift tokens indices for same key
      for (const tk of Object.keys(meta.msg_tokens || {})) {
        const v = meta.msg_tokens[tk];
        if (v && v.key === key && v.index === idx) delete meta.msg_tokens[tk];
        else if (v && v.key === key && v.index > idx) meta.msg_tokens[tk].index = v.index - 1;
      }
      meta.saved_texts[key].splice(idx, 1);
      if (meta.saved_texts[key].length === 0) delete meta.saved_texts[key];
      saveMsgMeta(meta);
      // refresh view (if key still exists)
      if (meta.saved_texts[key]) {
        const view = buildMsgKeyView(key, isAdmin);
        try { await bot.editMessageText(view.text, { chat_id: chatId, message_id: q.message.message_id, parse_mode: view.parse_mode, reply_markup: view.reply_markup }); } catch(_) {}
      } else {
        const view = buildMsgKeysKeyboard(isAdmin);
        try { await bot.editMessageText(view.text, { chat_id: chatId, message_id: q.message.message_id, parse_mode: view.parse_mode, reply_markup: view.reply_markup }); } catch(_) {}
      }
      await safeAnswerCallbackQuery(q.id, { text: 'Removed' }).catch(()=>{});
      return;
    }

    // delete entire key
    if (data.startsWith('msg_key_rm|')) {
      const key = data.split('|')[1];
      const meta = getMsgMeta();
      if (!meta.saved_texts[key]) { await safeAnswerCallbackQuery(q.id, { text: 'Key missing' }).catch(()=>{}); return; }
      // remove tokens referencing this key
      for (const tk of Object.keys(meta.msg_tokens || {})) {
        if (meta.msg_tokens[tk] && meta.msg_tokens[tk].key === key) delete meta.msg_tokens[tk];
      }
      delete meta.saved_texts[key];
      saveMsgMeta(meta);
      const view = buildMsgKeysKeyboard(isAdmin);
      try { await bot.editMessageText(view.text, { chat_id: chatId, message_id: q.message.message_id, parse_mode: view.parse_mode, reply_markup: view.reply_markup }); } catch(_) {}
      await safeAnswerCallbackQuery(q.id, { text: 'Key deleted' }).catch(()=>{});
      return;
    }

    // create a message-token for key/index
    if (data.startsWith('msg_token_create|')) {
      const parts = data.split('|'); const key = parts[1]; const idx = Number(parts[2] || 0);
      const meta = getMsgMeta();
      if (!meta.saved_texts[key] || idx < 0 || idx >= meta.saved_texts[key].length) { await safeAnswerCallbackQuery(q.id, { text: 'Item missing' }).catch(()=>{}); return; }
      const token = makeMsgToken();
      registerMsgToken(token, key, idx);
      await safeAnswerCallbackQuery(q.id, { text: `Token created: ${token}` }).catch(()=>{});
      // refresh view
      const view = buildMsgKeyView(key, isAdmin);
      try { await bot.editMessageText(view.text, { chat_id: chatId, message_id: q.message.message_id, parse_mode: view.parse_mode, reply_markup: view.reply_markup }); } catch(_) {}
      return;
    }

    // copy token (admin only) — show token in alert so admin can copy/paste
    if (data.startsWith('msg_token_copy|')) {
      const token = data.split('|')[1];
      await safeAnswerCallbackQuery(q.id, { text: `Token: ${token}` }).catch(()=>{});
      return;
    }

    // add to this key via inline (start awaiting)
    if (data.startsWith('msg_item_add|')) {
      const key = data.split('|')[1];
      pendingTextOps[chatId] = { mode: 'addto', key };
      await safeAnswerCallbackQuery(q.id, { text: 'Send HTML to append' }).catch(()=>{});
      await safeSendMessage(chatId, `Send the <b>HTML-formatted</b> message to append to <code>${escapeHtml(key)}</code>.`, { parse_mode: 'HTML' });
      return;
    }

  } catch (err) {
    console.error('msgstore callback error', err && (err.stack || err.message));
    try { await safeAnswerCallbackQuery(q.id).catch(()=>{}); } catch(_) {}
  }
});

async function createBatchForStoredMessage(key, index, html, adminId) {
  try {
    const safeKey = sanitizeFilenameForDisk ? sanitizeFilenameForDisk(String(key)) : String(key).replace(/[^\w.-]+/g,'_');
    const displayName = `${key} #${index + 1}`;
    const filename = `${safeKey}_${index + 1}`; // internal filename
    const token = generateToken();

    // create and register batch
    createBatchFile(filename, token, adminId);
    const batch = readBatchFile(filename) || { filename, token, files: [] };
    batch.display_name = displayName;
    batch.files = [{ type: 'text', text: html }];
    writeBatchFile(filename, batch);
    registerTokenInIndex(token, filename);

    // tell admin (quietly ignore for users)
    if (adminId) {
      const link = BOT_USERNAME ? `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(token)}` : '';
      await safeSendMessage(adminId,
        `📦 Created batch for <code>${escapeHtml(displayName)}</code>\nToken: <code>${token}</code>${link ? `\nOpen: ${link}` : ''}`,
        { parse_mode: 'HTML' }
      );
    }
    return { token, filename };
  } catch (e) {
    console.warn('createBatchForStoredMessage failed', e && e.message);
    return null;
  }
}

// -------------- inline callbacks Public Messages(/msgls) --------------
bot.on('callback_query', async (q) => {
  const data = q.data;
  if (!data) return;

    // Channel message browser open
    if (data === 'chls') {
      await safeAnswerCallbackQuery(q.id).catch(() => {});
      return sendChannelForwardByIndex(q.message.chat.id, 0);
    }

      // --- Admin resend/send actions from /showposted ---

    // Resend to channel only
    if (data && data.startsWith('admin_resend_channel_')) {
      const token = data.substring('admin_resend_channel_'.length);

      try {
        const resolved = await resolveBatchById(token);
        if (!resolved) {
          await safeAnswerCallbackQuery(q.id, { text: 'Batch not found', show_alert: true }).catch(()=>{});
          return;
        }
        const { batch, filename } = resolved;

        if (typeof updateOrPostBatchToChannel === 'function') {
          await updateOrPostBatchToChannel(batch, filename, { force: true });
          await safeAnswerCallbackQuery(q.id, { text: 'Sent to channel', show_alert: false }).catch(()=>{});
        } else {
          await safeAnswerCallbackQuery(q.id, { text: 'Channel post function missing', show_alert: true }).catch(()=>{});
        }
      } catch (e) {
        console.error('admin_resend_channel error', e && (e.stack || e.message));
        await safeAnswerCallbackQuery(q.id, { text: 'Error sending to channel', show_alert: true }).catch(()=>{});
      }
      return;
    }

    // Send to users only (broadcast)
    if (data && data.startsWith('admin_send_users_')) {
      const token = data.substring('admin_send_users_'.length);

      try {
        const resolved = await resolveBatchById(token);
        if (!resolved) {
          await safeAnswerCallbackQuery(q.id, { text: 'Batch not found', show_alert: true }).catch(()=>{});
          return;
        }
        const { batch } = resolved;

        if (typeof broadcastNewBatchToAllUsers === 'function') {
          await broadcastNewBatchToAllUsers(batch);
          await safeAnswerCallbackQuery(q.id, { text: 'Broadcast started', show_alert: false }).catch(()=>{});
        } else {
          await safeAnswerCallbackQuery(q.id, { text: 'Broadcast function missing', show_alert: true }).catch(()=>{});
        }
      } catch (e) {
        console.error('admin_send_users error', e && (e.stack || e.message));
        await safeAnswerCallbackQuery(q.id, { text: 'Broadcast error', show_alert: true }).catch(()=>{});
      }
      return;
    }

    // Resend to channel AND users
    if (data && data.startsWith('admin_resend_both_')) {
      const token = data.substring('admin_resend_both_'.length);

      try {
        const resolved = await resolveBatchById(token);
        if (!resolved) {
          await safeAnswerCallbackQuery(q.id, { text: 'Batch not found', show_alert: true }).catch(()=>{});
          return;
        }
        const { batch, filename } = resolved;

        if (typeof updateOrPostBatchToChannel === 'function') {
          await updateOrPostBatchToChannel(batch, filename, { force: true });
        }
        if (typeof broadcastNewBatchToAllUsers === 'function') {
          await broadcastNewBatchToAllUsers(batch);
        }

        await safeAnswerCallbackQuery(q.id, { text: 'Sent to channel & users', show_alert: false }).catch(()=>{});
      } catch (e) {
        console.error('admin_resend_both error', e && (e.stack || e.message));
        await safeAnswerCallbackQuery(q.id, { text: 'Error sending', show_alert: true }).catch(()=>{});
      }
      return;
    }

    if (data === 'msgls') {
    await safeAnswerCallbackQuery(q.id).catch(()=>{});
    const chatId = q.message.chat.id;

    // ⏳ show temp "building..." message
    const processing = await showProcessing(chatId, '⏳ Generating Telegraph index…');

      try {
        // if admin, send admin inline message-store UI first (so they keep admin controls)
        try {
          if (typeof ADMIN_ID !== 'undefined' && ADMIN_ID && String(q.from && q.from.id) === String(ADMIN_ID)) {
            // build admin inline menu (reuse existing admin keyboard builder if present)
            const replyMarkup = {
              inline_keyboard: [
                [{ text: '📋 Admin: list keys', callback_data: 'admin_msg_list' }],
                [{ text: '➕ Add message', callback_data: 'admin_msg_add' }],
                [{ text: '🔁 Rebuild index', callback_data: 'msgls_rebuild' }]
              ]
            };
            await safeSendMessage(chatId, 'Admin Message Store — quick actions:', { reply_markup: replyMarkup }).catch(()=>{});
          }
        } catch (_) {}

        // Also provide public Telegraph index (for everyone)
        // Use cache if present; do not force build for normal user call
        const { indexUrl } = await createTelegraphIndexForMsgKeys({ force: false });

        const kb = {
          inline_keyboard: [
            [{ text: '🗂️ Open Messages Index', url: indexUrl }],
            [{ text: '♻️ Rebuild', callback_data: 'msgls_rebuild' }]
          ]
        };

        // send the final result
        await safeSendMessage(chatId, 'Here you go:', { reply_markup: kb });

      } catch (err) {
        console.error('msgls handler failed', err && (err.stack || err.message));
        await safeSendMessage(chatId, 'Could not produce messages index.');
      } finally {
        await processing.done();
      }
      return;
    }

        if (data === 'msgls_rebuild') {
      await safeAnswerCallbackQuery(q.id).catch(()=>{});
      const chatId = q.message.chat.id;
      if (typeof ADMIN_ID !== 'undefined' && ADMIN_ID && String(q.from && q.from.id) !== String(ADMIN_ID)) {
        return safeAnswerCallbackQuery(q.id, { text: 'Admin only' }).catch(()=>{});
      }
      const processing = await showProcessing(chatId, '⏳ Rebuilding Telegraph index…');

      try {
        // Admin requested rebuild — force recreation
        const { indexUrl } = await createTelegraphIndexForMsgKeys({ force: true });
        const kb = { inline_keyboard: [[{ text: '🗂️ Open Messages Index', url: indexUrl }]] };
        await safeSendMessage(chatId, 'Rebuilt. Open the updated index:', { reply_markup: kb });
      } catch (_) {
        await safeSendMessage(chatId, 'Could not rebuild the index.');
      } finally {
        await processing.done();
      }
      return;
    }
});

// ---------- fuzzy helpers ----------
function levenshtein(a,b){ if(!a) return b?b.length:0; if(!b) return a.length; a=a.toLowerCase(); b=b.toLowerCase(); const m=a.length, n=b.length; const dp=Array.from({length:m+1},()=>new Array(n+1).fill(0)); for(let i=0;i<=m;i++) dp[i][0]=i; for(let j=0;j<=n;j++) dp[0][j]=j; for(let i=1;i<=m;i++) for(let j=1;j<=n;j++){ const cost = a[i-1]===b[j-1]?0:1; dp[i][j]=Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost); } return dp[m][n]; }
function similarity(a,b){ const maxLen=Math.max((a||'').length,(b||'').length,1); const dist=levenshtein(a||'', b||''); return 1-(dist/maxLen); }
function escapeHtml(s = '') {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// ---------- misc ----------
function exportBatchCsv(filename) { const batch = readBatchFile(filename); if (!batch) return null; const rows=['index,file_name,type,file_id']; batch.files.forEach((f,i)=>{ rows.push(`${i+1},"${(f.file_name||f.text||'').replace(/"/g,'""')}",${f.type},${f.file_id||''}`); }); return rows.join('\n'); }

console.log('Bot ready. Commands: /help, /sendfile, /doneadd, /addto <TOKEN>, /doneaddto, /edit_caption <TOKEN>, /listfiles, /deletefile <TOKEN>, /listusers, /getuser <id>, /start_<TOKEN>, /browse, /view_index');

