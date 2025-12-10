require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const fs = require('fs');
const path = require('path');

// ---------- Config ----------
const RAD_BOT_TOKEN = process.env.RAD_BOT_TOKEN;
const ADMIN_ID = process.env.ADMIN_ID ? String(process.env.ADMIN_ID) : null;
const ITEMS_PER_PAGE = 30; 
const SEARCH_RESULTS_PER_PAGE = 5; 

if (!RAD_BOT_TOKEN) throw new Error('❌ RAD_BOT_TOKEN is missing in .env');

const DATA_DIR = path.join(__dirname, 'knowledge_base');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const META_FILE = path.join(DATA_DIR, 'meta.json');
const FILES_DIR = path.join(DATA_DIR, 'Files'); 

// 🔒 Hidden System Files
const SYSTEM_FILES = ['users.json', 'meta.json', 'preview.json', 'config.json', 'Files']; 

// 🎓 Education Icons Map
const ICONS = {
    default: '📁',
    file: '📄', 
    qa: '❓',
    poll: '📊',
    'SSLC': '🎒',
    'PLUS TWO': '🎓',
    'NEET': '🩺',
    'JEE': '👷',
    'NTPC': '🚂',
    'PSC': '📜',
    'History': '🏺',
    'Science': '🧬',
    'Maths': '📐',
    'General': '🌐',
    'Quiz': '🧠'
};

// ---------- State Management ----------
const userPaths = {}; 
const userPages = {}; 
let DIR_CACHE = {}; 
let SEARCH_INDEX = []; 
let pendingUploads = {}; 
const activePolls = {}; 
const quizSessions = {}; 

const VIRTUAL_QUIZ_MENU = '__QUIZ_MENU__';

// ---------- Initialization ----------

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
if (!fs.existsSync(FILES_DIR)) fs.mkdirSync(FILES_DIR);

const DEFAULT_CATS = ['SSLC', 'PLUS TWO', 'NEET', 'JEE', 'NTPC', 'PSC', 'History', 'Science', 'Maths', 'General'];
DEFAULT_CATS.forEach(cat => {
    const p = path.join(DATA_DIR, cat);
    if (!fs.existsSync(p)) fs.mkdirSync(p);
});

// Init Users
let usersData = { ids: [], count: 0, scores: {} };
if (fs.existsSync(USERS_FILE)) {
    try { usersData = JSON.parse(fs.readFileSync(USERS_FILE)); } catch (e) {}
} else { fs.writeFileSync(USERS_FILE, JSON.stringify(usersData)); }

// Init Meta
let metaData = { 
    total_files: 0, 
    welcome_msg: "👋 <b>Welcome to EduMaster!</b>\n\nChoose an option below to start learning.",
    force_sub: { is_enabled: false, channels: [] },
    timers: { file: 0, bot_msg: 0, user_msg: 0 } 
};

if (fs.existsSync(META_FILE)) {
    try { 
        const loaded = JSON.parse(fs.readFileSync(META_FILE));
        metaData = { ...metaData, ...loaded };
        if (!metaData.force_sub) metaData.force_sub = { is_enabled: false, channels: [] };
        if (!metaData.timers) metaData.timers = { file: 0, bot_msg: 0, user_msg: 0 };
    } catch (e) {}
} else { saveMeta(); }

function saveMeta() { fs.writeFileSync(META_FILE, JSON.stringify(metaData, null, 2)); }

const bot = new Telegraf(RAD_BOT_TOKEN);

// ---------- Helper Functions ----------

function isAdmin(ctx) { return ctx.from && String(ctx.from.id) === ADMIN_ID; }
function sanitize(str) { return String(str || '').replace(/[/\\?%*:|"<>]/g, '-').trim(); }
function getAbsPath(relPath) { return path.resolve(DATA_DIR, relPath || ''); }

function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

function getSafeName(relPath, intendedName) {
    if (relPath.startsWith('Files')) {
        return sanitize(intendedName);
    }
    const absDir = getAbsPath(relPath);
    if (!fs.existsSync(absDir)) fs.mkdirSync(absDir, { recursive: true });
    
    const existing = fs.readdirSync(absDir).filter(f => f.endsWith('.json'));
    let max = 0;
    existing.forEach(f => {
        const num = parseInt(f.replace('.json', ''));
        if (!isNaN(num) && num > max) max = num;
    });
    return String(max + 1);
}

function parseLink(link) {
    if (!link) return null;
    const privateMatch = link.match(/(?:t\.me\/|telegram\.me\/)c\/(\d+)\/(\d+)/);
    if (privateMatch) return { chat_id: '-100' + privateMatch[1], message_id: parseInt(privateMatch[2]) };
    const publicMatch = link.match(/(?:t\.me\/|telegram\.me\/)(?!c\/)([^\/]+)\/(\d+)/);
    if (publicMatch) return { chat_id: '@' + publicMatch[1], message_id: parseInt(publicMatch[2]) };
    return null;
}

const getFolderIcon = (name) => {
    for (const key of Object.keys(ICONS)) {
        if (name.toUpperCase().includes(key.toUpperCase())) return ICONS[key];
    }
    return ICONS.default;
};

// 🔄 Cache & Search Index Builder
function rebuildCache(dir = DATA_DIR, relPath = '') {
    if (relPath === '') {
        DIR_CACHE = {}; 
        SEARCH_INDEX = []; 
    }
    if (!fs.existsSync(dir)) return;

    const items = fs.readdirSync(dir, { withFileTypes: true });
    
    const folders = items
        .filter(i => i.isDirectory() && !SYSTEM_FILES.includes(i.name))
        .map(i => {
            const configPath = path.join(dir, i.name, 'config.json');
            let icon = getFolderIcon(i.name);
            let locked_channel = null;
            if (fs.existsSync(configPath)) {
                try {
                    const conf = JSON.parse(fs.readFileSync(configPath));
                    if (conf.icon) icon = conf.icon;
                    if (conf.locked_channel) locked_channel = conf.locked_channel;
                } catch (e) {}
            }
            return { name: i.name, icon, locked_channel };
        });

    const files = items
        .filter(i => i.isFile() && i.name.endsWith('.json') && !SYSTEM_FILES.includes(i.name))
        .sort((a, b) => {
            const numA = parseInt(a.name);
            const numB = parseInt(b.name);
            if (!isNaN(numA) && !isNaN(numB)) return numA - numB;
            return a.name.localeCompare(b.name);
        })
        .map(i => {
            let type = 'file';
            try {
                const data = JSON.parse(fs.readFileSync(path.join(dir, i.name)));
                type = data.type || 'file';
                let icon = ICONS.file;
                if (type === 'qa') icon = ICONS.qa;
                if (type === 'poll') icon = ICONS.poll;
                
                SEARCH_INDEX.push({ 
                    name: i.name.replace('.json', ''), 
                    path: path.join(relPath, i.name), 
                    type: type 
                });

                return { name: i.name.replace('.json', ''), icon: icon, type: type };
            } catch (e) { return { name: i.name.replace('.json', ''), icon: ICONS.file, type: 'file' }; }
        });

    DIR_CACHE[relPath.replace(/\\/g, '/')] = { folders, files };

    for (const f of folders) {
        rebuildCache(path.join(dir, f.name), path.join(relPath, f.name));
    }
}
rebuildCache();

function getDirContents(pathOrVirtual) {
    if (pathOrVirtual === '') return { folders: [], files: [] }; 
    if (pathOrVirtual === VIRTUAL_QUIZ_MENU) {
        const rootContent = DIR_CACHE[''] || { folders: [], files: [] };
        const quizFolders = rootContent.folders.filter(f => f.name !== 'Files');
        return { folders: quizFolders, files: [] };
    }
    return DIR_CACHE[pathOrVirtual.replace(/\\/g, '/')] || { folders: [], files: [] };
}

function saveFileJson(relPath, fileName, fileData) {
    const abs = getAbsPath(relPath);
    if (!fs.existsSync(abs)) fs.mkdirSync(abs, { recursive: true });
    const finalName = getSafeName(relPath, fileName) + '.json';
    fs.writeFileSync(path.join(abs, finalName), JSON.stringify(fileData, null, 2));
    if (!metaData.total_files) metaData.total_files = 0;
    metaData.total_files++;
    saveMeta();
    rebuildCache(); 
}

// 🆕 UPDATED KEYBOARD BUILDER
function buildKeyboard(relPath, page = 0, isQuizActive = false, userId = null) {
    if (isQuizActive) return Markup.keyboard([['🛑 Stop Quiz']]).resize();

    const isAdminUser = userId && String(userId) === ADMIN_ID;
    let rows = [];

    // --- HOME ---
    if (relPath === '') {
        rows.push(['📂 Study Files', '🧠 Take a Quiz']);
        rows.push(['ℹ️ Help']); 
        return Markup.keyboard(rows).resize();
    }

    // --- QUIZ MENU (Categories) ---
    if (relPath === VIRTUAL_QUIZ_MENU) {
        const { folders } = getDirContents(VIRTUAL_QUIZ_MENU);
        const allItems = folders.map(f => `${f.icon} ${f.name}`);
        
        const totalPages = Math.ceil(allItems.length / ITEMS_PER_PAGE);
        const start = page * ITEMS_PER_PAGE;
        const pageItems = allItems.slice(start, start + ITEMS_PER_PAGE);

        for (let i = 0; i < pageItems.length; i += 2) rows.push(pageItems.slice(i, i + 2));

        const pagRow = [];
        if (page > 0) pagRow.push('⬅️ Prev');
        if (page < totalPages - 1) pagRow.push('Next ➡️');
        if (pagRow.length > 0) rows.push(pagRow);

        rows.push(['🏠 Home']); 
        return Markup.keyboard(rows).resize();
    }

    // --- FILES FOLDER (Study Files) ---
    if (relPath.startsWith('Files')) {
        const { folders, files } = getDirContents(relPath);
        const folderBtns = folders.map(f => `${f.icon} ${f.name}`);
        const fileBtns = files.map(f => `${f.icon} ${f.name}`);
        
        // 🟢 VISIBILITY FIX: Everyone sees Folders AND Files here
        const allItems = [...folderBtns, ...fileBtns];

        const totalPages = Math.ceil(allItems.length / ITEMS_PER_PAGE);
        const start = page * ITEMS_PER_PAGE;
        const pageItems = allItems.slice(start, start + ITEMS_PER_PAGE);

        for (let i = 0; i < pageItems.length; i += 2) rows.push(pageItems.slice(i, i + 2));

        const pagRow = [];
        if (page > 0) pagRow.push('⬅️ Prev');
        if (page < totalPages - 1) pagRow.push('Next ➡️');
        if (pagRow.length > 0) rows.push(pagRow);

        rows.push(['⬅️ Back', '🏠 Home']);
        rows.push(['🔍 Search']); 
        return Markup.keyboard(rows).resize();
    }

    // --- QUIZ CATEGORY ---
    const { folders, files } = getDirContents(relPath);
    const folderBtns = folders.map(f => `${f.icon} ${f.name}`);
    
    // 🟢 QUIZ LOGIC: Folders are visible to everyone. Files are Admin-only.
    let allItems = [...folderBtns];
    if (isAdminUser) {
        const fileBtns = files.map(f => `${f.icon} ${f.name}`);
        allItems = [...allItems, ...fileBtns];
    }
    
    const totalPages = Math.ceil(allItems.length / ITEMS_PER_PAGE);
    const start = page * ITEMS_PER_PAGE;
    const pageItems = allItems.slice(start, start + ITEMS_PER_PAGE);

    for (let i = 0; i < pageItems.length; i += 2) rows.push(pageItems.slice(i, i + 2));

    const pagRow = [];
    if (page > 0) pagRow.push('⬅️ Prev');
    if (page < totalPages - 1) pagRow.push('Next ➡️');
    if (pagRow.length > 0) rows.push(pagRow);

    rows.push(['🧠 Start Practice Quiz']);
    rows.push(['⬅️ Back', '🏠 Home']);

    return Markup.keyboard(rows).resize();
}

async function cleanReply(ctx, text, extra) {
    try { if (ctx.message) await ctx.deleteMessage().catch(() => {}); } catch (e) {}
    const sent = await ctx.reply(text, extra);
    if (metaData.timers.bot_msg > 0) {
        setTimeout(() => ctx.telegram.deleteMessage(ctx.chat.id, sent.message_id).catch(()=>{}), metaData.timers.bot_msg);
    }
    return sent;
}

// ---------- Middleware ----------

bot.use(async (ctx, next) => {
    if (!ctx.from || isAdmin(ctx) || !metaData.force_sub.is_enabled) return next();
    if (ctx.callbackQuery) return next(); 

    const channels = metaData.force_sub.channels;
    if (channels.length === 0) return next();

    let notJoined = [];
    for (const ch of channels) {
        try {
            const member = await ctx.telegram.getChatMember(ch, ctx.from.id);
            if (['left', 'kicked'].includes(member.status)) notJoined.push(ch);
        } catch (e) { notJoined.push(ch); } 
    }

    if (notJoined.length > 0) {
        const buttons = notJoined.map((ch, i) => [Markup.button.url(`📢 Join Channel ${i + 1}`, `https://t.me/${ch.replace('@', '')}`)]);
        buttons.push([Markup.button.callback('✅ I Have Joined', 'check_subscription')]);
        return ctx.reply(`⚠️ <b>Access Denied</b>\nPlease join our channels to use this bot.`, { parse_mode: 'HTML', ...Markup.inlineKeyboard(buttons) });
    }
    await next();
});

bot.action('check_subscription', async (ctx) => {
    await ctx.answerCbQuery("✅ Checking...");
    await ctx.deleteMessage();
    ctx.reply("👋 Welcome! Type /start or continue.");
});

// ---------- Search ----------

bot.command('search', (ctx) => {
    const query = ctx.message.text.replace('/search', '').trim().toLowerCase();
    if (!query) return ctx.reply("🔍 Usage: /search <keyword>");
    const ALLOWED_FOLDER = 'Files'; 
    const results = SEARCH_INDEX.filter(item => {
        const normalizedPath = item.path.replace(/\\/g, '/');
        const isInsideFiles = normalizedPath.startsWith(ALLOWED_FOLDER + '/') || normalizedPath === ALLOWED_FOLDER;
        const matchesQuery = item.name.toLowerCase().includes(query);
        return isInsideFiles && matchesQuery;
    });
    if (results.length === 0) return ctx.reply(`❌ No results found inside the '${ALLOWED_FOLDER}' folder.`);
    sendSearchResults(ctx, query, results, 0);
});

bot.hears('🔍 Search', (ctx) => {
    const curr = userPaths[ctx.chat.id] || '';
    if (!curr.startsWith('Files')) return;
    ctx.reply("🔍 Type /search followed by your topic.\nExample: <code>/search NEET</code>", { parse_mode: 'HTML' });
});

function sendSearchResults(ctx, query, results, page) {
    const totalPages = Math.ceil(results.length / SEARCH_RESULTS_PER_PAGE);
    const start = page * SEARCH_RESULTS_PER_PAGE;
    const pageItems = results.slice(start, start + SEARCH_RESULTS_PER_PAGE);
    let msg = `🔍 <b>Results for "${query}"</b> (Page ${page+1}/${totalPages})\n\n`;
    const buttons = [];
    pageItems.forEach(item => {
        buttons.push([Markup.button.callback(`${item.type === 'poll' ? '📊' : '📄'} ${item.name}`, `get|${item.path}`)]);
    });
    const nav = [];
    if (page > 0) nav.push(Markup.button.callback('⬅️', `srch|${query}|${page-1}`));
    if (page < totalPages - 1) nav.push(Markup.button.callback('➡️', `srch|${query}|${page+1}`));
    if (nav.length > 0) buttons.push(nav);
    const extra = { parse_mode: 'HTML', ...Markup.inlineKeyboard(buttons) };
    if (ctx.callbackQuery) ctx.editMessageText(msg, extra).catch(()=>{});
    else ctx.reply(msg, extra);
}

bot.action(/^srch\|(.*)\|(.*)$/, (ctx) => {
    const [, query, pageStr] = ctx.match;
    const ALLOWED_FOLDER = 'Files';
    const results = SEARCH_INDEX.filter(item => {
        const normalizedPath = item.path.replace(/\\/g, '/');
        return (normalizedPath.startsWith(ALLOWED_FOLDER + '/') || normalizedPath === ALLOWED_FOLDER) 
            && item.name.toLowerCase().includes(query);
    });
    sendSearchResults(ctx, query, results, parseInt(pageStr));
    ctx.answerCbQuery();
});

bot.action(/^get\|(.*)$/, async (ctx) => {
    const relPath = ctx.match[1];
    const absPath = path.join(DATA_DIR, relPath);
    if (!fs.existsSync(absPath)) return ctx.answerCbQuery("❌ File deleted.");
    const name = path.basename(relPath, '.json');
    await sendFileContent(ctx, name, absPath);
    ctx.answerCbQuery();
});

// ---------- Admin Commands ----------
bot.command('fson', (ctx) => { if (isAdmin(ctx)) { metaData.force_sub.is_enabled = true; saveMeta(); ctx.reply("🔐 Force Sub ENABLED."); }});
bot.command('fsoff', (ctx) => { if (isAdmin(ctx)) { metaData.force_sub.is_enabled = false; saveMeta(); ctx.reply("🔓 Force Sub DISABLED."); }});
bot.command('addfs', (ctx) => { if(isAdmin(ctx)) { const ch=ctx.message.text.split(' ')[1]; if(!ch)return; metaData.force_sub.channels.push(ch); saveMeta(); ctx.reply(`✅ Added ${ch}`); }});
bot.command('delfs', (ctx) => { if(isAdmin(ctx)) { const ch=ctx.message.text.split(' ')[1]; metaData.force_sub.channels=metaData.force_sub.channels.filter(c=>c!==ch); saveMeta(); ctx.reply(`🗑️ Removed ${ch}`); }});
bot.command('timers', (ctx) => { if(isAdmin(ctx)) ctx.reply(`⏱️ File: ${metaData.timers.file/60000}m | BotMsg: ${metaData.timers.bot_msg/60000}m`); });
bot.command('settimer', (ctx) => { if (isAdmin(ctx)) { const args = ctx.message.text.split(' '); const type = args[1], mins = parseInt(args[2]); if (!['file', 'bot'].includes(type) || isNaN(mins)) return ctx.reply("Usage: /settimer <file|bot> <mins>"); metaData.timers[type === 'file' ? 'file' : 'bot_msg'] = mins * 60000; saveMeta(); ctx.reply(`✅ Timer updated.`); }});
bot.command('mkdir', (ctx) => { if (!isAdmin(ctx)) return; const names = ctx.message.text.replace('/mkdir', '').split('|').map(s => s.trim()).filter(s => s); const current = getAbsPath(userPaths[ctx.chat.id] || ''); names.forEach(n => fs.mkdirSync(path.join(current, sanitize(n)), { recursive: true })); rebuildCache(); cleanReply(ctx, `✅ Created folders.`, buildKeyboard(userPaths[ctx.chat.id] || '', 0, false, ctx.from.id)); });
bot.command('addpoll', async (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Pre-process text to handle bulk pasting
    // We replace any occurrence of "/addpoll" with a newline to separate entries
    // We also split by newline characters
    const rawText = ctx.message.text;
    const cleanList = rawText
        .replace(/\/addpoll/g, '\n') // Turn command calls into new lines
        .split('\n')                 // Split by new lines
        .map(line => line.trim())    // Clean whitespace
        .filter(line => line.length > 5 && line.includes('|')); // Only keep valid lines with separators

    if (cleanList.length === 0) {
        return ctx.reply("❌ Usage: /addpoll Question | Opt1 | Opt2 | Index | Explanation\n\n(You can paste multiple lines at once)");
    }

    let success = 0;
    let fail = 0;
    const currentRelPath = userPaths[ctx.chat.id] || '';

    // 2. Process each line
    for (const line of cleanList) {
        try {
            const parts = line.split('|').map(s => s.trim()).filter(s => s.length > 0);

            // Validation: Need at least Q + 2 Opts + Index (4 parts)
            if (parts.length < 4) { fail++; continue; }

            let fileName, q, opts, correct = -1, expl = "";
            const isQuestion = parts[0].length > 20 || parts[0].includes('?');
            let startIndex = 0;

            // Determine if Filename is provided or needs generation
            if (isQuestion) {
                q = parts[0];
                // Create unique filename: First 5 words + Random Number
                const safeSnippet = q.replace(/[^a-zA-Z0-9 ]/g, "").split(/\s+/).slice(0, 5).join('_');
                // We add Math.random to ensure uniqueness during bulk upload
                fileName = `${safeSnippet}_${Date.now().toString().slice(-4)}_${Math.floor(Math.random() * 999)}`;
                startIndex = 1;
            } else {
                fileName = parts[0];
                q = parts[1];
                startIndex = 2;
            }

            const remaining = parts.slice(startIndex);
            const last = remaining[remaining.length - 1];
            const secondLast = remaining[remaining.length - 2];
            const isStrictIdx = (val) => /^\d+$/.test(val);

            // Parse Options, Correct Index, and Explanation
            if (remaining.length >= 2 && isStrictIdx(secondLast)) {
                correct = parseInt(secondLast);
                expl = last;
                opts = remaining.slice(0, -2);
            } else if (remaining.length >= 1 && isStrictIdx(last)) {
                correct = parseInt(last);
                opts = remaining.slice(0, -1);
            } else {
                opts = remaining; // No index found, treat all as options (invalid for Quiz)
            }

            // Index Validation
            if (correct === -1 || correct >= opts.length) { fail++; continue; }

            // Save
            saveFileJson(currentRelPath, fileName, {
                type: 'poll',
                question: q,
                options: opts,
                correct_option_id: correct,
                explanation: expl,
                added_at: Date.now()
            });

            success++;
        } catch (e) {
            console.error("AddPoll Error:", e);
            fail++;
        }
    }

    // 3. Final Report
    let msg = `✅ <b>Bulk Import Complete</b>\n\n📥 Imported: ${success}\n⚠️ Failed: ${fail}`;
    if (fail > 0) msg += `\n(Check format: Question | Opt1 | Opt2 | Index | Expl)`;

    cleanReply(ctx, msg, { parse_mode: 'HTML', ...buildKeyboard(currentRelPath, 0, false, ctx.from.id) });
});
bot.command('refresh', (ctx) => {if (!isAdmin(ctx)) return;rebuildCache();ctx.reply("♻️ System cache refreshed! New files/folders should be visible now.");});
bot.command('del', (ctx) => { if (!isAdmin(ctx)) return; const name = sanitize(ctx.message.text.replace('/del', '').trim()); const current = getAbsPath(userPaths[ctx.chat.id] || ''); const pDir = path.join(current, name); const pFile = path.join(current, name + '.json'); if (fs.existsSync(pDir)) fs.rmSync(pDir, { recursive: true }); else if (fs.existsSync(pFile)) fs.unlinkSync(pFile); else return ctx.reply("❌ Not found."); rebuildCache(); cleanReply(ctx, `🗑️ Deleted: ${name}`, buildKeyboard(userPaths[ctx.chat.id] || '', 0, false, ctx.from.id)); });
bot.action(/^del\|(.*)$/, (ctx) => { if (!isAdmin(ctx)) return ctx.answerCbQuery("🔒 Admins only"); const relPath = ctx.match[1]; const absPath = path.join(DATA_DIR, relPath); if (fs.existsSync(absPath)) fs.unlinkSync(absPath); rebuildCache(); ctx.deleteMessage(); ctx.reply(`🗑️ Deleted.`); });
bot.command('editq', (ctx) => { if (!isAdmin(ctx)) return; const [name, newQ] = ctx.message.text.replace('/editq', '').split('|').map(s => s.trim()); if (!name || !newQ) return ctx.reply("Usage: /editq FileName | NewQuestion"); const current = getAbsPath(userPaths[ctx.chat.id] || ''); const pFile = path.join(current, name + '.json'); if(fs.existsSync(pFile)) { const data = JSON.parse(fs.readFileSync(pFile)); if(data.type !== 'poll' && data.type !== 'qa') return ctx.reply("❌ Not a poll/qa"); data.question = newQ; fs.writeFileSync(pFile, JSON.stringify(data, null, 2)); ctx.reply("✅ Updated."); } else ctx.reply("❌ File not found."); });

// ---------- Restored Features (Leaderboard, MyScore, Help) ----------

bot.command('leaderboard', async (ctx) => {
    const users = JSON.parse(fs.readFileSync(USERS_FILE));
    if (!users.scores) return ctx.reply("📉 No scores yet.");
    const sorted = Object.entries(users.scores).sort((a,b) => b[1] - a[1]).slice(0, 10);
    let msg = "<b>🏆 Leaderboard</b>\n\n";
    for (const [i, [uid, score]] of sorted.entries()) {
        try { const c = await ctx.telegram.getChat(uid); msg += `${i+1}. <b>${c.first_name}</b>: ${score}\n`; }
        catch(e) { msg += `${i+1}. User ${uid}: ${score}\n`; }
    }
    ctx.replyWithHTML(msg);
});

bot.command('myscore', (ctx) => {
    const users = JSON.parse(fs.readFileSync(USERS_FILE));
    const s = (users.scores && users.scores[ctx.from.id]) || 0;
    ctx.reply(`🎯 Your Total Score: ${s}`);
});

const sendHelp = (ctx) => {
    if (isAdmin(ctx)) {
        return ctx.replyWithHTML(
            `<b>🔧 Admin Dashboard</b>\n\n` +
            `<b>📂 Files:</b> /mkdir, /add, /del\n` +
            `<b>🧠 Polls:</b> /addpoll, /editq, /setexpl\n` +
            `<b>⚙️ System:</b> /fson, /timers, /broadcast`
        );
    } else {
        return ctx.replyWithHTML(
            `<b>📖 User Guide</b>\n\n` +
            `<b>1. 🧠 Practice Quiz:</b> Click "Take a Quiz" > Select Category > "Start Practice Quiz".\n` +
            `<b>2. 📂 Study Files:</b> Click "Study Files" to browse notes.\n` +
            `<b>3. 🔍 Search:</b> Go to "Study Files" and click Search.\n` +
            `<b>4. 🏆 Stats:</b> /leaderboard, /myscore`
        );
    }
};
bot.hears('ℹ️ Help', sendHelp);
bot.command('help', sendHelp);

// ---------- Navigation Handlers ----------

bot.start((ctx) => {
    userPaths[ctx.chat.id] = '';
    const u = JSON.parse(fs.readFileSync(USERS_FILE));
    if (!u.ids.includes(ctx.from.id)) { u.ids.push(ctx.from.id); fs.writeFileSync(USERS_FILE, JSON.stringify(u)); }
    const welcome = isAdmin(ctx) ? `${metaData.welcome_msg}\n\n🔧 <b>Admin Mode</b>\n/help` : metaData.welcome_msg;
    cleanReply(ctx, welcome, { parse_mode: 'HTML', ...buildKeyboard('', 0, false, ctx.from.id) });
});

bot.hears('🏠 Home', (ctx) => { 
    userPaths[ctx.chat.id] = ''; 
    userPages[ctx.chat.id] = 0; 
    cleanReply(ctx, "🏠 Menu", buildKeyboard('', 0, false, ctx.from.id)); 
});

bot.hears('⬅️ Back', (ctx) => { 
    if(quizSessions[ctx.from.id]) return ctx.reply("Please /stopquiz first.");
    const curr = userPaths[ctx.chat.id] || '';
    if (curr === '' || curr === VIRTUAL_QUIZ_MENU || curr === 'Files') {
        userPaths[ctx.chat.id] = ''; 
    } else {
        const parts = curr.split(path.sep);
        if (parts.length === 1 && curr !== 'Files') {
            userPaths[ctx.chat.id] = VIRTUAL_QUIZ_MENU;
        } else {
             userPaths[ctx.chat.id] = parts.slice(0,-1).join(path.sep); 
             if(userPaths[ctx.chat.id] === '') userPaths[ctx.chat.id] = ''; 
        }
    }
    userPages[ctx.chat.id] = 0;
    cleanReply(ctx, "📂 Back", buildKeyboard(userPaths[ctx.chat.id], 0, false, ctx.from.id));
});

bot.hears('Next ➡️', (ctx) => { if(quizSessions[ctx.from.id]) return; userPages[ctx.chat.id] = (userPages[ctx.chat.id]||0)+1; cleanReply(ctx,"➡️",buildKeyboard(userPaths[ctx.chat.id], userPages[ctx.chat.id], false, ctx.from.id)); });
bot.hears('⬅️ Prev', (ctx) => { if(quizSessions[ctx.from.id]) return; const p=userPages[ctx.chat.id]||0; if(p>0) userPages[ctx.chat.id]=p-1; cleanReply(ctx,"⬅️",buildKeyboard(userPaths[ctx.chat.id], userPages[ctx.chat.id], false, ctx.from.id)); });

bot.hears('📂 Study Files', (ctx) => {
    userPaths[ctx.chat.id] = 'Files';
    userPages[ctx.chat.id] = 0;
    cleanReply(ctx, "📂 <b>Study Files</b>", { parse_mode: 'HTML', ...buildKeyboard('Files', 0, false, ctx.from.id) });
});

bot.hears('🧠 Take a Quiz', (ctx) => {
    userPaths[ctx.chat.id] = VIRTUAL_QUIZ_MENU;
    userPages[ctx.chat.id] = 0;
    cleanReply(ctx, "🧠 <b>Select a Quiz Category</b>", { parse_mode: 'HTML', ...buildKeyboard(VIRTUAL_QUIZ_MENU, 0, false, ctx.from.id) });
});

// ---------- Quiz Logic (MOVED UP - MUST BE BEFORE bot.on('text')) ----------

// 🟢 ASYNC START HANDLER
bot.hears('🧠 Start Practice Quiz', async (ctx) => {
    const userId = ctx.from.id;
    const chatId = ctx.chat.id;
    const relPath = userPaths[chatId]; 

    // 1. Validate
    if (!relPath || relPath === '' || relPath === 'Files' || relPath === VIRTUAL_QUIZ_MENU) {
         return ctx.reply("⚠️ Please navigate to a specific Category (e.g., NEET, SSLC) first.");
    }

    const absDir = getAbsPath(relPath);
    let validPolls = [];
    
    // 2. Feedback to User
    await ctx.reply("⏳ <b>Loading Quiz...</b>", { parse_mode: 'HTML' });

    try {
        if (!fs.existsSync(absDir)) return ctx.reply("❌ Folder does not exist.");
        
        // 3. ASYNC READ (Prevent Freezing)
        const files = await fs.promises.readdir(absDir);
        
        for (const file of files) {
            if (!file.endsWith('.json')) continue;
            try {
                const content = await fs.promises.readFile(path.join(absDir, file));
                const data = JSON.parse(content);

                // 4. STRICT VALIDATION
                if (data.type === 'poll' && 
                    data.correct_option_id !== undefined && 
                    data.correct_option_id > -1 &&
                    Array.isArray(data.options) && 
                    data.options.length >= 2 &&
                    data.question && data.question.length < 300
                ) {
                    validPolls.push(data);
                }
            } catch (err) { }
        }
    } catch (e) {
        console.error("Quiz Scan Error:", e);
        return ctx.reply("❌ Error scanning quiz files.");
    }

    if (validPolls.length === 0) {
        return ctx.reply("📉 No valid practice questions found in this folder.\n(Ensure polls have correct answers set and texts are not too long).");
    }

    const shuffled = shuffleArray(validPolls);
    const selection = shuffled.slice(0, 100);

    quizSessions[userId] = {
        queue: selection,      
        currentIdx: 0,
        score: 0,
        path: relPath,
        activePollId: null,
        chatId: chatId         
    };

    await ctx.reply(`🚀 <b>Starting Quiz!</b>\n\n📂 Category: <b>${path.basename(relPath)}</b>\n❓ Questions: ${selection.length}\n⏱️ Delay: 1.5s per question`, { 
        parse_mode: 'HTML', 
        ...buildKeyboard(relPath, 0, true, userId) 
    });
    
    sendNextQuizQuestion(null, userId);
});

bot.hears('🛑 Stop Quiz', (ctx) => stopQuiz(ctx));
bot.command('stopquiz', (ctx) => stopQuiz(ctx));

function stopQuiz(ctx, userId = null) {
    const id = userId || (ctx && ctx.from ? ctx.from.id : null);
    if (!id) return;
    
    const session = quizSessions[id];
    if (!session) {
        if (ctx && ctx.chat) ctx.reply("❌ No active quiz.");
        return;
    }
    
    const scoreMsg = `🏁 <b>Quiz Ended!</b>\n\n✅ Score: ${session.score} / ${session.currentIdx}\n📂 Category: ${session.path}`;
    const chatId = session.chatId; 
    delete quizSessions[id];
    
    bot.telegram.sendMessage(chatId, scoreMsg, { 
        parse_mode: 'HTML', 
        ...buildKeyboard(userPaths[chatId] || '', 0, false, id) 
    }).catch(e => console.log("StopQuiz Error:", e.message));
}

// 🟢 SAFE SENDER WITH TIMEOUT (PREVENTS LOOPS)
async function sendNextQuizQuestion(ctx, userId) {
    const session = quizSessions[userId];
    if (!session) return;

    if (session.currentIdx >= session.queue.length) {
        stopQuiz(null, userId);
        return;
    }

    const data = session.queue[session.currentIdx];
    
    try {
        if (!data.question || !Array.isArray(data.options) || data.options.length < 2) throw new Error("Invalid Data");

        const safeQ = data.question.length > 295 ? data.question.substring(0, 295) + '...' : data.question;
        const safeOpts = data.options.map(o => {
            const s = String(o);
            return s.length > 95 ? s.substring(0, 95) + '...' : s;
        });

        const m = await bot.telegram.sendQuiz(session.chatId, safeQ, safeOpts, { 
            correct_option_id: data.correct_option_id, 
            is_anonymous: false,
            explanation: data.explanation ? (data.explanation.length > 195 ? data.explanation.substring(0, 195) + '...' : data.explanation) : "",
            explanation_parse_mode: 'HTML'
        });

        session.activePollId = m.poll.id;
        activePolls[m.poll.id] = { 
            correct: data.correct_option_id, 
            points: 10, 
            isQuizMode: true, 
            userId: userId 
        };
        
    } catch(e) {
        console.error(`[Quiz Error] Skipped Q for ${userId}: ${e.message}`);
        session.currentIdx++;
        // 🔴 SAFETY DELAY
        setTimeout(() => { sendNextQuizQuestion(null, userId); }, 1000); 
    }
}

bot.on('poll_answer', (ctx) => {
    const { poll_id, user, option_ids } = ctx.pollAnswer;
    const pollData = activePolls[poll_id];

    if (pollData) {
        if (option_ids[0] === pollData.correct) {
            try {
                const u = JSON.parse(fs.readFileSync(USERS_FILE));
                if (!u.scores) u.scores = {};
                u.scores[user.id] = (u.scores[user.id] || 0) + pollData.points;
                fs.writeFileSync(USERS_FILE, JSON.stringify(u));
            } catch(e){}
        }

        if (pollData.isQuizMode && pollData.userId === user.id) {
            const session = quizSessions[user.id];
            if (session && session.activePollId === poll_id) {
                if (option_ids[0] === pollData.correct) session.score++;
                session.currentIdx++;
                setTimeout(() => { sendNextQuizQuestion(null, user.id); }, 1500); 
            }
        }
    }
});

async function sendFileContent(ctx, name, absPath) {
    try {
        const data = JSON.parse(fs.readFileSync(absPath));
        const extra = isAdmin(ctx) ? Markup.inlineKeyboard([
            [Markup.button.callback('🗑️ Delete', `del|${path.relative(DATA_DIR, absPath).replace(/\\/g, '/')}`)]
        ]) : null;

        if (data.type === 'copy') {
            const s = await ctx.telegram.copyMessage(ctx.chat.id, data.from_chat_id, data.message_id, { caption: data.caption, reply_markup: extra ? extra.reply_markup : undefined });
            if (metaData.timers.file > 0) setTimeout(()=>ctx.telegram.deleteMessage(ctx.chat.id, s.message_id).catch(()=>{}), metaData.timers.file);
        }
        else if (data.type === 'qa') ctx.reply(`❓ ${data.question}\n\n💡 ${data.answer}`, extra);
        else if (data.type === 'poll') {
            if (data.correct_option_id > -1) {
                const m = await ctx.replyWithQuiz(data.question, data.options, { correct_option_id: data.correct_option_id, is_anonymous: false, explanation: data.explanation || "", explanation_parse_mode: 'HTML' });
                activePolls[m.poll.id] = { correct: data.correct_option_id, points: 10 };
                if(isAdmin(ctx)) ctx.reply(`⚙️ Admin: ${name}`, extra); 
            } else { await ctx.replyWithPoll(data.question, data.options); }
        }
    } catch(e) { ctx.reply("❌ Error"); }
}

// ---------- GENERIC HANDLER (MUST BE LAST) ----------

bot.on('text', async (ctx) => {
    if(quizSessions[ctx.from.id] && !ctx.message.text.startsWith('/')) return ctx.reply("⚠️ Quiz in progress! Answer or /stopquiz");

    const text = ctx.message.text;
    // 🛑 If this list contains 'Start Practice Quiz', it returns here! 
    // That's why specific handlers must be ABOVE this function.
    if (text.startsWith('/') || ['🏠 Home', '⬅️ Back', 'Next ➡️', '⬅️ Prev', '🔍 Search', '🧠 Start Practice Quiz', '🛑 Stop Quiz', '📂 Study Files', '🧠 Take a Quiz', 'ℹ️ Help'].includes(text)) return;

    if (pendingUploads[ctx.from.id]) {
        const d = pendingUploads[ctx.from.id];
        saveFileJson(userPaths[ctx.chat.id]||'', text.trim(), { type: 'copy', from_chat_id: d.from_chat_id, message_id: d.message_id, caption: d.caption });
        delete pendingUploads[ctx.from.id];
        return cleanReply(ctx, `✅ Saved.`, buildKeyboard(userPaths[ctx.chat.id]||'', 0, false, ctx.from.id));
    }

    const curr = userPaths[ctx.chat.id] || '';
    const { folders, files } = getDirContents(curr);
    
    let match = folders.find(f => text.includes(f.name)) || files.find(f => text.includes(f.name));

    if (!match) return ctx.reply("🔍 Select an option from the menu.");

    if (folders.find(f => f.name === match.name)) {
        if (curr === VIRTUAL_QUIZ_MENU) {
            userPaths[ctx.chat.id] = match.name; 
        } else {
            userPaths[ctx.chat.id] = path.join(curr, match.name);
        }
        userPages[ctx.chat.id] = 0;
        return cleanReply(ctx, `📂 ${match.name}`, buildKeyboard(userPaths[ctx.chat.id], 0, false, ctx.from.id));
    } else {
        if (curr.startsWith('Files') || isAdmin(ctx)) {
             await sendFileContent(ctx, match.name, path.join(getAbsPath(curr), match.name + '.json'));
        } else {
             ctx.reply("⚠️ Please click 'Start Practice Quiz' to view questions.");
        }
    }
});

bot.launch().then(()=>console.log("✅ EduBot Active"));
process.once('SIGINT', ()=>bot.stop('SIGINT'));
process.once('SIGTERM', ()=>bot.stop('SIGTERM'));