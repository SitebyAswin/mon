const { Telegraf, Markup } = require('telegraf');
const fs = require('fs');
const path = require('path');
const archiver = require('archiver');

// ---------- Config ----------
const CLOUD_BOT_TOKEN = process.env.CLOUD_BOT_TOKEN;
const ADMIN_ID = process.env.ADMIN_ID ? String(process.env.ADMIN_ID) : null;
const ITEMS_PER_PAGE = 10; // ⚡ LIMIT ITEMS PER PAGE

if (!CLOUD_BOT_TOKEN) throw new Error('CLOUD_BOT_TOKEN is missing');

const DATA_DIR = path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const META_FILE = path.join(DATA_DIR, 'meta.json');
const DIRS = ['Series', 'Movies', 'Sequels'];
const CLEAR_TIMER = 24 * 60 * 60 * 1000; // 24 Hours in milliseconds

// 🔒 Hidden System Files
const SYSTEM_FILES = [
    'users.json', 
    'meta.json', 
    'telegraph_index.json', 
    'preview.json',
    'activity.json', 
    'config.json',
    'links.json' // <--- ADD THIS
];

const LINKS_FILE = path.join(DATA_DIR, 'links.json');

// Initialize links.json if not exists
if (!fs.existsSync(LINKS_FILE)) fs.writeFileSync(LINKS_FILE, JSON.stringify({}, null, 2));

// Helper to generate a random 8-character ID
function generateId() {
    return Math.random().toString(36).substr(2, 8);
}

// 📝 Activity Log File
const ACTIVITY_FILE = path.join(DATA_DIR, 'activity.json');
const MAX_LOGS = 1000; // Keep last 1000 actions to save space

// Initialize
if (!fs.existsSync(ACTIVITY_FILE)) fs.writeFileSync(ACTIVITY_FILE, JSON.stringify([], null, 2));

// 📝 Logger Function
function logUserAction(ctx, action, detail) {
    if (!ctx.from) return;
    
    try {
        const logs = JSON.parse(fs.readFileSync(ACTIVITY_FILE, 'utf8'));
        
        const newLog = {
            user_id: ctx.from.id,
            name: ctx.from.first_name,
            username: ctx.from.username ? `@${ctx.from.username}` : 'N/A',
            action: action, // e.g., 'NAVIGATE', 'DOWNLOAD', 'SEARCH'
            detail: detail, // e.g., 'Movies > Inception', 'Iron Man'
            time: new Date().toISOString()
        };

        logs.unshift(newLog); // Add to top
        
        // Trim if too big
        if (logs.length > MAX_LOGS) logs.length = MAX_LOGS;

        fs.writeFileSync(ACTIVITY_FILE, JSON.stringify(logs, null, 2));
    } catch (e) {
        console.error("Logging failed:", e);
    }
}

// 🎨 Custom Icons
const FOLDER_ICON = '📁'; // Change this to whatever you want (e.g. 💿, 📺, 📂)
const FILE_ICON = '📄';

// ---------- Initialization ----------

// Ensure directory structure
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
DIRS.forEach(d => {
    const p = path.join(DATA_DIR, d);
    if (!fs.existsSync(p)) fs.mkdirSync(p);
});

// 2. Initialize or Validate Users File
let usersData = { ids: [], count: 0 };
if (fs.existsSync(USERS_FILE)) {
    try {
        const raw = fs.readFileSync(USERS_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed.ids)) {
            usersData = parsed;
        } else {
            // Fix invalid structure
            usersData = { ids: [], count: 0 };
            fs.writeFileSync(USERS_FILE, JSON.stringify(usersData, null, 2));
        }
    } catch (err) {
        console.error("Error reading users.json, resetting:", err);
        fs.writeFileSync(USERS_FILE, JSON.stringify(usersData, null, 2));
    }
} else {
    fs.writeFileSync(USERS_FILE, JSON.stringify(usersData, null, 2));
}

// 3. Initialize or Validate Meta File
// Default Timers: 24 Hours (in milliseconds)
const DEFAULT_TIMER = 24 * 60 * 60 * 1000; 

let metaData = { 
    total_files: 0, 
    welcome_msg: "👋 Welcome to the Media Bot!\n Browse our collection below.",
    force_sub: { is_enabled: false, channels: [] },
    timers: { 
        file: DEFAULT_TIMER,      // For Videos/Files
        bot_msg: DEFAULT_TIMER,   // For Menu/Text replies
        user_msg: DEFAULT_TIMER   // For Commands user types
    }
};

// Helper to count files
const recalculateFiles = () => {
    let count = 0;
    const scan = (d) => {
        if (!fs.existsSync(d)) return;
        fs.readdirSync(d, {withFileTypes:true}).forEach(i => {
            if (SYSTEM_FILES.includes(i.name)) return;
            if (i.isDirectory()) scan(path.join(d,i.name));
            else if (i.name.endsWith('.json')) count++;
        });
    };
    try { scan(DATA_DIR); } catch(e){}
    return count;
};

if (fs.existsSync(META_FILE)) {
    try {
        const raw = fs.readFileSync(META_FILE, 'utf8');
        const loaded = JSON.parse(raw);
        metaData = { ...metaData, ...loaded };

        // Ensure structures exist if loading from old file
        if (!metaData.force_sub) metaData.force_sub = { is_enabled: false, channels: [] };
        if (!metaData.timers) metaData.timers = { file: DEFAULT_TIMER, bot_msg: DEFAULT_TIMER, user_msg: DEFAULT_TIMER };

    } catch (err) {
        console.error("Error reading meta.json, resetting:", err);
        metaData.total_files = recalculateFiles();
        fs.writeFileSync(META_FILE, JSON.stringify(metaData, null, 2));
    }
} else {
    metaData.total_files = recalculateFiles();
    fs.writeFileSync(META_FILE, JSON.stringify(metaData, null, 2));
}

// ---------- Session State ----------
const userPaths = {}; 
const userPages = {}; // ⚡ FIXED: This variable is required for pagination!
let pendingUploads = {}; // <--- ADD THIS (Stores file info while waiting for name)
const clipboard = {}; // Stores { userId: { name, sourcePath, type } }

const bot = new Telegraf(CLOUD_BOT_TOKEN);

// 1. User Message Auto-Delete
bot.use(async (ctx, next) => {
    if (ctx.message) {
        const chatId = ctx.chat.id;
        const msgId = ctx.message.message_id;
        // USE DYNAMIC TIMER HERE 👇
        setTimeout(() => {
            ctx.telegram.deleteMessage(chatId, msgId).catch(()=>{});
        }, metaData.timers.user_msg); 
    }
    await next();
});

// ⚡ CACHE STORAGE
// Structure: { "Series/GoT": { folders: ["S01"], files: ["Poster.json"] } }
let DIR_CACHE = {}; 

// 🚫 SEARCH BLACKLIST (Regex Patterns)
const IGNORED_NAMES = [
    /^\d{3,4}p$/i,             // Matches: 480p, 720p, 1080p, 4k
    /^Season[ _-]?\d+$/i,      // Matches: Season 1, Season_1, Season01
    /^EP\d+$/i,                // Matches: EP01, EP10
    /^Episode[ _-]?\d+$/i      // Matches: Episode 01
];

// ---------- Data Helpers ----------

function saveMeta() {
    fs.writeFileSync(META_FILE, JSON.stringify(metaData, null, 2));
}

function trackUser(ctx) {
    if (!ctx.from) return;
    const uid = ctx.from.id;
    let currentUsers = { ids: [], count: 0 };
        try {
            currentUsers = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
            if (!currentUsers.ids) currentUsers.ids = [];
        } catch (e) {}
    
        if (!currentUsers.ids.includes(uid)) {
            currentUsers.ids.push(uid);
            currentUsers.count = currentUsers.ids.length;
            fs.writeFileSync(USERS_FILE, JSON.stringify(currentUsers, null, 2));
        console.log(`[NEW USER] ${ctx.from.first_name} (${uid})`);
    }
}

function getStats() {
    try {
        const u = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
        return { users: u.count || 0, files: metaData.total_files || 0 };
    } catch (e) { return { users: 0, files: 0 }; }
}

function isAdmin(ctx) { return String(ctx.from.id) === ADMIN_ID; }

function sanitize(str) { return String(str || '').replace(/[/\\?%*:|"<>]/g, '-').trim(); }

// 🎨 Helper: Formats path like "Series > Drama > Lost"
function formatDisplayPath(relPath) {
    if (!relPath || relPath === '.' || relPath.trim() === '') return 'Main Collection';
    // Replace slashes with a nice arrow
    return relPath.replace(/\\/g, ' ﹥ ').replace(/\//g, ' ﹥ ');
}

function isIgnored(name) { return IGNORED_NAMES.some(rx => rx.test(name)); }

function getAbsPath(relPath) {
    if (!relPath) return DATA_DIR;
    const resolved = path.resolve(DATA_DIR, relPath);
    if (!resolved.startsWith(path.resolve(DATA_DIR))) return DATA_DIR;
    return resolved;
}

// 🔄 CACHE BUILDER FUNCTION (Updated)
function rebuildCache(dir = DATA_DIR, relPath = '') {
    if (relPath === '') DIR_CACHE = {}; // Clear on root scan
    if (!fs.existsSync(dir)) return;

    const items = fs.readdirSync(dir, { withFileTypes: true });
    const sorter = (a, b) => a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });

    // 1. Process Folders
    const folders = items
        .filter(i => i.isDirectory() && !SYSTEM_FILES.includes(i.name))
        .sort(sorter)
        .map(i => {
            // Check for config.json inside the folder
            const configPath = path.join(dir, i.name, 'config.json');
            let icon = FOLDER_ICON; // Default
            let locked_channel = null;

            if (fs.existsSync(configPath)) {
                try {
                    const conf = JSON.parse(fs.readFileSync(configPath));
                    if (conf.icon) icon = conf.icon;
                    if (conf.locked_channel) locked_channel = conf.locked_channel;
                } catch (e) {}
            }

            return { name: i.name, icon: icon, locked_channel: locked_channel };
        });

    // 2. Process Files
    const files = items
        .filter(i => i.isFile() && i.name.endsWith('.json') && !SYSTEM_FILES.includes(i.name))
        .map(i => i.name.replace('.json', ''))
        .sort((a,b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));

    // Store in Memory
    const cacheKey = relPath.replace(/\\/g, '/'); 
    DIR_CACHE[cacheKey] = { folders, files };

    // Recursively scan
    for (const folderObj of folders) {
        rebuildCache(path.join(dir, folderObj.name), path.join(relPath, folderObj.name));
    }
}

// 🧹 Filename Cleaner Helper
function cleanFilename(rawName) {
    if (!rawName) return "Untitled_File";

    let name = rawName;

    // 1. Remove File Extension (e.g. .mkv, .mp4, .avi)
    name = name.replace(/\.[a-zA-Z0-9]{3,4}$/, "");

    // 2. Remove Common Junk (Website domains, channel tags)
    name = name.replace(/(www\.[^\s]+|@[^\s]+|t\.me\/[^\s]+)/gi, "");

    // 3. Remove Brackets [] and Parentheses () often used for release groups
    // e.g. "[ReleaseGrp] Movie (2023)" -> "Movie"
    name = name.replace(/\[.*?\]/g, "").replace(/\(.*?\)/g, "");

    // 4. Remove Quality & Codec Tags
    const tags = [
        "1080p", "720p", "480p", "2160p", "4k", "UHD",
        "HEVC", "x264", "x265", "10bit", "HDR", "BluRay", 
        "WEB-DL", "DVDRip", "H264", "AAC", "5.1", "Dual Audio",
        "Hindi", "Eng", "Sub", "Esub"
    ];
    const tagRegex = new RegExp(`\\b(${tags.join('|')})\\b`, 'gi');
    name = name.replace(tagRegex, "");

    // 5. Cleanup: Replace dots/underscores with spaces, trim multiple spaces
    name = name.replace(/[._]/g, " ").replace(/\s+/g, " ").trim();

    // 6. Final Sanitize for File System
    return sanitize(name);
}

// 🚀 INITIALIZE CACHE ON STARTUP
console.log("🔄 Building Directory Cache...");
rebuildCache();
console.log("✅ Cache Built!");

function getDirContents(relPath) {
    // Normalize path key
    const cacheKey = relPath.replace(/\\/g, '/');

    // Return from RAM if exists
    if (DIR_CACHE[cacheKey]) {
        return DIR_CACHE[cacheKey];
    }

    // Fallback: If path not in cache (shouldn't happen, but safety first), read from disk
    // This handles cases where a folder was just created but cache sync missed a beat
    const abs = getAbsPath(relPath);
    if (!fs.existsSync(abs)) return { folders: [], files: [] };
    
    // (We could trigger a cache rebuild here, but let's just return empty for safety)
    return { folders: [], files: [] };
}

// 🔍 Recursive Search Helper with Ranking
function findPathRecursively(dummyDir, query, dummyRel = '') {
    let results = [];
    const cleanQuery = String(query).toLowerCase().trim();
    
    // safe regex escape
    const escapeRegExp = (string) => string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); 
    const queryWords = cleanQuery.split(/\s+/).filter(w => w.length > 0);

    // Iterate over the Cache
    for (const [folderPath, content] of Object.entries(DIR_CACHE)) {
        
        const checkItem = (name, isDirectory) => {
            if (isIgnored(name)) return;
            
            const normName = name.toLowerCase();
            let score = 0;

            // 1. EXACT MATCH (Highest Priority)
            if (normName === cleanQuery) {
                score = 100;
            }
            // 2. STARTS WITH (High Priority)
            else if (normName.startsWith(cleanQuery)) {
                score = 80;
            }
            // 3. WORD BOUNDARY (Medium Priority) - Matches "Lost" inside "The Lost World"
            else if (new RegExp(`\\b${escapeRegExp(cleanQuery)}\\b`).test(normName)) {
                score = 60;
            }
            // 4. GENERAL CONTAINS (Low Priority) - Matches "Lost" inside "Aquaman..."
            else {
                const isMatch = queryWords.every(word => normName.includes(word));
                if (isMatch) score = 20;
            }

            if (score > 0) {
                // TIE BREAKER: Shorter names are usually better matches
                // e.g. "Lost" (4 chars) > "Lost in Space" (13 chars)
                const lengthPenalty = Math.min(10, (name.length - cleanQuery.length) * 0.1);
                
                results.push({ 
                    name: name, 
                    relPath: path.join(folderPath, name), 
                    isDirectory: isDirectory,
                    score: score - lengthPenalty 
                });
            }
        };

        // Check Folders
        if (content.folders) {
            content.folders.forEach(item => {
                const name = typeof item === 'string' ? item : item.name;
                checkItem(name, true);
            });
        }

        // Check Files
        if (content.files) {
            content.files.forEach(fileName => checkItem(fileName, false));
        }
    }

    // Sort Results: Highest Score First
    return results.sort((a, b) => b.score - a.score);
}

// 🛠️ Improved Link Parser
function parseLink(link) {
    if (!link) return null;
    
    // 1. Private Link: t.me/c/12345/100 -> Chat: -10012345, ID: 100
    const privateMatch = link.match(/(?:t\.me\/|telegram\.me\/)c\/(\d+)\/(\d+)/);
    if (privateMatch) {
        return { 
            chat_id: '-100' + privateMatch[1], 
            message_id: parseInt(privateMatch[2]) 
        };
    }
    
    // 2. Public Link: t.me/username/100 -> Chat: @username, ID: 100
    // Excludes 'c/' to prevent overlapping
    const publicMatch = link.match(/(?:t\.me\/|telegram\.me\/)(?!c\/)([^\/]+)\/(\d+)/);
    if (publicMatch) {
        return { 
            chat_id: '@' + publicMatch[1], 
            message_id: parseInt(publicMatch[2]) 
        };
    }
    
    return null;
}

function saveFileJson(relPath, fileName, fileData) {
    const abs = getAbsPath(relPath);
    if (!fs.existsSync(abs)) fs.mkdirSync(abs, { recursive: true });
    const finalName = sanitize(fileName) + '.json';

        // Only increment if new file
        if (!fs.existsSync(path.join(abs, finalName))) {
            metaData.total_files++;
            saveMeta();
        }
        
    fs.writeFileSync(path.join(abs, finalName), JSON.stringify(fileData, null, 2));
}

// 🔍 Tree Generation Helper
function generateTree(dir, prefix = '') {
    let output = '';
    
    if (!fs.existsSync(dir)) return '';

    // Read directory
    const items = fs.readdirSync(dir, { withFileTypes: true })
        .filter(i => !SYSTEM_FILES.includes(i.name)); // Skip system files

    // Sort: Folders first, then Files
    items.sort((a, b) => {
        if (a.isDirectory() && !b.isDirectory()) return -1;
        if (!a.isDirectory() && b.isDirectory()) return 1;
        return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });
    });

    const count = items.length;

    items.forEach((item, index) => {
        const isLast = index === count - 1;
        const connector = isLast ? '└── ' : '├── ';
        const childPrefix = isLast ? '    ' : '│   ';
        
        if (item.isDirectory()) {
            output += `${prefix}${connector}📂 ${item.name}\n`;
            // Recurse into subfolder
            output += generateTree(path.join(dir, item.name), prefix + childPrefix);
        } else if (item.name.endsWith('.json')) {
            // Remove .json extension for display
            output += `${prefix}${connector}📄 ${item.name.replace('.json', '')}\n`;
        }
    });

    return output;
}

// 🔍 Recursive Generator with Links & Shortcuts
function generateFullTree(dir, prefix = '') {
    let output = '';
    
    if (!fs.existsSync(dir)) return '';

    // 1. Load Deep Links for lookup
    // We do this inside the recursion, but efficiently it relies on file cache usually. 
    // For better performance, we could pass it as an argument, but this is safer for now.
    let linkMap = {}; 
    if (fs.existsSync(LINKS_FILE)) {
        const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));
        // Create a Reverse Map: Path -> ShortID
        Object.keys(links).forEach(id => {
            const entry = links[id];
            const p = entry.path || entry; // Handle legacy format
            linkMap[p] = id;
        });
    }

    const items = fs.readdirSync(dir, { withFileTypes: true })
        .filter(i => !SYSTEM_FILES.includes(i.name));

    // Sort: Folders first
    items.sort((a, b) => {
        if (a.isDirectory() && !b.isDirectory()) return -1;
        if (!a.isDirectory() && b.isDirectory()) return 1;
        return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });
    });

    const count = items.length;

    items.forEach((item, index) => {
        const isLast = index === count - 1;
        const connector = isLast ? '└── ' : '├── ';
        const childPrefix = isLast ? '    ' : '│   ';
        
        // Calculate Relative Path for Deep Link Lookup
        const absPath = path.join(dir, item.name);
        const relPath = path.relative(DATA_DIR, absPath).replace(/\\/g, '/'); // Normalize slashes
        
        // Check for Deep Link
        let deepLinkStr = '';
        if (linkMap[relPath]) {
            deepLinkStr = ` [🔗 t.me/${bot.botInfo.username}?start=${linkMap[relPath]}]`;
        }

        if (item.isDirectory()) {
            // 📂 FOLDER Check
            let extra = "";
            const previewPath = path.join(dir, item.name, 'preview.json');
            
            if (fs.existsSync(previewPath)) {
                try {
                    const pData = JSON.parse(fs.readFileSync(previewPath, 'utf8'));
                    const cid = pData.from_chat_id || pData.chat_id;
                    const link = getSourceLink(cid, pData.message_id);
                    extra = ` [👁️ Preview: ${link}]`;
                } catch (e) {}
            }

            output += `${prefix}${connector}📂 ${item.name}${deepLinkStr}${extra}\n`;
            output += generateFullTree(path.join(dir, item.name), prefix + childPrefix);

        } else if (item.name.endsWith('.json')) {
            // 📄 FILE Check
            let extra = "";
            let icon = "📄"; // Default icon

            try {
                const fPath = path.join(dir, item.name);
                const fData = JSON.parse(fs.readFileSync(fPath, 'utf8'));

                // 🔀 Check for Redirect
                if (fData.type === 'redirect') {
                    icon = "↪️";
                    extra = ` [➡️ Goes to: ${fData.target_path}]`;
                }
                // 🔗 Check for URL
                else if (fData.type === 'url') {
                    extra = ` [🌐 External: ${fData.url}]`;
                } 
                // 📥 Check for Source
                else {
                    const cid = fData.from_chat_id || fData.chat_id;
                    const link = getSourceLink(cid, fData.message_id);
                    extra = ` [📥 Source: ${link}]`;
                }
            } catch (e) {}

            output += `${prefix}${connector}${icon} ${item.name.replace('.json', '')}${deepLinkStr}${extra}\n`;
        }
    });

    return output;
}

// 🌳 NEW COMMAND: /fulltree
bot.command('fulltree', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const currentRel = userPaths[ctx.chat.id] || ''; 
    const startDir = getAbsPath(currentRel);
    const displayName = currentRel ? `📂 ${currentRel}` : '📦 ROOT';

    await ctx.reply(`🕵️ Generating Detailed Tree (with links) for: "${displayName}"...`);

    try {
        const treeBody = generateFullTree(startDir);
        const treeStructure = `${displayName} (Detailed View)\n${treeBody}`;
        
        // Since links make lines long, we default to sending a file
        const safeName = (currentRel || 'root').replace(/[\/\\|]/g, '_');
        const reportPath = path.join(DATA_DIR, `${safeName}_full_structure.txt`);
        
        fs.writeFileSync(reportPath, treeStructure);
        
        await ctx.replyWithDocument(
            { source: reportPath, filename: `${safeName}_full_tree.txt` }, 
            { caption: `📂 Detailed structure for "${displayName}"` }
        );
        
        fs.unlinkSync(reportPath); // Cleanup

    } catch (e) {
        return ctx.reply(`❌ Error generating tree: ${e.message}`);
    }
});

// 📦 EXPORT / BACKUP COMMAND
bot.command('export', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const timestamp = new Date().toISOString().replace(/[:T.]/g, '-').slice(0, 19);
    const zipName = `backup_${timestamp}.zip`;
    const outputPath = path.join(__dirname, zipName); // Save temporarily in root
    
    // Create Write Stream
    const output = fs.createWriteStream(outputPath);
    const archive = archiver('zip', { zlib: { level: 9 } }); // Max compression

    const statusMsg = await ctx.reply("📦 Compressing database... Please wait.");

    // Event: Finished Zipping
    output.on('close', async () => {
        try {
            const stats = fs.statSync(outputPath);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);

            await ctx.replyWithDocument(
                { source: outputPath, filename: zipName }, 
                { caption: `✅ <b>Backup Complete</b>\n\n💾 Size: ${fileSizeInMB} MB\n📂 Total Files: ${metaData.total_files}`, parse_mode: 'HTML' }
            );
            
            // Clean up UI
            await ctx.telegram.deleteMessage(ctx.chat.id, statusMsg.message_id).catch(()=>{});
            
        } catch (e) {
            await ctx.reply(`❌ Error sending file: ${e.message}`);
        } finally {
            // Delete local temp file
            if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
        }
    });

    // Event: Errors
    archive.on('error', (err) => {
        ctx.reply(`❌ Backup Failed: ${err.message}`);
    });

    // Pipe archive data to the file
    archive.pipe(output);

    // Append the entire DATA_DIR to the zip
    // "false" means it won't create a 'data' subfolder inside the zip, it puts contents at root. 
    // Change to 'data' if you want them inside a folder.
    archive.directory(DATA_DIR, 'data'); 

    await archive.finalize();
});

// 🕵️ ADMIN: View User Activity
bot.command('activity', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const args = ctx.message.text.replace('/activity', '').trim();
    const logs = JSON.parse(fs.readFileSync(ACTIVITY_FILE, 'utf8'));

    // 1. Export Mode
    if (args === 'export') {
        return ctx.replyWithDocument({ source: ACTIVITY_FILE, filename: 'activity_logs.json' });
    }

    // 2. Specific User Mode
    if (args.match(/^\d+$/)) {
        const uid = parseInt(args);
        const userLogs = logs.filter(l => l.user_id === uid).slice(0, 15);
        
        if (userLogs.length === 0) return ctx.reply("❌ No activity found for this User ID.");

        let msg = `🕵️ <b>Activity for ID:</b> <code>${uid}</code>\n\n`;
        userLogs.forEach(l => {
            const time = new Date(l.time).toLocaleString('en-GB', { hour: '2-digit', minute:'2-digit', day:'numeric', month:'short' });
            msg += `⏰ <code>${time}</code>\n└ <b>${l.action}:</b> ${sanitize(l.detail)}\n\n`;
        });
        return ctx.replyWithHTML(msg);
    }

    // 3. Global Mode (Last 15 actions)
    let msg = `🕵️ <b>Recent User Activity</b>\n(Showing last 15 actions)\n\n`;
    const recent = logs.slice(0, 15);
    
    recent.forEach(l => {
        const time = new Date(l.time).toLocaleTimeString('en-GB', { hour: '2-digit', minute:'2-digit' });
        msg += `👤 <a href="tg://user?id=${l.user_id}">${sanitize(l.name)}</a> (${time})\n`;
        msg += `└ <b>${l.action}:</b> ${sanitize(l.detail)}\n`;
    });

    msg += `\n<i>Usage: /activity export OR /activity &lt;UserID&gt;</i>`;
    return ctx.replyWithHTML(msg);
});

// 🛠️ Helper to generate Telegram Links
function getSourceLink(chatId, msgId) {
    if (!chatId || !msgId) return "N/A";
    const cid = String(chatId);
    
    // Private Channel (-100xxxx -> /c/xxxx/id)
    if (cid.startsWith('-100')) {
        return `https://t.me/c/${cid.replace('-100', '')}/${msgId}`;
    }
    // Public Channel/Chat (@username -> /username/id)
    if (cid.startsWith('@')) {
        return `https://t.me/${cid.replace('@', '')}/${msgId}`;
    }
    return `ID:${cid}/${msgId}`;
}

// 🌐 Recursive HTML Generator
function generateHtmlTree(dir) {
    let html = '<ul>';
    
    if (!fs.existsSync(dir)) return '';

    // Load Links
    let linkMap = {}; 
    if (fs.existsSync(LINKS_FILE)) {
        const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));
        Object.keys(links).forEach(id => {
            const entry = links[id];
            linkMap[(entry.path || entry)] = id;
        });
    }

    const items = fs.readdirSync(dir, { withFileTypes: true })
        .filter(i => !SYSTEM_FILES.includes(i.name));

    items.sort((a, b) => {
        if (a.isDirectory() && !b.isDirectory()) return -1;
        if (!a.isDirectory() && b.isDirectory()) return 1;
        return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' });
    });

    items.forEach((item) => {
        const absPath = path.join(dir, item.name);
        const relPath = path.relative(DATA_DIR, absPath).replace(/\\/g, '/');
        
        // Deep Link Badge
        let deepLinkHtml = '';
        if (linkMap[relPath]) {
            const url = `https://t.me/${bot.botInfo.username}?start=${linkMap[relPath]}`;
            deepLinkHtml = ` <a href="${url}" target="_blank" class="tag deeplink">[🔗 Share Link]</a>`;
        }

        if (item.isDirectory()) {
            // 📂 FOLDER
            let previewHtml = '';
            const previewPath = path.join(dir, item.name, 'preview.json');
            
            if (fs.existsSync(previewPath)) {
                try {
                    const pData = JSON.parse(fs.readFileSync(previewPath, 'utf8'));
                    const cid = pData.from_chat_id || pData.chat_id;
                    const link = getSourceLink(cid, pData.message_id);
                    if (link) previewHtml = ` <a href="${link}" target="_blank" class="tag preview">[👁️ Preview]</a>`;
                } catch (e) {}
            }

            html += `<li><span class="folder">📂 ${item.name}</span>${deepLinkHtml}${previewHtml}`;
            html += generateHtmlTree(path.join(dir, item.name)); 
            html += `</li>`;

        } else if (item.name.endsWith('.json')) {
            // 📄 FILE
            let extraHtml = '';
            let icon = '📄';
            let cssClass = 'file';

            try {
                const fPath = path.join(dir, item.name);
                const fData = JSON.parse(fs.readFileSync(fPath, 'utf8'));

                if (fData.type === 'redirect') {
                    icon = '↪️';
                    cssClass = 'redirect';
                    extraHtml = ` <span class="tag redirect">[➡️ To: ${fData.target_path}]</span>`;
                }
                else if (fData.type === 'url') {
                    extraHtml = ` <a href="${fData.url}" target="_blank" class="tag url">[🌐 External]</a>`;
                } 
                else {
                    const cid = fData.from_chat_id || fData.chat_id;
                    const link = getSourceLink(cid, fData.message_id);
                    if (link) extraHtml = ` <a href="${link}" target="_blank" class="tag source">[📥 Source]</a>`;
                }
            } catch (e) {}

            html += `<li><span class="${cssClass}">${icon} ${item.name.replace('.json', '')}</span>${deepLinkHtml}${extraHtml}</li>`;
        }
    });

    html += '</ul>';
    return html;
}

// ⚡ PAGINATION LOGIC
function buildKeyboard(relPath, page = 0) {
    const { folders, files } = getDirContents(relPath);
    
    // UPDATED: Use the icon from the object
    const folderBtns = folders.map(f => `${f.icon} ${f.name}`);
    const fileBtns = files.map(f => `${FILE_ICON} ${f}`);
    const allItems = [...folderBtns, ...fileBtns];

    const totalPages = Math.ceil(allItems.length / ITEMS_PER_PAGE);
    
    const start = page * ITEMS_PER_PAGE;
    const end = start + ITEMS_PER_PAGE;
    const pageItems = allItems.slice(start, end);

    const rows = [];
    for (let i = 0; i < pageItems.length; i += 2) rows.push(pageItems.slice(i, i + 2));

    const pagRow = [];
    if (page > 0) pagRow.push('⬅️ Prev');
    if (page < totalPages - 1) pagRow.push('Next ➡️');
    if (pagRow.length > 0) rows.push(pagRow);

    // ⭐ NEW: Suggested Button Row
    rows.push(['✨ Suggested']);

    const navRow = [];
    if (relPath && relPath !== '') navRow.push('⬅️ Back');
    navRow.push('🏠 Index');
    navRow.push('ℹ️ Help');
    rows.push(navRow);

    return Markup.keyboard(rows).resize();
}

// 🎲 Helper: Get a Random Safe Item
function getRandomSuggestion(relPath) {
    // 1. Check for Admin Manual Suggestion in config.json
    const absPath = getAbsPath(relPath);
    const configPath = path.join(absPath, 'config.json');
    
    if (fs.existsSync(configPath)) {
        try {
            const conf = JSON.parse(fs.readFileSync(configPath));
            if (conf.suggestions && conf.suggestions.length > 0) {
                // Pick one of the admin set suggestions
                const pick = conf.suggestions[Math.floor(Math.random() * conf.suggestions.length)];
                return { name: pick, path: path.join(relPath, pick) };
            }
        } catch (e) {}
    }

    // 2. If no admin setting, find a Random item recursively
    // We use the DIR_CACHE to avoid hitting the disk too hard
    let candidates = [];
    
    const collectCandidates = (currentRel) => {
        const cacheKey = currentRel.replace(/\\/g, '/');
        const content = DIR_CACHE[cacheKey];
        if (!content) return;

        // A. Add Files (Always safe if we are in this folder)
        if (content.files) {
            content.files.forEach(f => {
                candidates.push({ name: f, path: path.join(currentRel, f), type: 'file' });
            });
        }

        // B. Traverse Folders (Check for Locks)
        if (content.folders) {
            content.folders.forEach(fObj => {
                // 🚫 SECURITY: Skip Locked Folders
                if (fObj.locked_channel) return; 

                // Add the folder itself as a candidate
                candidates.push({ name: fObj.name, path: path.join(currentRel, fObj.name), type: 'folder' });

                // Recurse deeper
                collectCandidates(path.join(currentRel, fObj.name));
            });
        }
    };

    // Start collection from current location
    collectCandidates(relPath);

    if (candidates.length === 0) return null;
    
    // Pick random
    return candidates[Math.floor(Math.random() * candidates.length)];
}

async function cleanReply(ctx, text, extra) {
    // Delete the user's immediate input (keep this if you want instant cleanup of commands)
    try { if (ctx.message) await ctx.deleteMessage().catch(() => {}); } catch (e) {}
    
    // Send the bot's response
    const sent = await ctx.reply(text, extra);
    
    // USE DYNAMIC TIMER HERE 👇
    setTimeout(() => {
        ctx.telegram.deleteMessage(ctx.chat.id, sent.message_id).catch(()=>{});
    }, metaData.timers.bot_msg);
    
    return sent;
}

// ---------- Help Logic (Separated) ----------
const sendHelp = (ctx) => {
    if (isAdmin(ctx)) {
        // ADMIN VIEW
        return ctx.replyWithHTML(
            `<b>🔧 Admin Dashboard</b>\n` +
            `Users: <code>${getStats().users}</code> | Files: <code>${getStats().files}</code>\n\n` +
            
            `<b>📂 Content Management:</b>\n` +
            `<i>Tip: Forward a file to the bot to save it quickly!</i>\n` +
            `/mkdir &lt;Name&gt; - Create Folder (Use | for multiple)\n` +
            `/add &lt;Name&gt; &lt;Link&gt; - Add Single File\n` +
            `/link &lt;SourcePath&gt; | &lt;ShortcutName&gt; - Redirect" or "Shortcut" system.\n` +
            `/addlink &lt;Name&gt; | &lt;URL&gt; - Add External Link\n` +
            `/getlink - Generate a Shareable Link\n` +
            `/linklist - View all active links\n` +  // <--- ADDED
            `/revoke &lt;ID&gt; - Delete a link\n` + // <--- ADDED
            `/del &lt;Name&gt; - Delete Item\n` +
            `/rn &lt;Old&gt; | &lt;New&gt; - Rename Item\n\n` +

            `<b>📋 Clipboard & Shortcuts:</b>\n` +
            `/mv &lt;Name&gt; - <b>Cut</b> item (Prepare to Move)\n` +
            `/pst - <b>Paste</b> item (Execute Move)\n` +
            `/cplink &lt;Name&gt; - <b>Copy</b> Link (Prepare Shortcut)\n` +
            `/pstlink - <b>Paste</b> Shortcut (Create Link Button)\n` +
            `/clrcp - Clear Clipboard\n\n` +

            `<b>📦 Batch Operations:</b>\n` +
            `/batch &lt;Pfx&gt; &lt;Start&gt; &lt;Link1&gt; &lt;Link2&gt; - Sequence Add\n` +
            `/batchlist - Bulk Add (Supports Multi-line or Pipe | format)\n\n` +
            
            `<b>🎨 Folder Tools:</b>\n` +
            `/seticon &lt;Icon&gt; - Set Folder Icon\n` +
            `/lock &lt;Channel&gt; - Lock Folder (Restrict Access)\n` +
            `/setpreview &lt;Link&gt; - Set Folder Thumbnail\n` +
            `/delpreview - Remove Folder Thumbnail\n` +
            `/suggest &lt;Name&gt; - Set "Suggested" item for this folder\n` +
            `/clearsuggest - Reset suggestion to Random\n\n` +
            
            `<b>🔐 Force Subscribe:</b>\n` +
            `/fson - Turn Force Sub ON\n` +
            `/fsoff - Turn Force Sub OFF\n` +
            `/addfs &lt;Channel&gt; - Add Channel (Max 3)\n` +
            `/delfs &lt;Channel&gt; - Remove Channel\n` +
            `/fslist - View Settings\n\n` +

            `<b>⚙️ System:</b>\n` +
            `/activity - View recent user logs\n` + 
            `/activity &lt;ID&gt; - View specific user logs\n` +
            `/broadcast &lt;Msg&gt; - Message all users\n` +
            `/export - Backup entire database (ZIP)\n` +
            `/pwd - Show current path\n` +
            `/settimer &lt;type&gt; &lt;mins&gt; - Set auto-delete time\n` +
            `  (Types: file, bot, user)\n` +
            `/timers - View current timer settings\n` +
            `/tree - View full folder structure\n` +
            `/fulltree - View structure with source links\n` +
            `/fullhtree - Get HTML map of structure\n` +
            `/refresh - Reload changes from disk\n` +
            `/setwelcome &lt;Msg&gt; - Set start message`
        );
    } else {
        // USER VIEW
        return ctx.replyWithHTML(
            "<b>📖 User Guide</b>\n\n" +
            "<b>1. 📂 Browse:</b> Tap the buttons to navigate folders.\n" +
            "<b>2. ✨ Suggested:</b> Tap to get a random recommendation!\n" +
            "<b>3. 📄 Download:</b> Tap a file to receive the video.\n" +
            "<b>4. 🔍 Search:</b> Type the name of any Movie or Series.\n" +
            "<b>5. 📩 Request:</b> Use <code>/request Name</code> to ask the admin."
        );
    }
};

// Track users on every message
bot.use(async (ctx, next) => {
    trackUser(ctx);
    await next();
});

// ---------- Core Commands ----------
bot.command('start', async (ctx) => {
    // 1. Check for Deep Link Payload
    const payload = ctx.message.text.split(' ')[1];

    if (payload) {
        const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));
        const linkData = links[payload];

        if (linkData) {
            const targetRel = linkData.path || linkData;
            const absTarget = getAbsPath(targetRel);

            // A. Handle FOLDER Link
            if (fs.existsSync(absTarget) && fs.lstatSync(absTarget).isDirectory()) {
                
                // 🔒 CHECK LOCK BEFORE OPENING
                const isAllowed = await checkFolderLock(ctx, absTarget);
                if (!isAllowed) return; // Stop if locked

                userPaths[ctx.chat.id] = targetRel;
                userPages[ctx.chat.id] = 0;
                
                logUserAction(ctx, 'DEEP_LINK_FOLDER', targetRel);
                return cleanReply(ctx, `📂 Opened via Link: ${path.basename(targetRel)}`, buildKeyboard(targetRel, 0));
            }

            // B. Handle FILE Link
            // Note: This checks if the FILE'S PARENT FOLDER is locked
            const targetFile = absTarget.endsWith('.json') ? absTarget : absTarget + '.json';
            
            if (fs.existsSync(targetFile)) {
                // Check lock of the parent folder
                const parentDir = path.dirname(targetFile);
                const isAllowed = await checkFolderLock(ctx, parentDir);
                if (!isAllowed) return; // Stop if locked

                logUserAction(ctx, 'DEEP_LINK_FILE', targetRel);
                return sendFileToUser(ctx, targetFile, path.basename(targetRel));
            }

            return ctx.reply("❌ This link points to a file/folder that has been deleted.");
        }
        return ctx.reply("❌ Invalid or expired link.");
    }

    // --- STANDARD START LOGIC (No changes below this line) ---
    userPaths[ctx.chat.id] = '';
    
    let msg = `${metaData.welcome_msg}\n\n`;
    if (isAdmin(ctx)) {
        const stats = getStats();
        msg += `🔐 <b>Admin Panel:</b>\n`;
        msg += `👥 Total Users: <code>${stats.users}</code>\n`;
        msg += `💾 Total Files: <code>${stats.files}</code>\n`;
        msg += `--------------------------\n`;
    }
    msg += `<i>👇 Select a category below or type a movie/series name to search.</i>`;

    return cleanReply(ctx, msg, { parse_mode: 'HTML', ...buildKeyboard('') });
});

// Admin command to change welcome message
bot.command('setwelcome', (ctx) => {
    if (!isAdmin(ctx)) return;
    const newMsg = ctx.message.text.replace('/setwelcome', '').trim();
    if (!newMsg) return ctx.reply("❌ Usage: /setwelcome <New Message>");
    
    metaData.welcome_msg = newMsg;
    saveMeta();
    return ctx.reply("✅ Welcome message updated!");
});

bot.hears('🏠 Index', (ctx) => {
    userPaths[ctx.chat.id] = '';
    userPages[ctx.chat.id] = 0;
    return cleanReply(ctx, "🏠 Home Directory", buildKeyboard('', 0));
});

bot.hears('⬅️ Back', (ctx) => {
    const current = userPaths[ctx.chat.id] || '';
    if (!current) return cleanReply(ctx, "Already at Root.", buildKeyboard('', 0));
    const parts = current.split(path.sep);
    parts.pop(); 
    const newPath = parts.join(path.sep);
    userPaths[ctx.chat.id] = newPath;
    userPages[ctx.chat.id] = 0; // Reset Page on Back
    return cleanReply(ctx, `📂 Path: ${newPath || 'Home'}`, buildKeyboard(newPath, 0));
});
// ⚡ PAGINATION HANDLERS
bot.hears('Next ➡️', (ctx) => {
    const currentRel = userPaths[ctx.chat.id] || '';
    const currentPage = userPages[ctx.chat.id] || 0;
    userPages[ctx.chat.id] = currentPage + 1;
    return cleanReply(ctx, `📄 Page ${userPages[ctx.chat.id] + 1}`, buildKeyboard(currentRel, userPages[ctx.chat.id]));
});

bot.hears('⬅️ Prev', (ctx) => {
    const currentRel = userPaths[ctx.chat.id] || '';
    const currentPage = userPages[ctx.chat.id] || 0;
    if (currentPage > 0) userPages[ctx.chat.id] = currentPage - 1;
    return cleanReply(ctx, `📄 Page ${userPages[ctx.chat.id] + 1}`, buildKeyboard(currentRel, userPages[ctx.chat.id]));
});

// ✨ Handle "Suggested" Button (Fixed: Now with Auto-Delete)
bot.hears('✨ Suggested', async (ctx) => {
    const currentRel = userPaths[ctx.chat.id] || '';
    
    // Get a suggestion
    const suggestion = getRandomSuggestion(currentRel);

    if (!suggestion) {
         // 👇 ADD THIS LINE
        logUserAction(ctx, 'SUGGESTION', `Clicked in ${currentRel || 'Root'}`);
        return ctx.reply("🤷 No suggestions found in this section.");
    }

    // --- Path Formatting ---
    let parentDir = path.dirname(suggestion.path);
    let displayFrom = (parentDir === '.' || parentDir === '') 
        ? 'Main Collection' 
        : parentDir.split(path.sep).join(' ﹥ ');

    // Reply with Context
    await ctx.reply(
        `🎲 I suggest: <b>${suggestion.name}</b>\n` +
        `📍 From: 📂 <i>${displayFrom}</i>`, 
        { parse_mode: 'HTML' }
    );

    const absPath = getAbsPath(suggestion.path);
    
    // 1. 📂 If it is a FOLDER
    if (suggestion.type === 'folder' || fs.existsSync(absPath)) {
         userPaths[ctx.chat.id] = suggestion.path;
         userPages[ctx.chat.id] = 0;
         
         // Check for preview
         const pFile = path.join(absPath, 'preview.json');
         if (fs.existsSync(pFile)) {
             try {
                const pData = JSON.parse(fs.readFileSync(pFile));
                const sid = pData.from_chat_id || pData.chat_id;
                await ctx.telegram.copyMessage(ctx.chat.id, sid, pData.message_id, buildKeyboard(suggestion.path, 0));
                return;
             } catch(e) {}
         }
         
         // Get folder icon from cache
         const cacheKey = currentRel.replace(/\\/g, '/');
         const parentCache = DIR_CACHE[cacheKey];
         let icon = FOLDER_ICON;
         if(parentCache) {
             const fObj = parentCache.folders.find(f => f.name === suggestion.name);
             if(fObj) icon = fObj.icon;
         }
         
         return cleanReply(ctx, `${icon} ${suggestion.name}`, buildKeyboard(suggestion.path, 0));
    }
    
    // 2. 📄 If it is a FILE (Updated with Timer & Delete Button)
    const filePath = absPath.endsWith('.json') ? absPath : absPath + '.json';
    if (fs.existsSync(filePath)) {
        try {
            const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

            // 🅰️ TYPE: URL / LINK
            if (data.type === 'url') {
                const sentMsg = await ctx.replyWithHTML(
                    `📄 <b>${suggestion.name}</b>\n\n🔗 <i>This file is hosted externally. Click below to access it.</i>`,
                    Markup.inlineKeyboard([[Markup.button.url('🚀 Open Link', data.url)]])
                );
                // Auto-delete URL message
                setTimeout(() => { 
                    ctx.telegram.deleteMessage(ctx.chat.id, sentMsg.message_id).catch(()=>{}); 
                }, metaData.timers.file);
                return;
            }

            // 🅱️ TYPE: FILE COPY (With Timer & Delete Button)
            const sid = data.from_chat_id || data.chat_id;
            
            // 1. Send Timer Message
            const fileMins = Math.floor(metaData.timers.file / 60000);
            const timerMsg = await ctx.reply(`⏳ Loading: ${suggestion.name}\n🗑️ Auto-deletes in ${fileMins} mins...`);

            // 2. Send File with "Delete / Close" button linked to timer
            const sentMsg = await ctx.telegram.copyMessage(ctx.chat.id, sid, data.message_id, {
                caption: data.caption || '',
                ...Markup.inlineKeyboard([[
                    Markup.button.callback('❌ Delete / Close', `del_msg:${timerMsg.message_id}`)
                ]])
            });

            // 3. Set Auto-Delete Timer
            setTimeout(async () => { 
                try { 
                    await ctx.telegram.deleteMessage(ctx.chat.id, sentMsg.message_id); 
                    await ctx.telegram.deleteMessage(ctx.chat.id, timerMsg.message_id); 
                } catch (e) {} 
            }, metaData.timers.file);

            return;

        } catch(e) {
            console.error("Suggestion Error:", e);
        }
    }
});

// Help Button Handler
bot.hears('ℹ️ Help', (ctx) => sendHelp(ctx));

// Keep /help command as a backup
bot.command('help', (ctx) => sendHelp(ctx));

// ---------- Force Subscribe Middleware ----------
bot.use(async (ctx, next) => {
    // 1. Bypass checks for: Admins, callbacks (handled separately), or if FS is OFF
    if (!ctx.from || isAdmin(ctx) || !metaData.force_sub.is_enabled) return next();
    if (ctx.callbackQuery) return next(); // Allow buttons to work so they can click "I Joined"

    const channels = metaData.force_sub.channels;
    if (channels.length === 0) return next(); // No channels set

    // 2. Check Membership
    let notJoined = [];
    for (const ch of channels) {
        try {
            const member = await ctx.telegram.getChatMember(ch, ctx.from.id);
            if (['left', 'kicked'].includes(member.status)) {
                notJoined.push(ch);
            }
        } catch (e) {
            // If bot is not admin in channel, it can't check. 
            // We assume they haven't joined to be safe, or log error.
            console.log(`FS Error for ${ch}: ${e.message}`);
            notJoined.push(ch);
        }
    }

    // 3. If User is missing channels, STOP them.
    if (notJoined.length > 0) {
        const buttons = notJoined.map((ch, i) => [Markup.button.url(`📢 Join Channel ${i + 1}`, `https://t.me/${ch.replace('@', '')}`)]);
        buttons.push([Markup.button.callback('✅ I Have Joined', 'check_subscription')]);

        return ctx.reply(
            `⚠️ <b>Access Denied</b>\n\nTo use this bot, you must join our update channels first.`,
            { parse_mode: 'HTML', ...Markup.inlineKeyboard(buttons) }
        );
    }

    // 4. User is safe, proceed
    await next();
});

// Callback for the "I Have Joined" button
bot.action('check_subscription', async (ctx) => {
    const channels = metaData.force_sub.channels;
    let allJoined = true;

    for (const ch of channels) {
        try {
            const member = await ctx.telegram.getChatMember(ch, ctx.from.id);
            if (['left', 'kicked'].includes(member.status)) allJoined = false;
        } catch (e) { allJoined = false; }
    }

    if (allJoined) {
        await ctx.deleteMessage().catch(() => {});
        await ctx.answerCbQuery("✅ Access Granted!");
        return ctx.reply("👋 Welcome back! Type /start or continue browsing.");
    } else {
        await ctx.answerCbQuery("❌ You still haven't joined all channels!", { show_alert: true });
    }
});

// ---------- Force Subscribe Management ----------

bot.command('fson', (ctx) => {
    if (!isAdmin(ctx)) return;
    metaData.force_sub.is_enabled = true;
    saveMeta();
    return ctx.reply("🔐 Force Subscribe is now **ENABLED**.");
});

bot.command('fsoff', (ctx) => {
    if (!isAdmin(ctx)) return;
    metaData.force_sub.is_enabled = false;
    saveMeta();
    return ctx.reply("🔓 Force Subscribe is now **DISABLED**.");
});

bot.command('addfs', async (ctx) => {
    if (!isAdmin(ctx)) return;
    const input = ctx.message.text.replace('/addfs', '').trim();
    
    // Validate Input
    if (!input) return ctx.reply("❌ Usage: /addfs @ChannelUsername\n(Make sure to add the bot as Admin in that channel first!)");
    
    // Extract Username (handles https://t.me/username and @username)
    let username = input.split('/').pop();
    if (!username.startsWith('@')) username = '@' + username;

    // Check Limit
    if (metaData.force_sub.channels.length >= 3) {
        return ctx.reply("❌ Limit Reached: You can only add up to 3 channels.");
    }

    // Check Duplicate
    if (metaData.force_sub.channels.includes(username)) {
        return ctx.reply("⚠️ Channel already in list.");
    }

    // Verify Bot is Admin there
    try {
        const chat = await ctx.telegram.getChat(username);
        const me = await ctx.telegram.getChatMember(chat.id, ctx.botInfo.id);
        if (me.status !== 'administrator') {
            return ctx.reply(`⚠️ I am not an Admin in ${username}. Please promote me first, then try again.`);
        }
    } catch (e) {
        return ctx.reply(`❌ Error: Could not find ${username}. Is the username correct?`);
    }

    // Save
    metaData.force_sub.channels.push(username);
    saveMeta();
    return ctx.reply(`✅ Added ${username} to Force Sub list.`);
});

bot.command('delfs', (ctx) => {
    if (!isAdmin(ctx)) return;
    const target = ctx.message.text.replace('/delfs', '').trim();
    if (!target) return ctx.reply("❌ Usage: /delfs @ChannelUsername");

    let cleanTarget = target.split('/').pop();
    if (!cleanTarget.startsWith('@')) cleanTarget = '@' + cleanTarget;

    const initialLen = metaData.force_sub.channels.length;
    metaData.force_sub.channels = metaData.force_sub.channels.filter(c => c !== cleanTarget);
    saveMeta();

    if (metaData.force_sub.channels.length < initialLen) {
        return ctx.reply(`🗑️ Removed ${cleanTarget}.`);
    } else {
        return ctx.reply("❌ Channel not found in list.");
    }
});

bot.command('fslist', (ctx) => {
    if (!isAdmin(ctx)) return;
    const { is_enabled, channels } = metaData.force_sub;
    
    let msg = `<b>🔐 Force Subscribe Settings</b>\n`;
    msg += `Status: <b>${is_enabled ? '🟢 ON' : '🔴 OFF'}</b>\n\n`;
    msg += `<b>Channels (${channels.length}/3):</b>\n`;
    
    if (channels.length === 0) msg += "<i>No channels set.</i>";
    else channels.forEach((c, i) => msg += `${i+1}. ${c}\n`);

    return ctx.replyWithHTML(msg);
});

// ---------- Folder Customization ----------

// 1. Set Folder Icon
bot.command('seticon', (ctx) => {
    if (!isAdmin(ctx)) return;
    const icon = ctx.message.text.replace('/seticon', '').trim();
    if (!icon) return ctx.reply("❌ Usage: /seticon 🎬");

    const currentRel = userPaths[ctx.chat.id] || '';
    if (!currentRel) return ctx.reply("❌ You cannot set an icon for Root. Navigate to a folder first.");

    const absPath = getAbsPath(currentRel);
    const configPath = path.join(absPath, 'config.json');

    // Update Config
    let conf = {};
    if (fs.existsSync(configPath)) conf = JSON.parse(fs.readFileSync(configPath));
    conf.icon = icon;
    
    fs.writeFileSync(configPath, JSON.stringify(conf, null, 2));
    
    // Refresh Cache so it updates immediately
    // We need to rebuild parent cache to see the icon change
    rebuildCache(); 
    
    return ctx.reply(`✅ Icon changed to ${icon}. (You may need to go Back and Forward to see it)`);
});

// 2. Lock Folder to Channel
bot.command('lock', (ctx) => {
    if (!isAdmin(ctx)) return;
    const channel = ctx.message.text.replace('/lock', '').trim();
    
    const currentRel = userPaths[ctx.chat.id] || '';
    if (!currentRel) return ctx.reply("❌ Navigate to the folder you want to lock first.");

    const absPath = getAbsPath(currentRel);
    const configPath = path.join(absPath, 'config.json');

    let conf = {};
    if (fs.existsSync(configPath)) conf = JSON.parse(fs.readFileSync(configPath));
    
    if (!channel) {
        // If empty, remove lock
        delete conf.locked_channel;
        fs.writeFileSync(configPath, JSON.stringify(conf, null, 2));
        rebuildCache();
        return ctx.reply("🔓 Folder unlocked.");
    }

    // Set Lock
    conf.locked_channel = channel.startsWith('@') ? channel : '@' + channel;
    fs.writeFileSync(configPath, JSON.stringify(conf, null, 2));
    rebuildCache();

    return ctx.reply(`🔒 Folder locked! Users must join ${conf.locked_channel} to open it.`);
});

// ---------- Timer Management ----------

bot.command('settimer', (ctx) => {
    if (!isAdmin(ctx)) return;
    const args = ctx.message.text.trim().split(/\s+/);

    // Usage: /settimer <type> <minutes>
    if (args.length !== 3) {
        return ctx.reply("❌ Usage: /settimer <file|bot|user> <minutes>\nExample: /settimer file 10");
    }

    const type = args[1].toLowerCase(); // file, bot, or user
    const mins = parseInt(args[2]);

    if (isNaN(mins) || mins < 1) return ctx.reply("❌ Time must be a number (minutes).");

    const ms = mins * 60 * 1000; // Convert to milliseconds

    if (type === 'file') {
        metaData.timers.file = ms;
        ctx.reply(`✅ <b>File</b> auto-delete set to ${mins} minutes.`);
    } else if (type === 'bot') {
        metaData.timers.bot_msg = ms;
        ctx.reply(`✅ <b>Bot Message</b> auto-delete set to ${mins} minutes.`);
    } else if (type === 'user') {
        metaData.timers.user_msg = ms;
        ctx.reply(`✅ <b>User Message</b> auto-delete set to ${mins} minutes.`);
    } else {
        return ctx.reply("❌ Invalid type. Use: file, bot, or user.");
    }
    
    saveMeta();
});

bot.command('timers', (ctx) => {
    if (!isAdmin(ctx)) return;
    const { file, bot_msg, user_msg } = metaData.timers;
    
    // Convert ms back to minutes for display
    const toMin = (ms) => Math.floor(ms / 60000);

    return ctx.replyWithHTML(
        `<b>⏱️ Auto-Delete Settings</b>\n\n` +
        `📂 <b>Files:</b> ${toMin(file)} mins\n` +
        `🤖 <b>Bot Msgs:</b> ${toMin(bot_msg)} mins\n` +
        `👤 <b>User Msgs:</b> ${toMin(user_msg)} mins`
    );
});

// Set a manual suggestion for the current folder
bot.command('suggest', (ctx) => {
    if (!isAdmin(ctx)) return;
    const name = ctx.message.text.replace('/suggest', '').trim();
    
    if (!name) return ctx.reply("❌ Usage: /suggest <Folder or Filename>");

    const currentRel = userPaths[ctx.chat.id] || '';
    const absPath = getAbsPath(currentRel);
    
    // Verify it exists
    const targetPath = path.join(absPath, name);
    const targetFile = path.join(absPath, name + '.json');
    
    if (!fs.existsSync(targetPath) && !fs.existsSync(targetFile)) {
        return ctx.reply("❌ That item does not exist in the current folder.");
    }

    // Save to config.json
    const configPath = path.join(absPath, 'config.json');
    let conf = {};
    if (fs.existsSync(configPath)) conf = JSON.parse(fs.readFileSync(configPath));
    
    if (!conf.suggestions) conf.suggestions = [];
    conf.suggestions.push(name); // Add to list (supports multiple)
    
    fs.writeFileSync(configPath, JSON.stringify(conf, null, 2));
    return ctx.reply(`✅ Added "${name}" to suggestions for this folder.`);
});

// Clear all suggestions for current folder
bot.command('clearsuggest', (ctx) => {
    if (!isAdmin(ctx)) return;
    const currentRel = userPaths[ctx.chat.id] || '';
    const absPath = getAbsPath(currentRel);
    const configPath = path.join(absPath, 'config.json');

    if (fs.existsSync(configPath)) {
        const conf = JSON.parse(fs.readFileSync(configPath));
        delete conf.suggestions;
        fs.writeFileSync(configPath, JSON.stringify(conf, null, 2));
        return ctx.reply("✅ Suggestions cleared for this folder. (Now using Random mode)");
    }
    return ctx.reply("ℹ️ No config found.");
});

// ---------- Admin tree ----------
bot.command('tree', async (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Get Current Location (Context Aware)
    const currentRel = userPaths[ctx.chat.id] || ''; 
    const startDir = getAbsPath(currentRel);
    
    // Set a Title for the output
    const displayName = currentRel ? `📂 ${currentRel}` : '📦 ROOT';

    await ctx.reply(`🌳 Generating structure for: "${displayName}"...`);

    try {
        // Generate the tree string starting from the current folder
        const treeBody = generateTree(startDir);
        const treeStructure = `${displayName}\n${treeBody}`;
        
        // Check length (Telegram Limit ~4096)
        if (treeStructure.length < 4000) {
            // Send as Message
            return ctx.replyWithHTML(`<b>📍 Structure View:</b>\n\n<pre>${treeStructure}</pre>`);
        } else {
            // Send as File (if too big)
            const safeName = (currentRel || 'root').replace(/[\/\\|]/g, '_');
            const reportPath = path.join(DATA_DIR, `${safeName}_structure.txt`);
            
            fs.writeFileSync(reportPath, treeStructure);
            
            await ctx.replyWithDocument(
                { source: reportPath, filename: `${safeName}_tree.txt` }, 
                { caption: `📂 The structure for "${displayName}" is too long for a message. Here is the file.` }
            );
            
            // Cleanup temp file
            fs.unlinkSync(reportPath);
        }
    } catch (e) {
        return ctx.reply(`❌ Error generating tree: ${e.message}`);
    }
});

// 🌳 NEW COMMAND: /fulltree
bot.command('fulltree', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const currentRel = userPaths[ctx.chat.id] || ''; 
    const startDir = getAbsPath(currentRel);
    const displayName = currentRel ? `📂 ${currentRel}` : '📦 ROOT';

    await ctx.reply(`🕵️ Generating Detailed Tree (with links) for: "${displayName}"...`);

    try {
        const treeBody = generateFullTree(startDir);
        const treeStructure = `${displayName} (Detailed View)\n${treeBody}`;
        
        // Since links make lines long, we default to sending a file
        const safeName = (currentRel || 'root').replace(/[\/\\|]/g, '_');
        const reportPath = path.join(DATA_DIR, `${safeName}_full_structure.txt`);
        
        fs.writeFileSync(reportPath, treeStructure);
        
        await ctx.replyWithDocument(
            { source: reportPath, filename: `${safeName}_full_tree.txt` }, 
            { caption: `📂 Detailed structure for "${displayName}"` }
        );
        
        fs.unlinkSync(reportPath); // Cleanup

    } catch (e) {
        return ctx.reply(`❌ Error generating tree: ${e.message}`);
    }
});

// 🌐 NEW COMMAND: /fullhtree (HTML Output)
bot.command('fullhtree', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const currentRel = userPaths[ctx.chat.id] || ''; 
    const startDir = getAbsPath(currentRel);
    const displayName = currentRel ? `📂 ${currentRel}` : '📦 ROOT';

    await ctx.reply(`🌐 Generating HTML Map for: "${displayName}"...`);

    try {
        const treeBody = generateHtmlTree(startDir);
        
        // HTML Template with CSS styling
        const fullHtml = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Directory Tree - ${displayName}</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #f4f4f9; padding: 20px; color: #333; }
        h2 { color: #2c3e50; border-bottom: 2px solid #ddd; padding-bottom: 10px; }
        ul { list-style-type: none; padding-left: 20px; border-left: 1px solid #ddd; }
        li { margin: 5px 0; position: relative; }
        li::before { content: ""; position: absolute; top: 12px; left: -20px; width: 15px; height: 1px; background: #ddd; }
        .folder { font-weight: bold; color: #d35400; cursor: default; }
        .file { color: #2980b9; }

        /* NEW STYLES */
        .deeplink { background: #e8daef; color: #8e44ad; border: 1px solid #8e44ad; }
        .source { background: #eaf2f8; color: #2980b9; border: 1px solid #2980b9; }
        .redirect { background: #ebf5fb; color: #d35400; font-style: italic; }
        .folder { font-weight: bold; color: #d35400; }

        /* Update existing */
        a { text-decoration: none; font-size: 0.8em; margin-left: 8px; padding: 2px 6px; border-radius: 4px; }

        .tag { display: inline-block; vertical-align: middle; }
        .preview { background: #e8f6f3; color: #16a085; border: 1px solid #16a085; }
        .file { background: #eaf2f8; color: #2980b9; } /* Text color overrides bg here slightly, fixed in style below */
        a.file { border: 1px solid #2980b9; color: #2980b9; background: #eaf2f8; }
        .url { background: #fef9e7; color: #f1c40f; border: 1px solid #f1c40f; }
        a:hover { opacity: 0.7; }
    </style>
</head>
<body>
    <h2>Structure: ${displayName}</h2>
    ${treeBody}
</body>
</html>`;

        const safeName = (currentRel || 'root').replace(/[\/\\|]/g, '_');
        const reportPath = path.join(DATA_DIR, `${safeName}_map.html`);
        
        fs.writeFileSync(reportPath, fullHtml);
        
        await ctx.replyWithDocument(
            { source: reportPath, filename: `${safeName}_map.html` }, 
            { caption: `📂 Interactive map for "${displayName}".\nOpen this file in your browser.` }
        );
        
        fs.unlinkSync(reportPath); // Cleanup

    } catch (e) {
        return ctx.reply(`❌ Error generating map: ${e.message}`);
    }
});

// ---------- Admin CRUD ----------
bot.command('mkdir', (ctx) => {
    if (!isAdmin(ctx)) return;
    const current = userPaths[ctx.chat.id] || '';
    
    // Remove command and trim
    const rawText = ctx.message.text.replace('/mkdir', '').trim();
    if (!rawText) return ctx.reply("❌ Usage: /mkdir Name1 | Name2 | Name3");

    // Split by '|', clean up spaces, and remove empty entries
    const folders = rawText.split('|').map(s => s.trim()).filter(s => s.length > 0);

    let created = [];
    let existing = [];

    folders.forEach(name => {
        const cleanName = sanitize(name);
        const abs = path.join(getAbsPath(current), cleanName);
        
        if (fs.existsSync(abs)) {
            existing.push(cleanName);
        } else {
            fs.mkdirSync(abs, { recursive: true });
            created.push(cleanName);
        }
    });

    // UPDATE CACHE
    rebuildCache(getAbsPath(current), current); // Only rebuilds the current folder down

    let msg = "";
    if (created.length > 0) msg += `✅ Created: ${created.join(', ')}\n`;
    if (existing.length > 0) msg += `⚠️ Existed: ${existing.join(', ')}`;

    return cleanReply(ctx, `✅ Created ${folders.length} folders.`, buildKeyboard(current, userPages[ctx.chat.id] || 0));
});

// 🔗 LINK COMMAND (Create a Shortcut to another folder)
bot.command('link', (ctx) => {
    if (!isAdmin(ctx)) return;

    // Usage: /link Source/Path | Name of Shortcut
    // Example: /link Sequels/Avengers | Avengers Shortcut
    const text = ctx.message.text.replace('/link', '').trim();
    const parts = text.split('|');

    if (parts.length < 2) return ctx.reply("❌ Usage: /link SourcePath | ShortcutName\nExample: /link Sequels/Avengers | Avengers");

    const targetPath = parts[0].trim(); // Where it leads to
    const shortcutName = sanitize(parts[1].trim()); // The name of the button here

    const currentRel = userPaths[ctx.chat.id] || '';
    const absCurrent = getAbsPath(currentRel);

    // Verify the target actually exists
    const absTarget = getAbsPath(targetPath);
    if (!fs.existsSync(absTarget)) {
        return ctx.reply(`❌ The target path "📂 ${targetPath}" does not exist.`);
    }

    // Create the Shortcut JSON
    const shortcutData = {
        type: 'redirect', // <--- Special type we will handle below
        target_path: targetPath,
        added_at: Date.now()
    };

    const shortcutFile = path.join(absCurrent, shortcutName + '.json');
    
    if (fs.existsSync(shortcutFile)) return ctx.reply("❌ A file with this name already exists here.");

    fs.writeFileSync(shortcutFile, JSON.stringify(shortcutData, null, 2));

    // Refresh Cache
    rebuildCache(absCurrent, currentRel);

    return cleanReply(ctx, `🔗 Linked "${shortcutName}" ➡️ 📂 ${targetPath}`, buildKeyboard(currentRel, 0));
});

// 🔗 COPY LINK (Step 1: Select the item to link to)
bot.command('cplink', (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Get the item name
    const name = ctx.message.text.replace('/cplink', '').trim();
    if (!name) return ctx.reply("❌ Usage: /cplink <Name of Folder or File>\n(Use this to select the item you want to link TO)");

    const currentRel = userPaths[ctx.chat.id] || '';
    const currentAbs = getAbsPath(currentRel);
    const cleanName = sanitize(name);

    // 2. Verify existence
    const targetDir = path.join(currentAbs, cleanName);
    const targetFile = path.join(currentAbs, cleanName + '.json');
    
    let targetRelPath = '';
    let type = '';

    if (fs.existsSync(targetDir)) {
        targetRelPath = path.join(currentRel, cleanName);
        type = 'folder';
    } else if (fs.existsSync(targetFile)) {
        targetRelPath = path.join(currentRel, cleanName); // Keep clean name for logic
        type = 'file';
    } else {
        return ctx.reply("❌ Item not found in current folder.");
    }

    // 3. Save to Clipboard with action 'link'
    clipboard[ctx.from.id] = {
        action: 'link', // Differentiates from 'cut' (/mv)
        name: cleanName, // The original name
        targetPath: targetRelPath, // The path where it lives
        type: type
    };

    return ctx.reply(
        `🔗 <b>Link Copied!</b>\n\n` +
        `Target: <code>${cleanName}</code>\n` +
        `Type: ${type.toUpperCase()}\n\n` +
        `Now navigate to the destination folder and type <b>/pstlink</b>`,
        { parse_mode: 'HTML' }
    );
});

// 🔗 PASTE LINK (Step 2: Place the shortcut)
bot.command('pstlink', (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Check Clipboard
    const item = clipboard[ctx.from.id];
    if (!item) return ctx.reply("❌ Clipboard empty. Use /cplink <Name> first.");
    
    // Ensure it was a Link action, not a Move action
    if (item.action !== 'link') return ctx.reply("❌ Clipboard contains a 'Cut' item. Use /pst to move it, or /cplink to copy a link.");

    // 2. Determine Name
    // Allow user to rename on paste: /pstlink Custom Name
    const customName = ctx.message.text.replace('/pstlink', '').trim();
    const finalName = sanitize(customName || item.name);

    const destRel = userPaths[ctx.chat.id] || '';
    const destAbs = getAbsPath(destRel);

    // 3. Create the Redirect JSON
    const shortcutFile = path.join(destAbs, finalName + '.json');

    if (fs.existsSync(shortcutFile)) {
        return ctx.reply(`❌ Error: An item named "${finalName}" already exists here.`);
    }

    // The Redirect Data
    const shortcutData = {
        type: 'redirect',
        target_path: item.targetPath, // Points to the original location
        original_type: item.type,
        added_at: Date.now()
    };

    try {
        fs.writeFileSync(shortcutFile, JSON.stringify(shortcutData, null, 2));

        // 4. Update Cache
        rebuildCache(destAbs, destRel);
        
        // Note: We DO NOT clear the clipboard for links, so you can paste the same link in multiple places!
        
        return cleanReply(
            ctx, 
            `🔗 <b>Shortcut Created!</b>\n"${finalName}" ➡️ ${item.name}`, 
            buildKeyboard(destRel, 0)
        );

    } catch (e) {
        return ctx.reply(`❌ Failed to create link: ${e.message}`);
    }
});

bot.command('clrcp', (ctx) => {
    if (clipboard[ctx.from.id]) {
        delete clipboard[ctx.from.id];
        return ctx.reply("🧹 Clipboard cleared.");
    }
    return ctx.reply("ℹ️ Clipboard was already empty.");
});

// 🔗 GENERATE SHAREABLE LINK
bot.command('getlink', (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Get arguments (e.g., /getlink Iron Man)
    const targetName = ctx.message.text.replace('/getlink', '').trim();
    const currentRel = userPaths[ctx.chat.id] || '';
    const absBase = getAbsPath(currentRel);

    let finalPath = '';
    let type = '';

    // Case A: User typed nothing -> Link to CURRENT FOLDER
    if (!targetName) {
        finalPath = currentRel; // The relative path (e.g., "Movies/Action")
        type = 'folder';
        if (!finalPath) return ctx.reply("❌ Cannot link to Root. Navigate to a folder first.");
    } 
    // Case B: User typed a name -> Link to FILE or SUBFOLDER
    else {
        // Check if file exists
        const fileCheck = path.join(absBase, targetName + '.json');
        const folderCheck = path.join(absBase, targetName);

        if (fs.existsSync(fileCheck)) {
            finalPath = path.join(currentRel, targetName); // Store path WITHOUT extension for cleanliness
            type = 'file';
        } else if (fs.existsSync(folderCheck)) {
            finalPath = path.join(currentRel, targetName);
            type = 'folder';
        } else {
            return ctx.reply("❌ Item not found.");
        }
    }

    // 2. Load Existing Links to prevent duplicates
    const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));
    
    // Check if this path already has a link
    let shortId = Object.keys(links).find(key => links[key] === finalPath || links[key].path === finalPath);
    
    // If not, create new
    if (!shortId) {
        shortId = generateId();
        links[shortId] = { path: finalPath, type: type, created_at: Date.now() };
        fs.writeFileSync(LINKS_FILE, JSON.stringify(links, null, 2));
    }

    // 3. Send Link
    const botUser = ctx.botInfo.username;
    const deepLink = `https://t.me/${botUser}?start=${shortId}`;
    
    return ctx.replyWithHTML(
        `🔗 <b>Link Generated!</b>\n\n` +
        `📂 <b>Target:</b> ${finalPath}\n` +
        `🖇 <b>URL:</b> ${deepLink}\n\n` +
        `<i>Anyone with this link can access this specific ${type}.</i>`
    );
});

// 📜 LIST ALL ACTIVE LINKS
bot.command('linklist', async (ctx) => {
    if (!isAdmin(ctx)) return;

    const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));
    const ids = Object.keys(links);

    if (ids.length === 0) return ctx.reply("ℹ️ No active deep links found.");

    let output = `🔗 <b>Active Deep Links (${ids.length})</b>\n\n`;
    
    // Sort by newest first (if you have created_at, otherwise random order)
    ids.forEach(id => {
        const item = links[id];
        const p = item.path || item; // Handle legacy format
        const type = item.type ? `[${item.type.toUpperCase()}]` : '';
        const url = `https://t.me/${ctx.botInfo.username}?start=${id}`;
        
        output += `🔹 <b>ID:</b> <code>${id}</code> ${type}\n`;
        output += `   📂 ${p}\n`;
        output += `   🔗 ${url}\n\n`;
    });

    // Check message length limit (Telegram limit is ~4096 chars)
    if (output.length > 4000) {
        const reportPath = path.join(DATA_DIR, 'active_links.txt');
        // Strip HTML tags for the text file
        fs.writeFileSync(reportPath, output.replace(/<[^>]*>/g, ''));
        
        await ctx.replyWithDocument(
            { source: reportPath, filename: 'active_links.txt' },
            { caption: `🔗 List of ${ids.length} active links.` }
        );
        fs.unlinkSync(reportPath);
    } else {
        // Send as text message
        ctx.replyWithHTML(output, { disable_web_page_preview: true });
    }
});

// 🗑️ REVOKE A LINK
bot.command('revoke', (ctx) => {
    if (!isAdmin(ctx)) return;

    const args = ctx.message.text.split(/\s+/);
    if (args.length !== 2) {
        return ctx.reply("❌ Usage: /revoke <LinkID>\nExample: /revoke a1b2c3d4\n(Use /linklist to find IDs)");
    }

    const id = args[1].trim();
    const links = JSON.parse(fs.readFileSync(LINKS_FILE, 'utf8'));

    // Check if ID exists
    if (!links[id]) {
        return ctx.reply("❌ Link ID not found.");
    }

    // Get info for confirmation message
    const target = links[id].path || links[id];

    // Delete and Save
    delete links[id];
    fs.writeFileSync(LINKS_FILE, JSON.stringify(links, null, 2));

    return ctx.replyWithHTML(`🗑️ <b>Revoked!</b>\n\nThe link for <code>${target}</code> is no longer active.`);
});

bot.command('refresh', (ctx) => {
    if (!isAdmin(ctx)) return;
    
    ctx.reply("🔄 Scanning directory structure...");
    
    // Force a full rebuild of the cache from the hard drive
    rebuildCache(); 
    
    return ctx.reply("✅ Cache Refreshed! Keyboards are now in sync with disk.");
});

bot.command('setpreview', (ctx) => {
    if (!isAdmin(ctx)) return;
    
    const args = ctx.message.text.trim().split(/\s+/);
    
    // Usage: /setpreview https://t.me/c/1234/567
    if (args.length !== 2) return ctx.reply("❌ Usage: /setpreview <Telegram Link>");

    const linkInfo = parseLink(args[1]);
    if (!linkInfo || !linkInfo.chat_id) return ctx.reply("❌ Invalid Link. Make sure it's a valid message link.");

    const currentRel = userPaths[ctx.chat.id] || '';
    if (!currentRel) return ctx.reply("❌ You cannot set a preview for the Root/Index. Please navigate to a folder first.");

    const absPath = getAbsPath(currentRel);
    
    // Save the Link IDs
    const previewData = {
        from_chat_id: linkInfo.chat_id,
        message_id: linkInfo.message_id
    };

    fs.writeFileSync(path.join(absPath, 'preview.json'), JSON.stringify(previewData));
    
    // Refresh Cache so the file remains hidden
    rebuildCache(absPath, currentRel);

    return ctx.reply("✅ Folder Preview updated from link!");
});

// Remove preview command remains the same
bot.command('delpreview', (ctx) => {
    if (!isAdmin(ctx)) return;
    const currentRel = userPaths[ctx.chat.id] || '';
    const file = path.join(getAbsPath(currentRel), 'preview.json');
    if (fs.existsSync(file)) {
        fs.unlinkSync(file);
        rebuildCache(getAbsPath(currentRel), currentRel);
        return ctx.reply("🗑️ Preview removed.");
    }
    return ctx.reply("❌ No preview found here.");
});

bot.command('add', async (ctx) => {
    if (!isAdmin(ctx)) return;
    const args = ctx.message.text.trim().split(/\s+/);
    if (args.length !== 3) return ctx.reply("Usage: /add <Name> <Link>");
    const fileInfo = parseLink(args[2]);
    if (!fileInfo || !fileInfo.chat_id) return ctx.reply("❌ Invalid Link.");
    
    saveFileJson(userPaths[ctx.chat.id] || '', args[1], {
        type: 'copy', from_chat_id: fileInfo.chat_id, message_id: fileInfo.message_id, added_at: Date.now()
    });
    // UPDATE CACHE
    // Since we added a file, we just need to refresh the current folder's cache
    rebuildCache(getAbsPath(userPaths[ctx.chat.id] || ''), userPaths[ctx.chat.id] || '');
    return cleanReply(ctx, `✅ Added "${args[1]}"`, buildKeyboard(userPaths[ctx.chat.id] || '', userPages[ctx.chat.id] || 0));
});

// Batch Command: /batch EP 1 <StartLink> <EndLink>
bot.command('batch', async (ctx) => {
    if (!isAdmin(ctx)) return;
    const args = ctx.message.text.trim().split(/\s+/);
    
    if (args.length !== 5) {
        return ctx.reply("❌ Usage: /batch <Prefix> <StartNum> <FirstLink> <LastLink>\nEx: /batch EP 1 https://t.me/c/xxx/100 https://t.me/c/xxx/110");
    }

    const prefix = args[1]; // e.g., "EP"
    const startNum = parseInt(args[2]); // e.g., 1
    const firstLink = args[3];
    const lastLink = args[4];

    const firstInfo = parseLink(firstLink);
    const lastInfo = parseLink(lastLink);

    if (!firstInfo || !lastInfo) return ctx.reply("❌ Invalid Links.");
    if (firstInfo.chat_id !== lastInfo.chat_id) return ctx.reply("❌ Links must be from the same channel/chat.");
    if (lastInfo.message_id < firstInfo.message_id) return ctx.reply("❌ End link must be greater than start link.");

    const current = userPaths[ctx.chat.id] || '';
    let count = 0;
    let fileNum = startNum;

    // Iterate through message IDs
    for (let msgId = firstInfo.message_id; msgId <= lastInfo.message_id; msgId++) {
        // Pad number: 1 -> "01", 10 -> "10"
        const numStr = fileNum < 10 ? `0${fileNum}` : `${fileNum}`;
        const fileName = `${prefix}${numStr}`;
        
        saveFileJson(current, fileName, {
            type: 'copy',
            from_chat_id: firstInfo.chat_id,
            message_id: msgId,
            added_at: Date.now()
        });
        
        count++;
        fileNum++;
    }

     // UPDATE CACHE
     rebuildCache(getAbsPath(userPaths[ctx.chat.id] || ''), userPaths[ctx.chat.id] || '');

    return cleanReply(ctx, `✅ Batch complete! Added ${count} files (${prefix}${startNum < 10 ? '0'+startNum : startNum} to ${prefix}${fileNum-1}).`, buildKeyboard(userPaths[ctx.chat.id] || '', userPages[ctx.chat.id] || 0));
});

// ---------- Improved Batch List ----------
bot.command('batchlist', async (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Clean Input
    let rawText = ctx.message.text.replace('/batchlist', '').trim();

    if (!rawText) {
        return ctx.replyWithHTML(
            "<b>❌ Batch List Usage:</b>\n\n" +
            "<b>Format 1 (Single Line / Pipe Separated):</b>\n" +
            "<code>/batchlist Iron Man https://t.me/c/.. | Thor https://t.me/c/..</code>\n\n" +
            "<b>Format 2 (Multi-Line):</b>\n" +
            "<code>Item One https://t.me/c/..</code>\n" +
            "<code>Item Two https://t.me/c/..</code>\n\n" +
            "<i>Tip: If you omit the name, the bot will try to auto-detect it from the file.</i>"
        );
    }

    const statusMsg = await ctx.reply("🔄 Parsing batch request...");
    const currentRel = userPaths[ctx.chat.id] || '';
    
    // 2. Intelligent Splitter
    // If text has newlines, split by newline. If not, split by pipe '|'.
    let rawItems = [];
    if (rawText.includes('\n')) {
        rawItems = rawText.split('\n');
    } else {
        rawItems = rawText.split('|');
    }

    let success = 0;
    let failed = 0;
    let logs = "";

    // 3. Process Items
    for (let i = 0; i < rawItems.length; i++) {
        const itemStr = rawItems[i].trim();
        if (!itemStr) continue;

        // Update status periodically
        if (i % 5 === 0 && i > 0) {
            await ctx.telegram.editMessageText(
                ctx.chat.id, 
                statusMsg.message_id, 
                undefined, 
                `🔄 Processing ${i}/${rawItems.length}...\n✅ OK: ${success} | ❌ Fail: ${failed}`
            ).catch(()=>{});
        }

        // --- SMART PARSER ---
        // Looks for the first occurrence of http:// or https://
        // Everything BEFORE it is the Name. Everything AFTER (inclusive) is the Link.
        const urlMatch = itemStr.match(/(https?:\/\/[^\s]+)/);

        if (!urlMatch) {
            failed++;
            logs += `❌ No Link found: "${itemStr.substring(0, 15)}..."\n`;
            continue;
        }

        const link = urlMatch[0]; // The URL
        
        // Extract Name: Remove the link from the string and clean up
        let name = itemStr.replace(link, '').trim();
        
        // Cleanup: Remove common separators users might type between name and link (e.g. "Name | Link", "Name - Link")
        name = name.replace(/[|\-:]+$/, '').trim();

        const linkInfo = parseLink(link);

        // A. TELEGRAM FILE (COPY)
        if (linkInfo && linkInfo.chat_id) {
            
            // Auto-Detect Name if empty
            if (!name) {
                try {
                    const tempMsg = await ctx.telegram.copyMessage(
                        ctx.chat.id, 
                        linkInfo.chat_id, 
                        linkInfo.message_id, 
                        { disable_notification: true }
                    );
                    
                    const rawName = tempMsg.document?.file_name || 
                                    tempMsg.video?.file_name || 
                                    tempMsg.audio?.file_name || 
                                    (tempMsg.caption ? tempMsg.caption.split('\n')[0] : "Untitled");
                    
                    name = cleanFilename(rawName);
                    await ctx.telegram.deleteMessage(ctx.chat.id, tempMsg.message_id).catch(()=>{});
                    await new Promise(r => setTimeout(r, 800)); // Anti-flood
                } catch (e) {
                    failed++;
                    logs += `⚠️ Auto-Name Failed: ${link}\n`;
                    continue;
                }
            } else {
                name = sanitize(name);
            }

            saveFileJson(currentRel, name, {
                type: 'copy',
                from_chat_id: linkInfo.chat_id,
                message_id: linkInfo.message_id,
                added_at: Date.now()
            });
            success++;
        }

        // B. EXTERNAL LINK (URL)
        else if (link.startsWith('http')) {
            if (!name) {
                failed++;
                logs += `⚠️ URL requires Name: ${link}\n`;
                continue;
            }
            saveFileJson(currentRel, sanitize(name), {
                type: 'url',
                url: link,
                added_at: Date.now()
            });
            success++;
        }
    }

    // 4. Finish
    rebuildCache(getAbsPath(currentRel), currentRel);
    await ctx.telegram.deleteMessage(ctx.chat.id, statusMsg.message_id).catch(()=>{});

    let resultMsg = `✅ <b>Batch Complete</b>\n` +
                    `📂 Path: ${currentRel || 'Root'}\n` +
                    `✅ Added: ${success}\n` +
                    `❌ Failed: ${failed}`;

    if (logs) {
         if (logs.length > 3000) logs = logs.substring(0, 3000) + "...";
         resultMsg += `\n\n<b>📝 Logs:</b>\n<pre>${logs}</pre>`;
    }

    return cleanReply(ctx, resultMsg, { parse_mode: 'HTML', ...buildKeyboard(currentRel, 0) });
});

// Add a Redirect Link (to another bot or site)
bot.command('addlink', (ctx) => {
    if (!isAdmin(ctx)) return;
    
    // Format: /addlink Name | URL
    const text = ctx.message.text.replace('/addlink', '').trim();
    const parts = text.split('|');

    if (parts.length < 2) return ctx.reply("❌ Usage: /addlink Button Name | https://t.me/Bot?start=123");

    const name = parts[0].trim();
    const url = parts.slice(1).join('|').trim(); // Join back in case URL has |

    if (!name || !url.startsWith('http')) return ctx.reply("❌ Invalid Name or URL.");

    const currentRel = userPaths[ctx.chat.id] || '';
    
    saveFileJson(currentRel, name, {
        type: 'url',
        url: url,
        added_at: Date.now()
    });

    rebuildCache(getAbsPath(currentRel), currentRel);
    return cleanReply(ctx, `✅ Added Link: "${name}"`, buildKeyboard(currentRel, 0));
});

bot.command('rn', (ctx) => {
    if (!isAdmin(ctx)) return;
    
    // Get Relative and Absolute paths
    const relPath = userPaths[ctx.chat.id] || '';
    const current = getAbsPath(relPath);
    
    const args = ctx.message.text.replace('/rn', '').split('|');
    if (args.length !== 2) return ctx.reply("❌ Usage: /rn Old Name | New Name");

    const oldName = sanitize(args[0].trim());
    const newName = sanitize(args[1].trim());

    // Define Paths
    const oldDir = path.join(current, oldName);
    const newDir = path.join(current, newName);
    const oldFile = path.join(current, oldName + '.json');
    const newFile = path.join(current, newName + '.json');

    // 🛡️ Safety: Check if new name already exists
    if (fs.existsSync(newDir) || fs.existsSync(newFile)) {
        return ctx.reply("❌ Destination name already exists.");
    }

    try {
        if (fs.existsSync(oldDir)) {
            fs.renameSync(oldDir, newDir);
        } else if (fs.existsSync(oldFile)) {
            fs.renameSync(oldFile, newFile);
        } else {
            return ctx.reply("❌ Item not found.");
        }

        // 🔄 UPDATE CACHE (Important!)
        // Refresh the memory for the current folder
        rebuildCache(current, relPath);

        return cleanReply(ctx, `✏️ Renamed: ${oldName} -> ${newName}`, buildKeyboard(relPath, userPages[ctx.chat.id] || 0));

    } catch (e) {
        return ctx.reply(`Error: ${e.message}`);
    }
});

bot.command('del', (ctx) => {
    if (!isAdmin(ctx)) return;
    const current = getAbsPath(userPaths[ctx.chat.id] || '');
    const name = ctx.message.text.replace('/del', '').trim();
    if (!name) return ctx.reply("❌ Usage: /del <Name>");
    const sName = sanitize(name);
    const pDir = path.join(current, sName);
    const pFile = path.join(current, sName + '.json');
    try {
        // Case 1: It is a Directory
        if (fs.existsSync(pDir)) {
            // Count files inside recursively to update meta
            let count = 0;
            const countRecursive = (d) => {
                fs.readdirSync(d, { withFileTypes: true }).forEach(i => {
                    if (i.isDirectory()) countRecursive(path.join(d, i.name));
                    else if (i.name.endsWith('.json')) count++;
                });
            };
            countRecursive(pDir);
            
            // Update Meta
            metaData.total_files = Math.max(0, metaData.total_files - count);
            saveMeta();
            
            // Delete Directory
            fs.rmSync(pDir, { recursive: true, force: true });

            // UPDATE CACHE
            // Deleting folders affects the tree structure, so it's safest to rebuild the specific parent path
            rebuildCache(current, userPaths[ctx.chat.id] || '');
            
            // Reset pagination and show keyboard
            userPages[ctx.chat.id] = 0;
            return cleanReply(ctx, `🗑️ Deleted Folder: ${name}`, buildKeyboard(userPaths[ctx.chat.id], 0));
        }
        // Case 2: It is a File
        else if (fs.existsSync(pFile)) {
            fs.unlinkSync(pFile);
            
            // Update Meta
            metaData.total_files = Math.max(0, metaData.total_files - 1);
            saveMeta();
            
            // Reset pagination and show keyboard
            userPages[ctx.chat.id] = 0;
            return cleanReply(ctx, `🗑️ Deleted File: ${name}`, buildKeyboard(userPaths[ctx.chat.id], 0));
        } 
        // Case 3: Not Found
        else {
            return ctx.reply("❌ Not found.");
        }
    } catch (e) {
        return ctx.reply(`Error: ${e.message}`);
    }
});

bot.command('pwd', (ctx) => {
    if (!isAdmin(ctx)) return;
    const p = userPaths[ctx.chat.id] || 'Root';
    // Ensure userPages is initialized to prevent error
    userPages[ctx.chat.id] = 0; 
    ctx.reply(`📂 Current Path: ${p}`);
});

// Add a simple sleep function to avoid hitting Telegram limits
const sleep = ms => new Promise(r => setTimeout(r, ms));

bot.command('broadcast', async (ctx) => {
    if (!isAdmin(ctx)) return;
    
    const message = ctx.message.text.replace('/broadcast', '').trim();
    if (!message) return ctx.reply("❌ Usage: /broadcast <Message>");

    const users = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')).ids;
    let success = 0, blocked = 0;

    await ctx.reply(`🚀 Starting broadcast to ${users.length} users...`);

    for (const userId of users) {
        try {
            await ctx.telegram.sendMessage(userId, `📢 <b>Announcement:</b>\n\n${message}`, { parse_mode: 'HTML' });
            success++;
        } catch (e) {
            blocked++; // User blocked the bot
        }
        await sleep(50); // 20 messages per second limit
    }

    return ctx.reply(`✅ Broadcast Complete.\nSent: ${success}\nFailed/Blocked: ${blocked}`);
});

// ✂️ CUT COMMAND (Select Item)
bot.command('mv', (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Parse Input
    const name = ctx.message.text.replace('/mv', '').trim();
    if (!name) return ctx.reply("❌ Usage: /mv <Name>\n(Use this to 'cut' an item from the current folder)");

    const currentRel = userPaths[ctx.chat.id] || '';
    const currentAbs = getAbsPath(currentRel);
    const cleanName = sanitize(name);

    // 2. Identify Target
    const targetDir = path.join(currentAbs, cleanName);
    const targetFile = path.join(currentAbs, cleanName + '.json');
    
    let sourcePath = null;
    let type = null;

    if (fs.existsSync(targetDir)) {
        sourcePath = targetDir;
        type = 'folder';
    } else if (fs.existsSync(targetFile)) {
        sourcePath = targetFile;
        type = 'file';
    } else {
        return ctx.reply("❌ Item not found in current folder.");
    }

    // 3. Save to Clipboard
    clipboard[ctx.from.id] = {
        name: cleanName,
        sourcePath: sourcePath,
        type: type
    };

    return ctx.reply(
        `✂️ <b>Cut to Clipboard:</b> "${cleanName}"\n\n` +
        `Now navigate to the destination folder and type <b>/pst</b> to move it here.`,
        { parse_mode: 'HTML' }
    );
});

// 📋 PASTE COMMAND (Execute Move)
bot.command('pst', (ctx) => {
    if (!isAdmin(ctx)) return;

    // 1. Check Clipboard
    const item = clipboard[ctx.from.id];
    if (!item) return ctx.reply("❌ Clipboard empty. Use /mv <Name> first.");

    const destRel = userPaths[ctx.chat.id] || '';
    const destAbs = getAbsPath(destRel);

    // 2. Validate Source Existence (In case it was deleted externally)
    if (!fs.existsSync(item.sourcePath)) {
        delete clipboard[ctx.from.id];
        return ctx.reply("❌ The source item no longer exists.");
    }

    // 3. Prevent "Folder into Itself" (Infinite Loop)
    // If moving a folder, ensure destination is not INSIDE the source
    if (item.type === 'folder' && destAbs.startsWith(item.sourcePath)) {
        return ctx.reply("❌ Cannot move a folder into itself.");
    }

    // 4. Calculate New Path
    // If file, append .json. If folder, keep name.
    const newFilename = item.type === 'file' ? item.name + '.json' : item.name;
    const finalDestPath = path.join(destAbs, newFilename);

    // 5. Check Collision
    if (fs.existsSync(finalDestPath)) {
        return ctx.reply(`❌ Error: An item named "${item.name}" already exists here.`);
    }

    // 6. Execute Move
    try {
        fs.renameSync(item.sourcePath, finalDestPath);

        // 7. Update Cache & Cleanup
        // We must rebuild cache for Root (to catch the removal) and Current (to show addition)
        rebuildCache(); 
        delete clipboard[ctx.from.id]; // Clear clipboard

        return cleanReply(
            ctx, 
            `✅ Moved "${item.name}" to 📂 ${destRel || 'Root'}`, 
            buildKeyboard(destRel, 0)
        );

    } catch (e) {
        return ctx.reply(`❌ Move Failed: ${e.message}`);
    }
});

// 🗑️ CLEAR CLIPBOARD (Optional)
bot.command('unmv', (ctx) => {
    if (clipboard[ctx.from.id]) {
        delete clipboard[ctx.from.id];
        ctx.reply("✅ Clipboard cleared.");
    } else {
        ctx.reply("ℹ️ Nothing in clipboard.");
    }
});

bot.command('request', async (ctx) => {
    const query = ctx.message.text.replace('/request', '').trim();
    if (!query) return ctx.reply("Usage: /request <Movie/Series Name>");

    // Send to Admin
    await ctx.telegram.sendMessage(ADMIN_ID, `📩 <b>New Request</b>\n\nUser: ${ctx.from.first_name} (ID: ${ctx.from.id})\nRequested: ${query}`, { parse_mode: 'HTML' });

    return ctx.reply("✅ Request sent to Admin!");
});

// ---------- Forward-to-Add Logic ----------

// Listen for media (Video, Document, Audio) sent by Admin
bot.on(['video', 'document', 'audio', 'photo'], async (ctx) => {
    if (!isAdmin(ctx)) return;

    // We need the Source Chat ID and Message ID
    // Note: This works best if forwarded from a Channel where the bot is Admin
    let srcChatId = ctx.chat.id;
    let srcMsgId = ctx.message.message_id;

    // If it is a forward, try to grab the original source
    if (ctx.message.forward_from_chat) {
        srcChatId = ctx.message.forward_from_chat.id;
        srcMsgId = ctx.message.forward_from_message_id;
    }

    // Attempt to guess a name from Caption or File Name
    let suggestedName = "";
    if (ctx.message.caption) suggestedName = ctx.message.caption.split('\n')[0]; // First line of caption
    else if (ctx.message.document) suggestedName = ctx.message.document.file_name;
    else if (ctx.message.video) suggestedName = ctx.message.video.file_name;
    
    // Clean the suggested name
    suggestedName = sanitize(suggestedName).replace('.json', '');

    // Store in memory
    pendingUploads[ctx.from.id] = {
        from_chat_id: srcChatId,
        message_id: srcMsgId,
        caption: ctx.message.caption || ""
    };

    return ctx.replyWithHTML(
        `📥 <b>File Received!</b>\n\n` +
        `Reply with the path and name to save it:\n` +
        `Format: <code>Folder/Subfolder | Filename</code>\n\n` +
        `<i>Suggested:</i> <code>${userPaths[ctx.chat.id] || 'Root'} | ${suggestedName || 'MyFile'}</code>`
    );
});

// ❌ SMART DELETE HANDLER (Deletes File + Timer Text)
bot.action(/^del_msg(?::(\d+))?$/, async (ctx) => {
    try {
        // 1. Delete the File (The message with the button)
        await ctx.deleteMessage().catch(() => {});

        // 2. Delete the Timer Text (If ID was passed)
        // The ID is in ctx.match[1] because of the regex
        const timerMsgId = ctx.match[1];
        if (timerMsgId) {
            await ctx.telegram.deleteMessage(ctx.chat.id, parseInt(timerMsgId)).catch(() => {});
        }

        await ctx.answerCbQuery("🗑️ Cleaned up!");
    } catch (e) {
        await ctx.answerCbQuery("⚠️ Already deleted.");
    }
});

// ---------- Universal Navigation & Sending ----------
bot.on('text', async (ctx) => {
    const text = ctx.message.text;
    if (text.startsWith('/')) return; 

    // 🆕 1. CHECK FOR PENDING UPLOADS
    if (pendingUploads && pendingUploads[ctx.from.id]) {
        const fileData = pendingUploads[ctx.from.id];
        const args = text.split('|');
        let targetFolder = userPaths[ctx.chat.id] || ''; 
        let targetName = args[0].trim();
        if (args.length > 1) { targetFolder = args[0].trim(); targetName = args[1].trim(); }
        if (!targetName) return ctx.reply("❌ Please provide a filename.");
        
        saveFileJson(targetFolder, targetName, { type: 'copy', from_chat_id: fileData.from_chat_id, message_id: fileData.message_id, caption: fileData.caption, added_at: Date.now() });
        delete pendingUploads[ctx.from.id];
        rebuildCache(getAbsPath(targetFolder), targetFolder);
        return ctx.reply(`✅ Saved "${targetName}" in 📂 ${targetFolder || 'Root'}`);
    }

    // --- SMART TEXT CLEANER ---
    const currentRel = userPaths[ctx.chat.id] || '';
    const currentAbs = getAbsPath(currentRel);
    const { folders, files } = getDirContents(currentRel);

    let cleanText = text;
    let isFolderRequest = false;
    let isFileRequest = false;

    // Check against known Folders (Handles Custom Icons)
    const matchedFolder = folders.find(f => `${f.icon} ${f.name}` === text);
    if (matchedFolder) {
        cleanText = matchedFolder.name;
        isFolderRequest = true;
    } 
    // Check against known Files
    else if (text.startsWith(FILE_ICON)) {
        cleanText = text.replace(FILE_ICON, '').trim();
        isFileRequest = true;
    }
    // Check Navigation Buttons (Stop here)
    else if (['ℹ️ Help', '⬅️ Back', '🏠 Index', 'Next ➡️', '⬅️ Prev'].some(b => text.startsWith(b))) {
        return; 
    }
    else {
        // Fallback cleanup
        cleanText = text.replace(new RegExp(`^(${FOLDER_ICON}|${FILE_ICON})\\s*`), '').trim();
    }

    // 🔴 LOCK CHECK
    if (isFolderRequest && matchedFolder && matchedFolder.locked_channel && !isAdmin(ctx)) {
        try {
            const member = await ctx.telegram.getChatMember(matchedFolder.locked_channel, ctx.from.id);
            if (!['creator', 'administrator', 'member'].includes(member.status)) {
                return ctx.reply(`🔒 <b>Locked Folder</b>\n\nYou must join ${matchedFolder.locked_channel} to open this folder.`, { parse_mode: 'HTML' });
            }
        } catch (e) {
            return ctx.reply(`❌ Access Denied. Join ${matchedFolder.locked_channel}.`);
        }
    }

    // 2. FOLDER NAVIGATION (Standard browsing)
    const targetDir = path.join(currentAbs, sanitize(cleanText));
    if (fs.existsSync(targetDir) && fs.lstatSync(targetDir).isDirectory()) {
        const newRel = path.join(currentRel, sanitize(cleanText));
        userPaths[ctx.chat.id] = newRel;
        userPages[ctx.chat.id] = 0;

        // Check Preview
        const previewFile = path.join(targetDir, 'preview.json');
        if (fs.existsSync(previewFile)) {
            logUserAction(ctx, 'NAVIGATE', cleanText);
            try {
                const pData = JSON.parse(fs.readFileSync(previewFile, 'utf8'));
                const sourceChat = pData.from_chat_id || pData.chat_id;
                try { await ctx.deleteMessage(); } catch(e){}

                const sentMsg = await ctx.telegram.copyMessage(ctx.chat.id, sourceChat, pData.message_id, {
                    ...buildKeyboard(newRel, 0)
                });
                setTimeout(() => ctx.telegram.deleteMessage(ctx.chat.id, sentMsg.message_id).catch(()=>{}), CLEAR_TIMER);
                return;
            } catch (err) {}
        }
        
        // ⭐ UPDATED REPLY WITH PATH
        const displayPath = formatDisplayPath(newRel);
        const icon = matchedFolder ? matchedFolder.icon : FOLDER_ICON;
        return cleanReply(ctx, `${icon} 📂 Path: ${displayPath}`, buildKeyboard(newRel, 0));
    }

    // 3. FILE SENDING
    const targetFile = path.join(currentAbs, sanitize(cleanText) + '.json');
    if (fs.existsSync(targetFile)) {
        logUserAction(ctx, 'DOWNLOAD', cleanText);
        try { await ctx.deleteMessage(); } catch(e){}
        return sendFileToUser(ctx, targetFile, cleanText);
    }

    // 4. STOPPER (If icon detected but file missing)
    if (isFolderRequest || isFileRequest) {
        return ctx.reply("❌ Item not found on disk. Try /refresh to sync.");
    }

    // 5. SEARCH (Smart Search & Jump)
    if (cleanText.length < 2) return ctx.reply("🔍 Query too short.");
    logUserAction(ctx, 'SEARCH', cleanText);
    
    // Use the improved findPathRecursively function
    const results = findPathRecursively(DATA_DIR, cleanText);
    if (results.length === 0) return ctx.reply("❌ No results found.");

    const topMatch = results[0];

    // ⭐ EXACT MATCH / SELECTION LOGIC
    // If the user clicked a button from a previous search result, it falls here.
    // We check if it's a high score (exact name match) or the only result.
    if (results.length === 1 || topMatch.score >= 90) {
        let targetPath = topMatch.isDirectory ? topMatch.relPath : path.dirname(topMatch.relPath);
        
        // If it's a file, we enter its parent folder so the user can see it
        if (!topMatch.isDirectory) {
             userPaths[ctx.chat.id] = targetPath;
        } else {
             userPaths[ctx.chat.id] = topMatch.relPath;
        }

        userPages[ctx.chat.id] = 0;
        
        // ⭐ UPDATED REPLY WITH FULL PATH
        const displayPath = formatDisplayPath(userPaths[ctx.chat.id]);
        return cleanReply(ctx, `📂 Opened: ${displayPath}`, buildKeyboard(userPaths[ctx.chat.id], 0));
    }

    // MULTIPLE RESULTS MENU
    const topResults = results.slice(0, 8);
    const rows = [];
    topResults.forEach(r => {
        const icon = r.isDirectory ? FOLDER_ICON : FILE_ICON;
        rows.push([`${icon} ${r.name}`]); 
    });
    rows.push(['🏠 Index']);

    return cleanReply(
        ctx, 
        `🔍 <b>Multiple results found for:</b> "${cleanText}"\nSelect one below:`, 
        { parse_mode: 'HTML', ...Markup.keyboard(rows).resize() }
    );
});

// 📦 HELPER: Centralized File Sending Logic
async function sendFileToUser(ctx, absPath, displayName) {
    try {
        const data = JSON.parse(fs.readFileSync(absPath, 'utf8'));
        
        // 🔀 REDIRECT Support (If you added the previous feature)
        if (data.type === 'redirect') {
            userPaths[ctx.chat.id] = data.target_path;
            userPages[ctx.chat.id] = 0;
            return cleanReply(ctx, `📂 Redirecting to: ${path.basename(data.target_path)}`, buildKeyboard(data.target_path, 0));
        }

        // 🅰️ URL
        if (data.type === 'url') {
            const sentMsg = await ctx.replyWithHTML(
                `📄 <b>${displayName}</b>\n\n🔗 <i>External Link:</i>`,
                Markup.inlineKeyboard([[Markup.button.url('🚀 Open Link', data.url)]])
            );
            setTimeout(() => { 
                ctx.telegram.deleteMessage(ctx.chat.id, sentMsg.message_id).catch(()=>{}); 
            }, metaData.timers.file);
            return;
        }

        // 🅱️ FILE COPY
        const sourceChat = data.from_chat_id || data.chat_id;
        if (!sourceChat) return ctx.reply("❌ Error: Source ID missing.");

        // Timer Logic
        const fileMins = Math.floor(metaData.timers.file / 60000);
        const timerMsg = await ctx.reply(`⏳ Loading: ${displayName}\n🗑️ Auto-deletes in ${fileMins} mins...`);

        // Send File
        const sentMsg = await ctx.telegram.copyMessage(ctx.chat.id, sourceChat, data.message_id, {
            caption: data.caption || '',
            ...Markup.inlineKeyboard([[
                Markup.button.callback('❌ Delete / Close', `del_msg:${timerMsg.message_id}`)
            ]])
        });

        // Auto Delete
        setTimeout(async () => { 
            try { 
                await ctx.telegram.deleteMessage(ctx.chat.id, sentMsg.message_id); 
                await ctx.telegram.deleteMessage(ctx.chat.id, timerMsg.message_id); 
            } catch (e) {} 
        }, metaData.timers.file);

    } catch (e) {
        ctx.reply(`❌ Error sending file: ${e.message}`);
    }
}

// 🔒 Helper: Check if a folder path is locked
async function checkFolderLock(ctx, absPath) {
    if (isAdmin(ctx)) return true; // Admins bypass locks

    const configPath = path.join(absPath, 'config.json');
    
    // If no config exists, it's not locked
    if (!fs.existsSync(configPath)) return true;

    try {
        const conf = JSON.parse(fs.readFileSync(configPath));
        
        // If no lock configured
        if (!conf.locked_channel) return true;

        const channel = conf.locked_channel;

        // Check Membership
        const member = await ctx.telegram.getChatMember(channel, ctx.from.id);
        if (['creator', 'administrator', 'member'].includes(member.status)) {
            return true; // Allowed
        }

        // Access Denied
        await ctx.reply(
            `🔒 <b>Locked Folder</b>\n\nYou must join ${channel} to access this content.`,
            { 
                parse_mode: 'HTML',
                ...Markup.inlineKeyboard([
                    [Markup.button.url('📢 Join Channel', `https://t.me/${channel.replace('@', '')}`)],
                    [Markup.button.url('🔄 Try Link Again', `https://t.me/${ctx.botInfo.username}?start=${ctx.message.text.split(' ')[1]}`)]
                ])
            }
        );
        return false; // Denied

    } catch (e) {
        console.error("Lock Check Error:", e);
        // If we can't check (bot not admin in channel), default to blocking or allowing?
        // Safe bet: Block and ask to contact admin, or just Allow if you prefer loose security.
        // Here we Block:
        await ctx.reply(`❌ Error checking permissions for ${conf.locked_channel}. Make sure I am Admin there.`);
        return false;
    }
}

// Launch
bot.launch().then(() => console.log('🚀 Collections Bot Started'));

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));

// ==========================================
// 🌐 WEBSITE INTEGRATION (Express)
// ==========================================
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

app.set('view engine', 'ejs');
app.use(express.static('public')); // Optional for CSS files

// Helper: Get data for website (Reuses your bot logic)
function getWebContents(relPath) {
    // Security: Prevent accessing files outside data folder
    if (relPath.includes('..')) return { folders: [], files: [] };
    
    // Use the existing Directory Cache from the bot
    const cacheKey = relPath.replace(/\\/g, '/');
    if (DIR_CACHE[cacheKey]) {
        return DIR_CACHE[cacheKey];
    }
    return { folders: [], files: [] };
}

// Route: Home & Browse
app.get('/', (req, res) => {
    const currentPath = req.query.path || '';
    const rawItems = getWebContents(currentPath);

    // 🛠️ FIX: Convert raw strings to objects so EJS can read '.name'
    const items = {
        folders: rawItems.folders, 
        files: rawItems.files.map(fileString => ({ name: fileString })) 
    };

    res.render('index', { items, currentPath });
});

// Route: Search
app.get('/search', (req, res) => {
    const query = req.query.q || '';
    if (!query) return res.redirect('/');

    // Reuse bot's recursive search function
    const results = findPathRecursively(DATA_DIR, query);
    
    // Format results for the template
    const formatted = {
        folders: results.filter(r => r.isDirectory).map(r => ({ name: r.name, icon: '📂' })),
        files: results.filter(r => !r.isDirectory).map(r => ({ name: r.name }))
    };

    res.render('index', { items: formatted, currentPath: `Search: ${query}` });
});

// Route: View File / Get Link
app.get('/view', (req, res) => {
    let relPath = req.query.path;
    if (!relPath) return res.redirect('/');

    // 🛠️ FIX: The cache stores names without ".json", but the disk needs it.
    // We append .json here if it's missing.
    if (!relPath.endsWith('.json')) relPath += '.json';

    const absPath = path.join(DATA_DIR, relPath);
    
    // Security check
    if (!absPath.startsWith(DATA_DIR) || !fs.existsSync(absPath)) {
        return res.send('File not found or deleted.');
    }

    try {
        const data = JSON.parse(fs.readFileSync(absPath, 'utf8'));
        const botUser = bot.botInfo.username;
        
        // If it's a URL, redirect immediately
        if (data.type === 'url') return res.redirect(data.url);

        res.send(`
            <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body { background:#121212; color:white; font-family:sans-serif; text-align:center; padding-top:50px; }
                    .btn { padding:15px 30px; background:#0088cc; color:white; text-decoration:none; border-radius:5px; font-size:1.2em; display:inline-block; margin-top:20px;}
                    .back { color:#888; display:block; margin-top:30px; text-decoration:none; }
                </style>
            </head>
            <body>
                <h1>${relPath.split(/[/\\]/).pop().replace('.json', '')}</h1>
                <p>This file is stored on Telegram.</p>
                
                <a href="https://t.me/${botUser}" class="btn">
                    🚀 Open in Telegram Bot
                </a>
                
                <a href="/" class="back">⬅ Back to Home</a>
            </body>
            </html>
        `);
    } catch (e) {
        res.send("Error reading file.");
    }
});

// Start Server
app.listen(PORT, () => {
    console.log(`🌐 Website running at http://localhost:${PORT}`);
});