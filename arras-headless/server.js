(async () => {
    const { Worker } = await import("worker_threads");
    const path = await import("path");
    const { WebSocketServer } = await import("ws");
    const { pack, unpack } = await import("msgpackr");
    const http = await import("http");
    const fetchModule = await import("node-fetch");
    const realFetch = fetchModule.default || fetchModule;

    const noop = () => {};
    console.log = noop;
    console.error = noop;
    console.warn = noop;
    console.info = noop;
    console.debug = noop;

    // only real status line we print
    let totalSpawned = 0;
    function printSpawned() {
        process.stdout.write(`\r[spawned] ${totalSpawned} bots   `);
    }

    const WORKER_MEMORY_MB = 64;
    const BOTS_PER_WORKER = 8;
    const PREWARM_POOL_SIZE = 4;
    const MAX_PROXIES = 4000;

    let PROXIES = [];

    const PROXY_SOURCES = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=8000&country=all&ssl=all&anonymity=all",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        "https://www.proxy-list.download/api/v1/get?type=http",
        "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt"
    ];

    async function fetchProxies() {
        const all = new Set();
        await Promise.allSettled(
            PROXY_SOURCES.map(async (url) => {
                try {
                    const res = await realFetch(url, { timeout: 12000 });
                    if (!res.ok) return;
                    const text = await res.text();
                    for (const line of text.split(/\r?\n/)) {
                        const cleaned = line.trim().replace(/^https?:\/\//i, "");
                        if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}$/.test(cleaned)) {
                            all.add(`http://${cleaned}`);
                            if (all.size >= MAX_PROXIES) break;
                        }
                    }
                } catch {}
            })
        );
        PROXIES = Array.from(all).slice(0, MAX_PROXIES);
        for (let i = PROXIES.length - 1; i > 0; i--) {
            const j = (Math.random() * (i + 1)) | 0;
            [PROXIES[i], PROXIES[j]] = [PROXIES[j], PROXIES[i]];
        }
    }

    let arrasScriptCache = null;
    let arrasWasmCache = null;

    const server = http.createServer((req, res) => {
        res.writeHead(426, { "Content-Type": "text/plain" });
        res.end("lll elk ez big fat noob");
    });

    function randint(a, b) {
        return Math.floor(Math.random() * (b - a + 1)) + a;
    }

    const botWorkerPath = path.join(__dirname, "index.js");

    function extractArrasScript(html) {
        const start = html.indexOf("<script>");
        if (start === -1) throw new Error("no script");
        const s = start + 8;
        const end = html.indexOf("</script", s);
        if (end === -1) throw new Error("no close");
        return html.slice(s, end);
    }

    async function preloadArrasAssets() {
        try {
            const [htmlRes, wasmRes] = await Promise.all([
                realFetch("https://arras.io"),
                realFetch("https://arras.io/app.wasm")
            ]);
            arrasScriptCache = extractArrasScript(await htmlRes.text());
            arrasWasmCache = new Uint8Array(await wasmRes.arrayBuffer());
        } catch {}
    }

    function createBotWorker(session) {
        const worker = new Worker(botWorkerPath, {
            resourceLimits: {
                maxOldGenerationSizeMb: WORKER_MEMORY_MB,
                maxYoungGenerationSizeMb: 24,
                codeRangeSizeMb: 24
            }
        });
        worker.send = (msg) => worker.postMessage(msg);
        worker.botId = null;
        worker.botIds = [];
        worker.activeBots = 0;
        worker.isPooled = false;
        worker.resolvedHash = null;

        worker.on("error", noop);
        worker.on("message", (message) => {
            if (!message) return;
            if (message.type === "died") {
                const idx = worker.botIds.indexOf(message.id);
                if (idx !== -1) worker.botIds.splice(idx, 1);
                worker.activeBots = Math.max(0, worker.activeBots - 1);
            } else if (message.type === "hash_update" && message.hash) {
                worker.resolvedHash = message.hash;
                if (session) {
                    session.resolvedHash = message.hash;
                    if (session.ws) {
                        try { session.ws.send(pack(["R", message.hash])); } catch {}
                    }
                }
            }
        });
        worker.on("exit", () => {
            let idx = session.workers.indexOf(worker);
            if (idx !== -1) session.workers.splice(idx, 1);
            idx = session.pool.indexOf(worker);
            if (idx !== -1) session.pool.splice(idx, 1);
        });
        return worker;
    }

    function prepareWorker(worker) {
        worker.send({
            type: "prepare",
            arrasCache: arrasScriptCache,
            wasmCache: arrasWasmCache
        });
    }

    function fillPool(session) {
        const total = session.workers.length + session.pool.length;
        const needed = Math.max(0, PREWARM_POOL_SIZE - total);
        for (let i = 0; i < needed; i++) {
            const worker = createBotWorker(session);
            worker.isPooled = true;
            session.pool.push(worker);
            prepareWorker(worker);
        }
    }

    function acquireWorker(session) {
        let worker = session.workers.find((w) => w.activeBots < BOTS_PER_WORKER);
        if (worker) return worker;
        worker = session.pool.shift() || createBotWorker(session);
        worker.isPooled = false;
        if (!session.workers.includes(worker)) session.workers.push(worker);
        return worker;
    }

    function spawnBotNow(session, hash, botName) {
        if (!PROXIES.length) return;
        if (session.proxyIdx >= PROXIES.length) session.proxyIdx = 0;

        const worker = acquireWorker(session);
        const botId = session.nextBotId++;
        worker.botId = botId;
        worker.botIds.push(botId);
        worker.activeBots++;

        let selectedTank = session.tank;
        if (session.tanks.length) {
            selectedTank = session.tanks[session.tankIdx];
            session.tankIdx = (session.tankIdx + 1) % session.tanks.length;
        }

        const rawHash = String(hash || "").replace(/^#/, "");
        const spawnHash = session.resolvedHash
            ? "#" + session.resolvedHash
            : "#" + rawHash;

        const proxyUrl = PROXIES[session.proxyIdx];
        session.proxyIdx = (session.proxyIdx + 1) % PROXIES.length;

        worker.send({
            type: "start",
            config: {
                id: botId,
                proxy: { type: "http", url: proxyUrl },
                hash: spawnHash,
                name: botName,
                stats: [0, 0, 0, 0, 0, 0, 0, 9],
                type: "follow",
                token: "follow-8fe6ca",
                autoFire: false,
                autoRespawn: true,
                keys: [],
                keysHold: [],
                tank: selectedTank,
                chatSpam: "",
                initialTarget: { tank: selectedTank },
                squadId: rawHash,
                reconnectAttempts: 3,
                reconnectDelay: 10000,
                arrasCache: arrasScriptCache,
                wasmCache: arrasWasmCache,
                teamColor: session.teamColor
            }
        });

        totalSpawned++;
        printSpawned();
    }

    const sessions = new Map();
    const wss = new WebSocketServer({ server });

    wss.on("connection", (ws, req) => {
        const addr = req.socket.remoteAddress;

        if (!sessions.has(addr)) {
            sessions.set(addr, {
                workers: [],
                pool: [],
                nextBotId: 0,
                tank: "auto6",
                tanks: [],
                tankIdx: 0,
                proxyIdx: 0,
                resolvedHash: null,
                teamColor: null
            });
        }
        const session = sessions.get(addr);
        session.ws = ws;

        let challenge = null;
        let verified = false;

        const packet = (...args) => {
            try { ws.send(pack(args)); } catch {}
        };
        const close = () => {
            try { ws.close(); } catch {}
        };

        ws.on("message", (msg) => {
            try {
                const data = unpack(msg);
                const type = data.shift();

                switch (type) {
                    case "M":
                        if (challenge || data[0] != 72011) return close();
                        challenge = randint(0b1000000000, 0b1111111111);
                        packet("M", challenge);
                        break;

                    case "C":
                        if (data[0] == (challenge ^ 845)) {
                            verified = true;
                            fillPool(session);
                        } else close();
                        break;

                    case "Z":
                        session.tank = data[0];
                        if (Array.isArray(session.tank)) {
                            session.tanks = session.tank;
                            session.tankIdx = 0;
                            for (const w of session.workers) {
                                for (const id of w.botIds) {
                                    const t = session.tanks[session.tankIdx];
                                    w.send({ type: "tankselect", tank: t, botId: id });
                                    session.tankIdx = (session.tankIdx + 1) % session.tanks.length;
                                }
                            }
                        } else {
                            session.tanks = [];
                            for (const w of session.workers) {
                                w.send({ type: "tankselect", tank: session.tank });
                            }
                        }
                        break;

                    case "F":
                        if (!verified) break;
                        {
                            const hash = data[0];
                            let count = 1;
                            let botName = "thara's Bot";
                            const a = data[1];
                            const b = data[2];

                            if (typeof a === "number" || (typeof a === "string" && /^\d+$/.test(String(a)))) {
                                count = Math.max(1, parseInt(a, 10) || 1);
                                botName = String(b ?? "thara's Bot").trim() || "thara's Bot";
                            } else if (typeof b === "number" || (typeof b === "string" && /^\d+$/.test(String(b)))) {
                                botName = String(a ?? "thara's Bot").trim() || "thara's Bot";
                                count = Math.max(1, parseInt(b, 10) || 1);
                            } else {
                                botName = String(a ?? b ?? "thara's Bot").trim() || "thara's Bot";
                                count = 1;
                            }

                            count = Math.min(count, 2000);
                            for (let i = 0; i < count; i++) {
                                spawnBotNow(session, hash, botName);
                            }
                        }
                        break;

                    case "B":
                        if (!verified) break;
                        for (const w of session.workers) {
                            w.send({ type: "destroy" });
                            w.botIds = [];
                            w.activeBots = 0;
                        }
                        session.workers = [];
                        totalSpawned = 0;
                        printSpawned();
                        fillPool(session);
                        break;

                    case "A":
                        if (!verified) break;
                        {
                            const payload = {
                                type: "position",
                                x: data[0], y: data[1],
                                mouseX: data[2], mouseY: data[3],
                                mouseDown: data[4], rMouseDown: data[5],
                                mouse: data[6], feeding: data[7],
                                shift: data[8], autofire: data[9],
                                autospin: data[10], manualMode: data[11],
                                manualX: data[12], manualY: data[13],
                                teamColor: session.teamColor
                            };
                            for (const w of session.workers) w.send(payload);
                        }
                        break;

                    case "T":
                        if (!verified) break;
                        {
                            const payload = { type: "chat", message: data[0], spam: data[1] };
                            for (const w of session.workers) w.send(payload);
                        }
                        break;

                    case "H":
                        if (!verified) break;
                        {
                            const team = String(data[0] || "").toLowerCase().trim();
                            if (["green", "blue", "pink", "purple"].includes(team) && session.teamColor !== team) {
                                session.teamColor = team;
                                for (const w of session.workers) {
                                    w.send({ type: "teamcolor", teamColor: team });
                                }
                            }
                        }
                        break;

                    default:
                        break;
                }
            } catch {}
        });

        ws.on("close", () => {
            for (const w of session.workers) {
                try { w.terminate(); } catch {}
            }
            session.workers = [];
            session.pool = [];
            sessions.delete(addr);
        });

        ws.on("error", noop);
    });

    await fetchProxies();
    await preloadArrasAssets();

    const port = process.env.PORT || 8082;
    server.listen(port, () => {
        process.stdout.write(`[spawned] 0 bots   `);
    });
})();
