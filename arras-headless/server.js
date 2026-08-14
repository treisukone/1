(async () => {
    const { Worker } = await import("worker_threads");
    const path = await import("path");
    const { WebSocketServer } = await import("ws");
    const { pack, unpack } = await import("msgpackr");
    const http = await import("http");
    const fetchModule = await import("node-fetch");
    const fs = await import("fs");
    const realFetch = fetchModule.default || fetchModule;

    const noop = () => {};
const _clog = console.log; console.log = noop; console.error = noop; console.warn = noop;
    let PROXIES = ["http://oXjFaNnSLL6-zone-custom-region-US:f9b1dcca498d@proxy.ipmax.cc:10000"];
    const prod = false;
    const WORKER_MEMORY_MB = 96;
    const BOTS_PER_WORKER = 8;
    const PREWARM_POOL_SIZE = 8;
    const SPAWN_BASE_DELAY_MS = 30;
    const SPAWN_JITTER_MS = 60;

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
        const scriptTagStart = html.indexOf("<script>");
        if (scriptTagStart === -1) {
            throw new Error("Could not find arras script tag");
        }
        const scriptStart = scriptTagStart + 8;
        const scriptTagEnd = html.indexOf("</script", scriptStart);
        if (scriptTagEnd === -1) {
            throw new Error("Could not find arras script close tag");
        }
        return html.slice(scriptStart, scriptTagEnd);
    }

    async function preloadArrasAssets() {
        try {
            console.log("Preloading arras script + wasm...");
            const [htmlRes, wasmRes] = await Promise.all([
                realFetch("https://arras.io"),
                realFetch("https://arras.io/app.wasm")
            ]);

            const html = await htmlRes.text();
            const wasm = await wasmRes.arrayBuffer();

            arrasScriptCache = extractArrasScript(html);
            arrasWasmCache = new Uint8Array(wasm);
            console.log("Arras preload ready:", arrasScriptCache.length, "script chars,", arrasWasmCache.byteLength, "wasm bytes");
        } catch (err) {
            console.error("Arras preload failed. Bots will fall back to per-worker fetch:", err);
        }
    }

    function createBotWorker(session) {
        const worker = new Worker(botWorkerPath, {
            resourceLimits: {
                maxOldGenerationSizeMb: WORKER_MEMORY_MB,
                maxYoungGenerationSizeMb: 32,
                codeRangeSizeMb: 32,
            }
        });
        worker.send = (message) => worker.postMessage(message);
        worker.botId = null;
        worker.botIds = [];
        worker.activeBots = 0;
        worker.isPooled = false;
        worker.resolvedHash = null;
        worker.on("error", (err) => {
            console.error(`Bot worker ${worker.botId ?? "?"} error:`, err);
        });
        worker.on("message", (message) => {
            if (!message) return;
            if (message.type === "log") {
                /* bot log silenced */
            } else if (message.type === "died") {
                const idx = worker.botIds.indexOf(message.id);
                if (idx !== -1) worker.botIds.splice(idx, 1);
                worker.activeBots = Math.max(0, worker.activeBots - 1);
            } else if (message.type === "hash_update") {
                if (message.hash) {
                    worker.resolvedHash = message.hash;
                    if (session) {
                        session.resolvedHash = message.hash;
                        if (session.ws) session.ws.send(pack(["R", message.hash]));
                    }
                }
            }
        });
        worker.on("exit", (code) => {
            let idx = session.workers.indexOf(worker);
            if (idx !== -1) session.workers.splice(idx, 1);
            idx = session.pool.indexOf(worker);
            if (idx !== -1) session.pool.splice(idx, 1);
            // worker exit silenced
        });
        return worker;
    }

    function prepareWorker(worker) {
        worker.send({
            type: "prepare",
            arrasCache: arrasScriptCache,
            wasmCache: arrasWasmCache,
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
        let worker = session.workers.find((candidate) => candidate.activeBots < BOTS_PER_WORKER);
        if (worker) return worker;

        worker = session.pool.shift() || createBotWorker(session);
        worker.isPooled = false;
        if (!session.workers.includes(worker)) {
            session.workers.push(worker);
        }
        return worker;
    }

    function queueBotSpawn(session, hash, botName) {
        // Spawn immediately without queuing delays
        spawnBotNow(session, hash, botName);
    }

    function spawnBotNow(session, hash, botName) {
        if (PROXIES.length === 0) {
            return;
            return;
        }
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

        const spawnHash = session.resolvedHash ? "#" + session.resolvedHash : "#" + hash;
        worker.send({
            type: "start", config: {
                id: botId,
                proxy: {
                    type: "http",
                    url: PROXIES[session.proxyIdx]
                },
                hash: spawnHash,
                name: botName,
                stats: [0, 0, 0, 0, 0, 0, 0, 9],
                type: "follow",
                token: "follow-8fe6ca",
                autoFire: false,
                autoRespawn: true,
                keys: [],
                keysHold: [],
                tank: "Auto4",
                chatSpam: "",
                initialTarget: { tank: selectedTank },
                squadId: hash,
                reconnectAttempts: 5,
                reconnectDelay: 8000,
                arrasCache: arrasScriptCache,
                wasmCache: arrasWasmCache,
                teamColor: session.teamColor,
            }
        });

        if (PROXIES.length > 0) session.proxyIdx = (session.proxyIdx + 1) % PROXIES.length;
    }

    const sessions = new Map();
    const wss = new WebSocketServer({ server });

    wss.on("connection", (ws, req) => {
        const addr = req.socket.remoteAddress;
        // connection silenced

        if (!sessions.has(addr)) {
            sessions.set(addr, {
                workers: [],
                pool: [],
                spawnQueue: [],
                spawnQueueActive: false,
                spawnTimer: null,
                nextBotId: 0,
                tank: "auto6",
                tanks: [],
                tankIdx: 0,
                proxyIdx: 0,
                resolvedHash: null,
                teamColor: null,
            });
        }
        const session = sessions.get(addr);
        session.ws = ws;

        let challenge;
        let verified = false;

        function packet(...args) {
            ws.send(pack(args));
        }

        function close() {
            ws.close();
        }

        ws.on("message", (msg) => {
            try {
                const data = unpack(msg);
                const type = data.shift();

                switch (type) {
                    case "M":
                        if (challenge || data[0] != 72011) {
                            close();
                            return;
                        }
                        challenge = randint(0b1000000000, 0b1111111111);
                        packet("M", challenge);
                        break;

                    case "C":
                        if (data[0] == (challenge ^ 845)) {
                            verified = true;
                            // verify silenced
                            fillPool(session);
                        } else {
                            close();
                            console.log(addr, "true noob");
                        }
                        break;

                    case "Z":
                        session.tank = data[0];
                        if (session.tank instanceof Array) {
                            session.tanks = session.tank;
                            session.tankIdx = 0;
                            for (const worker of session.workers) {
                                for (const botId of worker.botIds) {
                                    const t = session.tanks[session.tankIdx];
                                    worker.send({ type: "tankselect", tank: t, botId });
                                    session.tankIdx = (session.tankIdx + 1) % session.tanks.length;
                                }
                            }
                        } else {
                            session.tanks = [];
                            for (const worker of session.workers) {
                                worker.send({ type: "tankselect", tank: session.tank });
                            }
                        }
                        break;

                    case "F":
                        if (verified) {
                            const hash = data[0];
                            const name = data[1];
                            const count = data[2] || 1;
                            for (let i = 0; i < count; i++) {
                                queueBotSpawn(session, hash, name);
                            }
                        }
                        break;

                    case "R":
                        if (verified) {
                            const hash = data[0];
                            const name = data[1];
                            const count = data[2] || 1;
                            for (let i = 0; i < count; i++) {
                                queueBotSpawn(session, hash, name);
                            }
                        }
                        break;

                    case "K":
                        if (verified) {
                            const hash = data[0];
                            for (const worker of session.workers) {
                                worker.send({ type: "kill", hash: "#" + hash });
                            }
                        }
                        break;

                    case "T":
                        if (verified) {
                            const tank = data[0];
                            for (const worker of session.workers) {
                                worker.send({ type: "tankselect", tank });
                            }
                        }
                        break;

                    case "A":
                        if (verified) {
                            const flag = data[0];
                            for (const worker of session.workers) {
                                worker.send({ type: "autofire", flag });
                            }
                        }
                        break;

                    case "P":
                        if (verified) {
                            const key = data[0];
                            const state = data[1];
                            for (const worker of session.workers) {
                                worker.send({ type: "key", key, state });
                            }
                        }
                        break;

                    case "H":
                        if (verified) {
                            const key = data[0];
                            const state = data[1];
                            for (const worker of session.workers) {
                                worker.send({ type: "keyhold", key, state });
                            }
                        }
                        break;

                    case "S":
                        if (verified) {
                            const msg = data[0];
                            for (const worker of session.workers) {
                                worker.send({ type: "chat", msg });
                            }
                        }
                        break;

                    case "X":
                        if (verified) {
                            const color = data[0];
                            session.teamColor = color;
                            for (const worker of session.workers) {
                                worker.send({ type: "teamcolor", color });
                            }
                        }
                        break;

                    case "Q":
                        if (verified) {
                            for (const worker of session.workers) {
                                worker.terminate();
                            }
                            session.workers = [];
                            session.pool = [];
                            session.spawnQueue = [];
                            session.spawnQueueActive = false;
                            if (session.spawnTimer) {
                                clearTimeout(session.spawnTimer);
                                session.spawnTimer = null;
                            }
                        }
                        break;

                    case "D":
                        close();
                        break;

                    default:
                        break;
                }
            } catch (err) {
                console.error("ws message error:", err);
            }
        });

        ws.on("close", () => {
            for (const worker of session.workers) {
                worker.terminate();
            }
            session.workers = [];
            session.pool = [];
            session.spawnQueue = [];
            session.spawnQueueActive = false;
            if (session.spawnTimer) {
                clearTimeout(session.spawnTimer);
                session.spawnTimer = null;
            }
            sessions.delete(addr);
        });

        ws.on("error", (err) => {
            console.error("ws error:", err);
        });
    });

    await preloadArrasAssets();

    const PORT = process.env.PORT || 8082;
    server.listen(PORT, () => {
        console.log(`Server listening on port ${PORT}`);
    });
})();
