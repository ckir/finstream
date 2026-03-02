import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import express from 'express';
import path from 'path';
import { bus, FinStreamEvents } from './events.js';
import { Logger } from './Logger.js';
import { MarketState } from './types.js';
import { Reception } from './Reception.js';

/**
 * ServerStarter: The Gateway
 * Manages HTTP/REST and WebSocket communication for Admins and Consumers.
 */
export class ServerStarter {
    private app = express();
    private server = createServer(this.app);
    private reception: Reception;
    private marketState: MarketState = MarketState.UNKNOWN;

    // Separate WS servers for clear role isolation
    private consumerWs: WebSocketServer;
    private adminWs: WebSocketServer;

    constructor(reception: Reception) {
        this.reception = reception;
        
        // Setup WS Servers
        this.consumerWs = new WebSocketServer({ noServer: true });
        this.adminWs = new WebSocketServer({ noServer: true });

        // Sync local market state to deny consumers when closed
        bus.on(FinStreamEvents.MARKET_STATUS_CHANGED, (status) => {
            this.marketState = status.state;
        });

        this.setupRoutes();
        this.setupUpgradeHandler();
    }

    private setupRoutes() {
        this.app.use(express.json());

        // 1. Health Endpoint (Includes Clock Status)
        this.app.get('/health', (req, res) => {
            res.json({ status: 'OK', market: this.marketState, time: new Date().toISOString() });
        });

        // 2. Docs Endpoint (Static TypeDoc)
        const docsPath = path.resolve('docs/api');
        this.app.use('/docs', express.static(docsPath));

        // 3. Consumer API /api/v1/
        this.app.get('/api/v1/registry/new', async (req, res) => {
            const uuid = await this.reception.registerConsumer();
            res.json({ uuid });
        });

        this.app.post('/api/v1/assets/subscribe/:id', async (req, res) => {
            const { uuid, symbols } = req.body;
            await this.reception.subscribe(uuid, req.params.id, symbols || []);
            res.json({ success: true });
        });

        // 4. Admin API /api/v1/admin/
        this.app.get('/api/v1/admin/status', (req, res) => {
            res.json({ message: "Admin authenticated via Caddy" });
        });
    }

    private setupUpgradeHandler() {
        this.server.on('upgrade', (request, socket, head) => {
            const pathname = new URL(request.url!, `http://${request.headers.host}`).pathname;

            if (pathname === '/ws/admin') {
                this.adminWs.handleUpgrade(request, socket, head, (ws) => {
                    this.adminWs.emit('connection', ws, request);
                });
            } else if (pathname === '/ws/consumer') {
                // Check if market is closed before allowing upgrade
                if (this.marketState === MarketState.CLOSED) {
                    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
                    socket.destroy();
                    return;
                }
                this.consumerWs.handleUpgrade(request, socket, head, (ws) => {
                    this.consumerWs.emit('connection', ws, request);
                });
            } else {
                socket.destroy();
            }
        });

        this.setupWsLogic();
    }

    private setupWsLogic() {
        // Consumer logic: link UUID to the socket in Reception
        this.consumerWs.on('connection', (ws, req) => {
            const uuid = new URL(req.url!, `http://${req.headers.host}`).searchParams.get('uuid');
            if (uuid) {
                this.reception.registerSocket(uuid, ws);
                Logger.info(`Consumer WebSocket connected: ${uuid}`);
            }
        });

        // Admin logic: listen to bus and push telemetry
        this.adminWs.on('connection', (ws) => {
            Logger.info("Admin WebSocket connected.");
            const telemetryHandler = (data: any) => {
                if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
            };
            bus.on(FinStreamEvents.TELEMETRY_UPDATE, telemetryHandler);
            ws.on('close', () => bus.off(FinStreamEvents.TELEMETRY_UPDATE, telemetryHandler));
        });
    }

    public start(port: number = 3000) {
        this.server.listen(port, () => {
            Logger.info(`ServerStarter listening on port ${port}`);
        });
    }
}
