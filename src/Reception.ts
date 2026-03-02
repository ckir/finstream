import os from 'os';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { Database } from '@tsrlib/database';
import { bus, FinStreamEvents } from './events.js';
import { Logger } from './Logger.js';
import { TickData } from './types.js';

/**
 * Reception: Consumer & Subscription Manager
 * Handles SQLite session persistence and data routing.
 */
export class Reception {
    private db!: Database;
    private dbPath = path.join(os.tmpdir(), 'finstream.db');
    private activeConnections: Map<string, any> = new Map(); // UUID -> WebSocket

    constructor() {
        // Listen for data from TaskMan and distribute to subscribers
        bus.on(FinStreamEvents.INGESTOR_DATA, (data: TickData) => {
            this.distributeData(data);
        });
    }

    async initDb() {
        Logger.info(`Initialising session database at ${this.dbPath}`);
        this.db = new Database(this.dbPath);
        
        await this.db.run(`
            CREATE TABLE IF NOT EXISTS consumers (
                uuid TEXT PRIMARY KEY,
                last_seen INTEGER
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                uuid TEXT,
                ingestor_id TEXT,
                symbols TEXT,
                FOREIGN KEY(uuid) REFERENCES consumers(uuid)
            );
        `);
    }

    async registerConsumer(): Promise<string> {
        const uuid = uuidv4();
        await this.db.run('INSERT INTO consumers (uuid, last_seen) VALUES (?, ?)', [uuid, Date.now()]);
        return uuid;
    }

    async subscribe(uuid: string, ingestorId: string, symbols: string[]) {
        // symbols stored as JSON string in SQLite
        await this.db.run(
            'INSERT INTO subscriptions (uuid, ingestor_id, symbols) VALUES (?, ?, ?)',
            [uuid, ingestorId, JSON.stringify(symbols)]
        );

        // Notify TaskMan to ensure this ingestor is running
        bus.emit(FinStreamEvents.SUBSCRIPTION_REQUEST, { ingestorId, action: 'start' });
        Logger.info(`Consumer ${uuid} subscribed to ${ingestorId} for symbols: ${symbols}`);
    }

    private async distributeData(tick: TickData) {
        // Find all consumers subscribed to this ingestor
        const subscribers = await this.db.all(
            'SELECT uuid, symbols FROM subscriptions WHERE ingestor_id = ?', 
            [tick.ingestorId]
        );

        for (const sub of subscribers) {
            const symbols = JSON.parse(sub.symbols);
            // If it's a 'pricing' type with specific symbols, filter; otherwise send all
            if (symbols.length === 0 || symbols.includes(tick.symbol)) {
                const ws = this.activeConnections.get(sub.uuid);
                if (ws && ws.readyState === 1) { // 1 = OPEN
                    ws.send(JSON.stringify(tick));
                }
            }
        }
    }

    // Called by ServerStarter when a WS connects
    registerSocket(uuid: string, ws: any) {
        this.activeConnections.set(uuid, ws);
    }
}
