import { spawn, ChildProcess } from 'child_process';
import { isBun } from './runtime.js';
import { bus, FinStreamEvents } from './events.js';
import { Logger } from './Logger.js';
import { ConfigLoader } from './ConfigLoader.js';
import { IngestorConfig, MarketStatus, MarketState } from './types.js';

/**
 * TaskMan: The Ingestor Supervisor
 * Manages external processes and pipes data to the system bus.
 */
export class TaskMan {
    private activeIngestors: Map<string, ChildProcess | any> = new Map();
    private marketLive: boolean = false;

    constructor() {
        // Listen for market status changes
        bus.on(FinStreamEvents.MARKET_STATUS_CHANGED, (status: MarketStatus) => {
            this.marketLive = status.isLive;
            if (!this.marketLive) {
                this.stopAllIngestors("Market Closed");
            }
        });

        // Listen for subscription events from Reception
        bus.on(FinStreamEvents.SUBSCRIPTION_REQUEST, (data: { ingestorId: string, action: 'start' | 'stop' }) => {
            if (this.marketLive && data.action === 'start') {
                this.startIngestor(data.ingestorId);
            }
        });
    }

    private startIngestor(id: string) {
        if (this.activeIngestors.has(id)) return;

        const config = ConfigLoader.get<IngestorConfig>(`ingestors.${id}`);
        if (!config) {
            Logger.error(`Configuration for ingestor ${id} not found.`);
            return;
        }

        Logger.info(`Starting ingestor: ${id} (${config.executable})`);

        // Handle Bun vs Node spawning
        const child = isBun 
            ? (Bun as any).spawn([config.executable], { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' })
            : spawn(config.executable, [], { stdio: ['pipe', 'pipe', 'pipe'] });

        this.setupPipes(id, child);
        this.activeIngestors.set(id, child);
    }

    private setupPipes(id: string, child: any) {
        // Handle Data (Stdout)
        const stdout = isBun ? child.stdout : child.stdout;
        
        // Simple line-by-line JSON parser for the "Pure Pipe"
        let buffer = '';
        stdout.on('data', (chunk: Buffer) => {
            buffer += chunk.toString();
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
                try {
                    const data = JSON.parse(line);
                    bus.emitData({
                        ingestorId: id,
                        symbol: data.symbol || 'unknown',
                        data: data,
                        receivedAt: Date.now()
                    });
                } catch (e) {
                    Logger.debug(`Malformed JSON from ${id}: ${line}`);
                }
            }
        });

        // Handle Logs (Stderr)
        child.stderr.on('data', (data: Buffer) => {
            Logger.warn(`Ingestor [${id}] log: ${data.toString().trim()}`);
        });

        // Handle Crash/Exit
        child.on('exit', (code: number) => {
            this.activeIngestors.delete(id);
            Logger.error(`Ingestor [${id}] exited with code ${code}`);
            
            // Auto-restart if market is still live
            if (this.marketLive) {
                Logger.info(`Restarting ingestor ${id} in 5s...`);
                setTimeout(() => this.startIngestor(id), 5000);
            }
        });
    }

    public stopAllIngestors(reason: string) {
        Logger.info(`Stopping all ingestors. Reason: ${reason}`);
        for (const [id, child] of this.activeIngestors) {
            if (isBun) child.kill();
            else child.kill('SIGKILL'); // Hard kill per requirement
            this.activeIngestors.delete(id);
        }
    }
}
