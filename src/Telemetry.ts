import { bus, FinStreamEvents } from './events.js';
import { Logger } from './Logger.js';
import { TelemetrySnapshot } from './types.js';

/**
 * Telemetry: System Usage Statistics Collector
 * Forwards snapshots to Admin WS and logs periodically.
 */
export class Telemetry {
    private intervalMs: number;
    private timer: Timer | null = null;
    private startTime: number = Date.now();
    private messageCount: number = 0;

    constructor(intervalSec: number = 60) {
        this.intervalMs = intervalSec * 1000;

        // Count incoming data messages to calculate throughput
        bus.on(FinStreamEvents.INGESTOR_DATA, () => {
            this.messageCount++;
        });
    }

    public startSnapshotTimer() {
        Logger.info(`Telemetry started with ${this.intervalMs / 1000}s interval.`);
        this.timer = setInterval(() => this.takeSnapshot(), this.intervalMs);
    }

    private takeSnapshot() {
        const memory = process.memoryUsage();
        
        const snapshot: TelemetrySnapshot = {
            timestamp: new Date().toISOString(),
            uptime: Math.floor((Date.now() - this.startTime) / 1000),
            memoryUsage: Math.floor(memory.rss / 1024 / 1024), // MB
            activeSubscriptions: 0, // Injected by Reception in a real scenario
            ingestorStatuses: {},   // Injected by TaskMan in a real scenario
            messagesPerSecond: Math.floor(this.messageCount / (this.intervalMs / 1000))
        };

        // Reset throughput counter for next window
        this.messageCount = 0;

        // 1. Forward to Admin Channel via Bus
        bus.emitTelemetry(snapshot);

        // 2. Log to Vector sink
        Logger.info("System Telemetry Snapshot", { telemetry: snapshot });
    }

    public stop() {
        if (this.timer) clearInterval(this.timer);
    }
}
