import { EventEmitter } from 'events';
import { MarketStatus as TsrMarketStatus } from '@tsrlib/markets';
import { DateTime } from 'luxon';
import { bus } from './events.js';
import { MarketState, MarketStatus } from './types.js';
import { Logger } from './Logger.js';

/**
 * NasdaqClock: The System Metronome
 * Polls Nasdaq API and dictates system state.
 */
export class NasdaqClock {
    private intervalLive: number;
    private intervalClosed: number;
    private timer: Timer | null = null;
    private lastStatus: MarketState = MarketState.UNKNOWN;

    constructor(liveIntervalSec: number = 10, closedIntervalSec: number = 3600) {
        this.intervalLive = liveIntervalSec * 1000;
        this.intervalClosed = closedIntervalSec * 1000;
    }

    public async start() {
        Logger.info("NasdaqClock started.");
        await this.tick();
    }

    private async tick() {
        try {
            // Using tsrlib's MarketStatus to check Nasdaq
            const status = await TsrMarketStatus.getNasdaqStatus();
            
            const marketState = this.mapState(status.status);
            const isLive = marketState !== MarketState.CLOSED;

            const currentStatus: MarketStatus = {
                state: marketState,
                timestamp: DateTime.now().setZone('America/New_York').toISO()!,
                isLive
            };

            // Only emit and log if the state actually changed
            if (marketState !== this.lastStatus) {
                this.lastStatus = marketState;
                bus.emitMarketStatus(currentStatus);
            }

            // Schedule next tick based on market state
            const nextInterval = isLive ? this.intervalLive : this.intervalClosed;
            this.timer = setTimeout(() => this.tick(), nextInterval);

        } catch (error) {
            Logger.error("NasdaqClock polling error. Inferring state from last known.", error);
            // Fallback: retry in 30 seconds if API is down
            this.timer = setTimeout(() => this.tick(), 30000);
        }
    }

    private mapState(tsrStatus: string): MarketState {
        switch (tsrStatus.toLowerCase()) {
            case 'open': return MarketState.OPEN;
            case 'pre-market': return MarketState.PRE_MARKET;
            case 'after-hours': return MarketState.AFTER_HOURS;
            case 'closed': return MarketState.CLOSED;
            default: return MarketState.UNKNOWN;
        }
    }

    public stop() {
        if (this.timer) clearTimeout(this.timer);
    }
}
