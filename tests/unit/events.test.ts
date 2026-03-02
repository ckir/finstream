import { describe, it, expect, vi } from 'vitest';
import { FinStreamBus, FinStreamEvents } from '../../src/events.js';
import { MarketState } from '../../src/types.js';

describe('FinStreamBus', () => {
    it('should emit and listen to MARKET_STATUS_CHANGED', () => {
        const bus = new FinStreamBus();
        const spy = vi.fn();
        bus.on(FinStreamEvents.MARKET_STATUS_CHANGED, spy);

        const status = { state: MarketState.OPEN, timestamp: '2024-01-01', isLive: true };
        bus.emitMarketStatus(status);

        expect(spy).toHaveBeenCalledWith(status);
    });

    it('should emit and listen to INGESTOR_DATA', () => {
        const bus = new FinStreamBus();
        const spy = vi.fn();
        bus.on(FinStreamEvents.INGESTOR_DATA, spy);

        const data = { ingestorId: 'yahoo', symbol: 'AAPL', data: { price: 150 }, receivedAt: 123 };
        bus.emitData(data);

        expect(spy).toHaveBeenCalledWith(data);
    });
});
