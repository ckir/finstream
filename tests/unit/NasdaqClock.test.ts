import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { NasdaqClock } from '../../src/NasdaqClock.js';
import { bus, FinStreamEvents } from '../../src/events.js';
import { MarketState } from '../../src/types.js';

// Mock tsrlib
vi.mock('@tsrlib/markets', () => ({
    MarketStatus: {
        getNasdaqStatus: vi.fn().mockResolvedValue({ status: 'Open' })
    }
}));

describe('NasdaqClock', () => {
    beforeEach(() => {
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.clearAllTimers();
    });

    it('should start and emit MARKET_STATUS_CHANGED on initial tick', async () => {
        const clock = new NasdaqClock(10, 3600);
        const emitSpy = vi.spyOn(bus, 'emit');

        await clock.start();

        expect(emitSpy).toHaveBeenCalledWith(
            FinStreamEvents.MARKET_STATUS_CHANGED, 
            expect.objectContaining({ state: MarketState.OPEN, isLive: true })
        );
        clock.stop();
    });
});
