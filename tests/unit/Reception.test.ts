import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Reception } from '../../src/Reception.js';
import { bus, FinStreamEvents } from '../../src/events.js';

// Mock sqlite database
const mockRun = vi.fn();
const mockAll = vi.fn();
vi.mock('@tsrlib/database', () => ({
    Database: vi.fn().mockImplementation(() => ({
        run: mockRun,
        all: mockAll
    }))
}));

describe('Reception', () => {
    let reception: Reception;

    beforeEach(() => {
        vi.clearAllMocks();
        reception = new Reception();
    });

    it('should request TaskMan to start ingestor on subscription', async () => {
        await reception.initDb();
        const busSpy = vi.spyOn(bus, 'emit');

        await reception.subscribe('user-uuid-1', 'yahoo', ['AAPL']);

        expect(mockRun).toHaveBeenCalledWith(expect.stringContaining('INSERT INTO subscriptions'), expect.any(Array));
        expect(busSpy).toHaveBeenCalledWith(FinStreamEvents.SUBSCRIPTION_REQUEST, { ingestorId: 'yahoo', action: 'start' });
    });

    it('should route data to connected clients based on subscriptions', async () => {
        await reception.initDb();
        const mockWs = { readyState: 1, send: vi.fn() };
        reception.registerSocket('user-uuid-1', mockWs);

        // Mock DB returning a subscription
        mockAll.mockResolvedValue([{ uuid: 'user-uuid-1', symbols: '["AAPL"]' }]);

        // Simulate incoming data
        await (reception as any).distributeData({
            ingestorId: 'yahoo',
            symbol: 'AAPL',
            data: { price: 100 },
            receivedAt: 123
        });

        expect(mockWs.send).toHaveBeenCalled();
    });
});
