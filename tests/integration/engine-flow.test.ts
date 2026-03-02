import { describe, it, expect, vi, beforeEach } from 'vitest';
import { bus, FinStreamEvents } from '../../src/events.js';
import { TaskMan } from '../../src/TaskMan.js';
import { Reception } from '../../src/Reception.js';
import { MarketState } from '../../src/types.js';

// Mock dependencies
vi.mock('@tsrlib/database', () => ({
    Database: vi.fn().mockImplementation(() => ({
        run: vi.fn(),
        all: vi.fn().mockResolvedValue([{ uuid: 'test-uuid', symbols: '[]' }]) // Subscribed to all
    }))
}));

vi.mock('../../src/ConfigLoader.js', () => ({
    ConfigLoader: { get: vi.fn().mockReturnValue({ executable: 'dummy', params: {} }) }
}));

describe('FinStream Engine Integration Flow', () => {
    let taskMan: TaskMan;
    let reception: Reception;

    beforeEach(async () => {
        vi.clearAllMocks();
        taskMan = new TaskMan();
        reception = new Reception();
        await reception.initDb();
    });

    it('should handle full data pipeline: Sub -> Start -> Data -> Route -> Kill', async () => {
        // 1. Setup WS Client
        const wsClient = { readyState: 1, send: vi.fn() };
        reception.registerSocket('test-uuid', wsClient);

        // 2. Open Market
        bus.emitMarketStatus({ state: MarketState.OPEN, timestamp: '', isLive: true });

        // 3. User Subscribes (Should trigger TaskMan to start ingestor)
        const startSpy = vi.spyOn(taskMan as any, 'startIngestor');
        await reception.subscribe('test-uuid', 'yahoo', []);
        
        expect(startSpy).toHaveBeenCalledWith('yahoo');

        // 4. Simulate Ingestor producing data
        bus.emitData({
            ingestorId: 'yahoo',
            symbol: 'MSFT',
            data: { price: 300 },
            receivedAt: Date.now()
        });

        // 5. Verify Reception routed data to the WebSocket
        // (Wait a tick for async distributeData to resolve)
        await new Promise(resolve => setTimeout(resolve, 10));
        expect(wsClient.send).toHaveBeenCalledWith(expect.stringContaining('"symbol":"MSFT"'));

        // 6. Close Market (Should trigger TaskMan to kill processes)
        const killSpy = vi.spyOn(taskMan, 'stopAllIngestors');
        bus.emitMarketStatus({ state: MarketState.CLOSED, timestamp: '', isLive: false });

        expect(killSpy).toHaveBeenCalledWith("Market Closed");
    });
});
