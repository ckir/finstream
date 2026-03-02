import { describe, it, expect, vi, beforeEach } from 'vitest';
import { TaskMan } from '../../src/TaskMan.js';
import { bus, FinStreamEvents } from '../../src/events.js';
import { MarketState } from '../../src/types.js';

vi.mock('../../src/ConfigLoader.js', () => ({
    ConfigLoader: {
        get: vi.fn().mockReturnValue({ executable: 'dummy.exe', params: {} })
    }
}));

vi.mock('child_process', () => ({
    spawn: vi.fn().mockReturnValue({
        stdout: { on: vi.fn() },
        stderr: { on: vi.fn() },
        on: vi.fn(),
        kill: vi.fn()
    })
}));

describe('TaskMan', () => {
    let taskMan: TaskMan;

    beforeEach(() => {
        vi.clearAllMocks();
        taskMan = new TaskMan();
    });

    it('should kill all processes when market closes', () => {
        const killSpy = vi.fn();
        // Force an active ingestor into the private map for testing
        (taskMan as any).activeIngestors.set('test-ingestor', { kill: killSpy });

        // Simulate market close
        bus.emit(FinStreamEvents.MARKET_STATUS_CHANGED, { state: MarketState.CLOSED, isLive: false });

        expect(killSpy).toHaveBeenCalledWith(expect.any(String)); // 'SIGKILL' or undefined depending on runtime
        expect((taskMan as any).activeIngestors.size).toBe(0);
    });
});
