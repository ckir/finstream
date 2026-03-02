import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Telemetry } from '../../src/Telemetry.js';
import { bus, FinStreamEvents } from '../../src/events.js';

describe('Telemetry', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 1024 * 1024 * 50 } as any); // 50MB
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should calculate throughput and emit snapshot', () => {
        const telemetry = new Telemetry(60);
        const emitSpy = vi.spyOn(bus, 'emitTelemetry');

        telemetry.startSnapshotTimer();

        // Simulate 120 messages
        for (let i = 0; i < 120; i++) {
            bus.emit(FinStreamEvents.INGESTOR_DATA, {});
        }

        // Advance timer by 60 seconds
        vi.advanceTimersByTime(60000);

        expect(emitSpy).toHaveBeenCalledWith(expect.objectContaining({
            memoryUsage: 50,
            messagesPerSecond: 2 // 120 msgs / 60 sec
        }));

        telemetry.stop();
    });
});
