import { describe, it, expect, vi } from 'vitest';
import { getRuntime, isBun, isNode } from '../../src/runtime.js';

describe('Runtime Detector', () => {
    it('should accurately detect the runtime', () => {
        const runtime = getRuntime();
        expect(['bun', 'node']).toContain(runtime);
        if (typeof Bun !== 'undefined') {
            expect(isBun).toBe(true);
            expect(isNode).toBe(false);
        } else {
            expect(isBun).toBe(false);
            expect(isNode).toBe(true);
        }
    });
});
