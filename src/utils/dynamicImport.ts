/**
 * Utility to handle runtime-specific module resolution.
 * Allows FinStream to swap Node-native modules for Bun-native ones at runtime.
 */
import { isBun } from '../runtime.js';

export async function loadOptimizedModule<T>(nodeModulePath: string, bunModulePath: string): Promise<T> {
    const path = isBun ? bunModulePath : nodeModulePath;
    return import(path);
}
