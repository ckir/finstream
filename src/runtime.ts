import { Runtime } from './types.js';

/**
 * Runtime detection utility to distinguish between Bun and Node.js.
 */
export const getRuntime = (): Runtime => {
  return (typeof Bun !== 'undefined') ? 'bun' : 'node';
};

export const isBun = getRuntime() === 'bun';
export const isNode = getRuntime() === 'node';
