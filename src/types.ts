/**
 * Shared Domain Types for FinStream
 */

export type Runtime = 'bun' | 'node';

export enum MarketState {
  PRE_MARKET = 'PRE_MARKET',
  OPEN = 'OPEN',
  AFTER_HOURS = 'AFTER_HOURS',
  CLOSED = 'CLOSED',
  UNKNOWN = 'UNKNOWN'
}

export interface MarketStatus {
  state: MarketState;
  timestamp: string;
  isLive: boolean; 
}

export type IngestorType = 'pricing' | 'info';

export interface IngestorConfig {
  id: string;
  type: IngestorType;
  executable: string;
  params: Record<string, any>;
}

export interface TelemetrySnapshot {
  timestamp: string;
  uptime: number;
  memoryUsage: number;
  activeSubscriptions: number;
  ingestorStatuses: Record<string, 'running' | 'stopped' | 'error'>;
  messagesPerSecond: number;
}

export interface TickData {
  ingestorId: string;
  symbol: string;
  data: any;
  receivedAt: number;
}
