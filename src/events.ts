import { EventEmitter } from 'events';
import { MarketStatus, TickData, TelemetrySnapshot } from './types.js';

/**
 * Central Event Bus for internal module communication.
 */
export enum FinStreamEvents {
  MARKET_STATUS_CHANGED = 'MARKET_STATUS_CHANGED',
  INGESTOR_DATA = 'INGESTOR_DATA',
  TELEMETRY_UPDATE = 'TELEMETRY_UPDATE',
  SUBSCRIPTION_REQUEST = 'SUBSCRIPTION_REQUEST'
}

export class FinStreamBus extends EventEmitter {
  emitMarketStatus(status: MarketStatus) {
    this.emit(FinStreamEvents.MARKET_STATUS_CHANGED, status);
  }

  emitData(data: TickData) {
    this.emit(FinStreamEvents.INGESTOR_DATA, data);
  }

  emitTelemetry(stats: TelemetrySnapshot) {
    this.emit(FinStreamEvents.TELEMETRY_UPDATE, stats);
  }
}

// Global Singleton
export const bus = new FinStreamBus();
