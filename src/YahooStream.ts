import { YahooStreaming } from '@tsrlib/markets';
import { Logger } from './Logger.js';

/**
 * YahooStream Ingestor
 * Connects to Yahoo Finance via tsrlib and pipes data to stdout as JSON.
 */
export class YahooStreamIngestor {
    private streamer: YahooStreaming;

    constructor(symbols: string[]) {
        this.streamer = new YahooStreaming(symbols);
        
        // Listen to tsrlib events and pipe to STDOUT for TaskMan
        this.streamer.on('data', (data) => {
            console.log(JSON.stringify({
                symbol: data.symbol,
                price: data.price,
                timestamp: Date.now(),
                source: 'yahoo'
            }));
        });

        this.streamer.on('error', (err) => {
            console.error(`YahooStream Error: ${err.message}`);
        });
    }

    start() {
        Logger.info("YahooStream Ingestor service initialized.");
        this.streamer.connect();
    }
}

// If executed directly (as a child process by TaskMan)
if (import.meta.url === `file://${process.argv[1]}`) {
    const symbols = JSON.parse(process.argv[2] || '[]');
    const service = new YahooStreamIngestor(symbols);
    service.start();
}
