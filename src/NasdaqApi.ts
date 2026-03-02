/**
 * NasdaqApi Ingestor (Stub)
 * Future implementation will use @tsrlib/markets ApiNasdaqUnlimited.
 */
export class NasdaqApiIngestor {
    constructor(symbols: string[]) {
        console.error("NasdaqApi Ingestor is currently a stub.");
    }

    start() {
        // Periodic heartbeat so TaskMan knows it's "alive"
        setInterval(() => {
            console.log(JSON.stringify({ 
                symbol: 'STUB', 
                status: 'waiting_for_tsrlib_module',
                timestamp: Date.now() 
            }));
        }, 5000);
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    const service = new NasdaqApiIngestor([]);
    service.start();
}
