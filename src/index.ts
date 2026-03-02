/**
 * FinStream Entry Point
 * Orchestrates the lifecycle of the data engine.
 */
import { getRuntime } from './runtime.js';
import { bus, FinStreamEvents } from './events.js';
import { Logger } from './Logger.js';
import { ConfigLoader } from './ConfigLoader.js';

async function main() {
    const runtime = getRuntime();
    
    // 1. Initialise Logger (already configured via tsrlib wrapper)
    Logger.info(`FinStream Engine starting on ${runtime.toUpperCase()}`);

    try {
        // 2. Load Configuration
        Logger.debug("Loading system configuration...");
        await ConfigLoader.load();

        // 3. Initialize the Metronome (NasdaqClock)
        // We import it dynamically if we need runtime-specific versions later
        const { NasdaqClock } = await import('./NasdaqClock.js');
        const clock = new NasdaqClock();

        // 4. Initialise Core Modules
        const { TaskMan } = await import('./TaskMan.js');
        const { Reception } = await import('./Reception.js');
        const { Telemetry } = await import('./Telemetry.js');
        const { ServerStarter } = await import('./ServerStarter.js');

        const reception = new Reception();
        const taskMan = new TaskMan();
        const telemetry = new Telemetry();
        const server = new ServerStarter();

        // 5. Wire System Events
        bus.on(FinStreamEvents.MARKET_STATUS_CHANGED, (status) => {
            Logger.info(`Market Status Change: ${status.state} (Live: ${status.isLive})`);
        });

        // 6. Start the Engines
        await reception.initDb();
        clock.start();
        telemetry.startSnapshotTimer();
        server.start();

        Logger.info("FinStream Services Operational.");

    } catch (error) {
        Logger.error("Fatal startup error", { error });
        process.exit(1);
    }
}

main();
