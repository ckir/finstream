import { ConfigManager } from '@tsrlib/configs';
import { Logger } from './Logger.js';

/**
 * ConfigLoader
 * Pulls configuration from FINSTREAM_CLOUD_CONFIG_URL and handles auto-reloading.
 */
export class ConfigLoader {
    private static manager: ConfigManager;
    private static readonly DEFAULT_URL = process.env.FINSTREAM_CLOUD_CONFIG_URL || '';

    static async load() {
        if (!this.DEFAULT_URL) {
            Logger.warn("FINSTREAM_CLOUD_CONFIG_URL is not set. Using local defaults.");
        }

        try {
            this.manager = new ConfigManager({
                url: this.DEFAULT_URL,
                autoReload: true,
                reloadIntervalMs: 60000 // 1 minute
            });

            await this.manager.init();
            Logger.info("Configuration loaded successfully.");

            this.manager.on('reload', () => {
                Logger.info("Configuration reloaded from cloud.");
            });
            
        } catch (error) {
            Logger.error("Failed to load configuration", error);
            throw error;
        }
    }

    static get<T>(key: string): T {
        return this.manager.get(key);
    }
}
