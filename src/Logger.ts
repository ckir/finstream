import { LoggersSection } from '@tsrlib/loggers';
import { serializeError } from 'serialize-error';

/**
 * FinStream Logger
 * Wraps tsrlib Logger and integrates serialize-error for logging objects/errors.
 */
class FinStreamLogger {
    private logger = LoggersSection.logger.child({ 
        app: 'finstream',
        version: '1.0.0'
    });

    info(message: string, meta?: any) {
        this.logger.info(message, meta);
    }

    debug(message: string, meta?: any) {
        this.logger.debug(message, meta);
    }

    warn(message: string, meta?: any) {
        this.logger.warn(message, meta);
    }

    error(message: string, error?: any) {
        const serialized = error ? serializeError(error) : undefined;
        this.logger.error(message, { error: serialized });
    }
}

export const Logger = new FinStreamLogger();
