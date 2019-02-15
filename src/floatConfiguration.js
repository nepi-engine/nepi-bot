import jsum from 'jsum';
import { emitMessage } from './kafka';

class FloatConfiguration {
    constructor(config) {
        if (true /* Test to check if config is bit packed */) {
            this.config = this.unpackBits(config);
        } else {
            this.config = config;
        }
    }

    unpackBits = () => {
        // Bit unpacking business logic
        unpackedConfig = {...this.config}

        return unpackedConfig
    }

    chunkConfig = (maxBytes) => {
        // Business logic to chunk config file

        return this.config;
    }

    calculateChecksum = () => {
        return jsum(this.config)
    }

    verifyChecksum = (checksum) => {
        return jsum(this.config) === checksum;
    }

    toJSON = () => {
        return JSON.stringify(this.config);
    }

    sendToKafka = () => {
        emitMessage('float-message', this.config);
    }
}

export default FloatConfiguration;
