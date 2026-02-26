const mqtt = require('mqtt');

class GeniusAppSDK {
    constructor(appName, mqttUrl = process.env.MQTT_URL || 'mqtt://127.0.0.1:1883') {
        this.appName = appName;
        this.mqttClient = mqtt.connect(mqttUrl, {
            username: process.env.MQTT_USERNAME || 'admin',
            password: process.env.MQTT_PASSWORD || 'admin123'
        });

        this.isRegistered = false;
        this.settings = {};

        // Setup Logger
        this.logger = {
            log: (level, message, data = {}) => {
                const payload = {
                    level: level.toUpperCase(),
                    service: this.appName,
                    message: message,
                    meta: data,
                    timestamp: new Date().toISOString()
                };
                // Publish to central logger path
                this.mqttClient.publish(`logs/${this.appName}`, JSON.stringify(payload));
                console.log(`[${level.toUpperCase()}] ${message}`, data);
            },
            info: (msg, data) => this.logger.log('INFO', msg, data),
            warn: (msg, data) => this.logger.log('WARN', msg, data),
            error: (msg, data) => this.logger.log('ERROR', msg, data),
            debug: (msg, data) => this.logger.log('DEBUG', msg, data)
        };

        this.mqttClient.on('connect', () => {
            this.logger.info(`Connected to Genius MQTT Broker`);
            // Subscribe to our own settings topic to receive updates from WebUI
            this.mqttClient.subscribe(`config/${this.appName}/set`);
        });

        this.mqttClient.on('message', (topic, message) => {
            if (topic === `config/${this.appName}/set`) {
                try {
                    const newSettings = JSON.parse(message.toString());
                    this.settings = { ...this.settings, ...newSettings };
                    this.logger.info('Settings updated', this.settings);
                    if (this.onSettingsUpdated) {
                        this.onSettingsUpdated(this.settings);
                    }
                } catch (e) {
                    this.logger.error('Failed to parse settings update', e.message);
                }
            }
        });
    }

    /**
     * Register the app with the Genius System
     */
    register(devUUID, ownerUUID) {
        return new Promise((resolve, reject) => {
            const payload = {
                appName: this.appName,
                developerId: devUUID,
                ownerId: ownerUUID,
                timestamp: Date.now()
            };

            // Send registration request
            this.mqttClient.publish(`GeniusApps/register`, JSON.stringify(payload));
            this.logger.info(`Registration request sent for ${this.appName}`);

            // For now, auto-accept. In future, listen for GeniusApps/register/ack
            setTimeout(() => {
                this.isRegistered = true;
                this.logger.info(`App ${this.appName} successfully registered.`);
                resolve(true);
            }, 500);
        });
    }

    /**
     * Define the Settings UI for the Web Dashboard
     * @param {Object} schema JSON schema defining the fields (type, label, default)
     */
    createSettingsMenu(schema) {
        // Publish schema to WebUI
        this.mqttClient.publish(`${this.appName}/settings/schema`, JSON.stringify(schema), { retain: true });
        this.logger.info('Settings schema published to WebUI');

        // Store default settings locally
        for (const [key, config] of Object.entries(schema)) {
            if (this.settings[key] === undefined) {
                this.settings[key] = config.default !== undefined ? config.default : null;
            }
        }
    }

    /**
     * Helper to publish data to InfluxDB bridge
     */
    publishData(measurement, fields, tags = {}) {
        const payload = {
            measurement: measurement,
            fields: fields,
            tags: { app: this.appName, ...tags }
        };
        this.mqttClient.publish(`energy/${this.appName}/${measurement}`, JSON.stringify(payload));
    }
}

module.exports = GeniusAppSDK;
