const GeniusAppSDK = require('./GeniusAppSDK');
const mqtt = require('mqtt');

// 1. Initialize the App
const app = new GeniusAppSDK('ConnectPowerboxApp');

// 2. Define the Settings Schema for the WebUI
const settingsSchema = {
    target_device_id: { type: "string", label: "Target Connect ID (e.g., connect/DEMO_MAC)", default: "" },
    calculation_interval: { type: "number", label: "Aggregration Interval (seconds)", default: 15 },
    remote_broker_url: { type: "string", label: "Remote Broker URL", default: "mqtt://mqtt.smappeegenius.com:1883" },
    remote_broker_username: { type: "string", label: "Remote Broker Username", default: "admin" },
    remote_broker_password: { type: "string", label: "Remote Broker Password", default: "admin123" }
};
app.createSettingsMenu(settingsSchema);

let remoteClient = null;

// Memory state
let vAcc = [0, 0, 0, 0, 0, 0]; // L1N, L2N, L3N, L1L2, L2L3, L3L1
let iAcc = Array(28).fill(0);
let accCount = 0;
let lastUpdate = Date.now();
let latestTelemetry = null; // Store latest telemetry for merging

// Data Structures for the WebUI Tables
let ctDataMap = Array(28).fill(null).map((_, i) => ({
    id: i + 1,
    phaseLocked: false,
    matchPhase: 'Analyzing',
    meanI: 0,
    activePower15s: 0,
    apparentPower15s: 0,
    maxActivePower: 0,
    pf15s: 0,
    maxPF: 0,
    avgPF: 0,
    pfCount: 0,
    longTermAvgPower: 0,
    applianceTag: 'Idle',
    currentAssoc: 0 // To track what we've written back
}));

let systemTopology = "Unknown / Fluctuating";


// 3. Connect to App Events
app.onSettingsUpdated = (newSettings) => {
    app.logger.info(`Settings updated! Target Device ID: ${newSettings.target_device_id}`);
    setupRemoteClient();
};

function setupRemoteClient() {
    if (remoteClient) {
        remoteClient.end();
        remoteClient = null;
    }
    const url = app.settings.remote_broker_url;
    if (!url) return;

    app.logger.info(`Connecting to remote broker: ${url}`);
    remoteClient = mqtt.connect(url, {
        username: app.settings.remote_broker_username,
        password: app.settings.remote_broker_password,
        reconnectPeriod: 5000
    });

    remoteClient.on('connect', () => {
        app.logger.info(`Connected to remote broker ${url}`);
        const targetId = app.settings.target_device_id;
        if (targetId) {
            remoteClient.subscribe(`${targetId}/data`);
            app.logger.info(`Subscribed to remote target: ${targetId}/data`);
        }
    });

    remoteClient.on('message', (topic, message) => {
        const targetId = app.settings.target_device_id;
        if (!targetId || targetId.trim() === '') return;

        if (topic === `${targetId}/data`) {
            try {
                const payload = JSON.parse(message.toString());
                processIncomingData(payload);
            } catch (e) {
                app.logger.error("Error parsing Connect telemetry from remote", e);
            }
        }
    });

    remoteClient.on('error', (err) => {
        app.logger.error(`Remote broker error: ${err.message}`);
    });
}

// Listen to the target Connect's high-frequency data
app.mqttClient.on('message', (topic, message) => {
    const targetId = app.settings.target_device_id;
    if (!targetId || targetId.trim() === '') return;

    // e.g. connect/58:BF:25:DA:xx:xx/data
    if (topic === `${targetId}/data`) {
        try {
            const payload = JSON.parse(message.toString());
            processIncomingData(payload);
        } catch (e) {
            app.logger.error("Error parsing Connect telemetry", e);
        }
    }
});


function processIncomingData(payload) {
    if (!payload.voltages || !payload.channels) return;
    latestTelemetry = payload;

    const vArr = payload.voltages; // [vL1_N, vL2_N, vL3_N, vL1_L2, vL2_L3, vL3_L1]
    const cArr = payload.channels; // Array of objects

    // Accumulate Voltages
    for (let i = 0; i < 6; i++) {
        vAcc[i] += (vArr[i] || 0);
    }

    // Accumulate Currents (use .current from payload, which is fundamental based on the driver we analyzed)
    for (let i = 0; i < 28; i++) {
        const ct = cArr.find(c => c.id === i + 1);
        if (ct) {
            iAcc[i] += (ct.current || 0);

            // Instantaneous power capture
            ctDataMap[i].activePower15s = ct.active_power || 0;
            ctDataMap[i].apparentPower15s = ct.apparent_power || 0;
        }
    }

    accCount++;

    const intervalMs = (app.settings.calculation_interval || 15) * 1000;
    if (Date.now() - lastUpdate >= intervalMs) {
        perform15SecondCalculations();
        lastUpdate = Date.now();
    }
}


function perform15SecondCalculations() {
    if (accCount === 0) return;

    const vNames = ["L1N", "L2N", "L3N", "L1L2", "L2L3", "L3L1"];
    const meanV = vAcc.map(v => v / accCount);

    // Reset voltage accumulators
    vAcc.fill(0);

    // Determine Topology
    const avg_L1L2_L2L3 = (meanV[3] + meanV[4]) / 2.0;
    const avg_L2L3_L3L1 = (meanV[4] + meanV[5]) / 2.0;
    const avg_L1L2_L3L1 = (meanV[3] + meanV[5]) / 2.0;

    if (Math.abs(meanV[0] - meanV[1]) <= 1.0 && meanV[2] < 10.0) {
        systemTopology = "Single Phase (L1)";
    } else if (Math.abs(meanV[1] - meanV[2]) <= 1.0 && meanV[0] < 10.0) {
        systemTopology = "Single Phase (L2)";
    } else if (Math.abs(meanV[0] - meanV[2]) <= 1.0 && meanV[1] < 10.0) {
        systemTopology = "Single Phase (L3)";
    } else if (meanV[3] > 330.0 || meanV[4] > 330.0 || meanV[5] > 330.0) {
        systemTopology = "3x400V (3P+N)";
    } else if (meanV[5] < 150.0 || meanV[5] < 0.7 * avg_L1L2_L2L3) {
        systemTopology = "3x230V Delta (L1-L2)";
    } else if (meanV[3] < 150.0 || meanV[3] < 0.7 * avg_L2L3_L3L1) {
        systemTopology = "3x230V Delta (L2-L3)";
    } else if (meanV[4] < 150.0 || meanV[4] < 0.7 * avg_L1L2_L3L1) {
        systemTopology = "3x230V Delta (L3-L1)";
    } else {
        systemTopology = "Unknown / Fluctuating";
    }

    app.logger.info(`Detected Topology: ${systemTopology}`);

    let newCtPhases = Array(28).fill(0); // For configuration writeback
    let newCtTypes = Array(28).fill(0); // 0 = 50A defaults

    for (let i = 0; i < 28; i++) {
        let meanI = iAcc[i] / accCount;
        iAcc[i] = 0; // Reset accumulator

        const ct = ctDataMap[i];
        ct.meanI = meanI;

        if (!ct.phaseLocked) {
            if (meanI > 0.05) {
                // If not locked, remains 'Analyzing' unless forced by Single Phase
                ct.matchPhase = "Analyzing";
            }
        }

        let p_inst = ct.activePower15s;
        let s_inst = ct.apparentPower15s;

        if (Math.abs(p_inst) > ct.maxActivePower) {
            ct.maxActivePower = Math.abs(p_inst);
        }

        // Custom PF Estimation
        if (!isNaN(p_inst) && !isNaN(s_inst) && s_inst > 10.0) {
            let pf = Math.abs(p_inst) / s_inst;
            pf = Math.min(Math.max(pf, 0.0), 1.0); // Clamp to 0..1

            ct.pf15s = pf;
            if (pf > ct.maxPF) ct.maxPF = pf;

            if (pf > 0.95 && !ct.phaseLocked && !ct.matchPhase.startsWith("Unknown") && !ct.matchPhase.startsWith("Analyzing")) {
                ct.phaseLocked = true;
                app.logger.info(`CT${i} | Locked Phase to ${ct.matchPhase} (PF ${pf.toFixed(2)})`);
            }

            if (ct.pfCount === 0 || isNaN(ct.avgPF)) {
                ct.avgPF = pf;
                ct.pfCount = 1;
            } else {
                ct.avgPF = ((ct.avgPF * ct.pfCount) + pf) / (ct.pfCount + 1);
                ct.pfCount++;
            }
        } else {
            ct.pf15s = 0;
        }

        // Long Term Averaging
        if (ct.longTermAvgPower === 0.0) {
            ct.longTermAvgPower = p_inst;
        } else {
            ct.longTermAvgPower = (ct.longTermAvgPower * 0.9) + (p_inst * 0.1);
        }

        // Appliance Tagging Heuristics
        let lta = ct.longTermAvgPower;
        if (lta < -50.0) {
            ct.applianceTag = "Solar/Exporting";
        } else if (lta > 50.0) {
            ct.applianceTag = "Load/Importing";
        } else {
            ct.applianceTag = "Idle";
        }

        // Single Phase Override Override
        let finalV = -1;
        if (systemTopology.startsWith("Single Phase (L1)")) {
            finalV = 0; ct.matchPhase = "L1N (Forced)";
        } else if (systemTopology.startsWith("Single Phase (L2)")) {
            finalV = 1; ct.matchPhase = "L2N (Forced)";
        } else if (systemTopology.startsWith("Single Phase (L3)")) {
            finalV = 2; ct.matchPhase = "L3N (Forced)";
        } else {
            // Re-resolve matching Phase string to finalV mapping
            const dPh = ct.matchPhase;
            if (dPh.startsWith("L1N")) finalV = 0;
            else if (dPh.startsWith("L2N")) finalV = 1;
            else if (dPh.startsWith("L3N")) finalV = 2;
            else if (dPh.startsWith("L1L2")) finalV = 3;
            else if (dPh.startsWith("L2L3")) finalV = 4;
            else if (dPh.startsWith("L3L1")) finalV = 5;
        }

        // Modbus Assocation Generation
        if (finalV >= 0 && finalV <= 5) {
            let currentlyReversed = [16, 32, 64, 18, 36, 20].includes(ct.currentAssoc);

            let truePower = p_inst;
            if (currentlyReversed) truePower = -truePower;

            let targetReversed = currentlyReversed;
            if (ct.applianceTag === "Solar/Exporting") {
                targetReversed = false;
            } else if (truePower < -20.0) {
                targetReversed = true;
            } else if (truePower > 20.0) {
                targetReversed = false;
            }

            let mdbsVal = 0;
            switch (finalV) {
                case 0: mdbsVal = targetReversed ? 16 : 1; break;
                case 1: mdbsVal = targetReversed ? 32 : 2; break;
                case 2: mdbsVal = targetReversed ? 64 : 4; break;
                case 3: mdbsVal = targetReversed ? 18 : 33; break;
                case 4: mdbsVal = targetReversed ? 36 : 66; break;
                case 5: mdbsVal = targetReversed ? 20 : 65; break;
            }

            ct.currentAssoc = mdbsVal;
            newCtPhases[i] = mdbsVal;
        } else {
            // Keep existing (or default to 0 if not assigned)
            newCtPhases[i] = ct.currentAssoc;
        }
    }


    // --- 4. Write Configuration back to Connect ---
    publishConfig(newCtTypes, newCtPhases);


    // --- 5. Publish Rich Data to Genius WebUI ---
    publishUiData(meanV);

    accCount = 0; // restart cycle
}

function publishConfig(typesArray, phasesArray) {
    const targetId = app.settings.target_device_id;
    if (!targetId || targetId.trim() === '') return;

    const payload = {
        ct_types: typesArray,
        ct_phases: phasesArray
    };

    app.publishData(`writeback`, payload); // Publishes internally to Genius App topics
    // Actually write to the specific target configure topic:
    if (remoteClient && remoteClient.connected) {
        remoteClient.publish(`${targetId}/config/in`, JSON.stringify(payload));
        app.logger.info(`Sent CT Configuration Map to remote device (${targetId}).`);
    } else {
        app.mqttClient.publish(`${targetId}/config/in`, JSON.stringify(payload));
        app.logger.info(`Sent CT Configuration Map to local broker device (${targetId}).`);
    }
}

function publishUiData(meanV) {
    const payload = {
        topology: systemTopology,
        voltages: {
            L1N: meanV[0], L2N: meanV[1], L3N: meanV[2],
            L1L2: meanV[3], L2L3: meanV[4], L3L1: meanV[5]
        },
        frequency: latestTelemetry ? latestTelemetry.frequency : 0,
        cts: ctDataMap.map(c => ({
            id: c.id,
            tag: c.applianceTag,
            phase: c.matchPhase,
            locked: c.phaseLocked,
            current: c.meanI.toFixed(2),
            activeP: c.activePower15s.toFixed(1),
            apparentP: c.apparentPower15s.toFixed(1),
            pf: c.pf15s.toFixed(2),
            avgPf: isNaN(c.avgPF) ? 'N/A' : c.avgPF.toFixed(2),
            assoc: c.currentAssoc
        }))
    };

    app.publishData('ui_data', payload);
}

// 6. Main App Logic
async function main() {
    await app.register('Dev-Connect-01', 'Owner-01');
    app.logger.info("Connect Powerbox Parser started.");

    // Ensure we are listening globally:
    app.mqttClient.subscribe('connect/+/data');

    // Also explicitly subscribe to the dynamic target ID if one is set
    if (app.settings.target_device_id) {
        app.mqttClient.subscribe(`${app.settings.target_device_id}/data`);
    }

    // Attempt to connect to remote broker using default parsed settings
    setupRemoteClient();
}

main();
