const GeniusAppSDK = require('./GeniusAppSDK');
const mqtt = require('mqtt');

// 1. Initialize the App
const app = new GeniusAppSDK('ConnectPowerboxApp');

// 2. State & Settings
app.settings = {
    target_device_id: "",
    calculation_interval: 15
};

let remoteClient = null;

// Memory state
const startupTime = Date.now();
let vAcc = [0, 0, 0, 0, 0, 0]; // L1N, L2N, L3N, L1L2, L2L3, L3L1
let iAcc = Array(28).fill(0);
let accCount = 0;
let lastUpdate = Date.now();
let latestTelemetry = null; // Store latest telemetry for merging
let hasInitialConfig = false; // Prevents overwriting device Modbus before initial sync
let isCalibrationActive = false; // Memory toggle to prevent multi-node conflicts

// Data Structures for the WebUI Tables
let ctDataMap = Array(28).fill(null).map((_, i) => ({
    id: i + 1,
    phaseLocked: false,
    matchPhase: 'Analyzing',
    meanI: 0,
    activePower15s: 0,
    apparentPower15s: 0,
    reactivePower15s: 0,
    maxActivePower: 0,
    pf15s: 0,
    maxPF: 0,
    avgPF: 0,
    pfCount: 0,
    longTermAvgPower: 0,
    applianceTag: 'Idle',
    currentAssoc: 0, // To track what we've written back
    scanState: 0, // 0: Idle/Locked, 1: Scan L1, 2: Scan L2, 3: Scan L3
    scanTick: 0,  // Counts 15s intervals during a scan phase
    assignedVoltage: 'Pending',
    lastAssoc: 0,
    lastAssocChange: 0, // Timestamp when association changed
    phaseStats: [null, null, null] // Tracks {activeP, apparentS, pf} for L1, L2, L3 sweeps
}));

let systemTopology = "Unknown / Fluctuating";


// 3. Connect to App Events
app.onSettingsUpdated = (newSettings) => {
    // Gracefully handle bad inputs or legacy artifacts
    if (app.settings.target_device_id) {
        let tid = app.settings.target_device_id;
        tid = tid.trim();
        if (tid.endsWith('/data')) tid = tid.replace(/\/data$/, '');
        // Fix issues like "connect/ connect/" or "connect/connect/" 
        tid = tid.replace(/^connect\/\s*connect\//i, 'connect/');
        app.settings.target_device_id = tid;
    }

    app.logger.info(`Settings updated! Target Device ID: ${app.settings.target_device_id}`);
    setupRemoteClient();
};

function setupRemoteClient() {
    if (remoteClient) {
        remoteClient.end();
        remoteClient = null;
    }

    // Use configured remote broker, or fall back to cloud broker where Connect posts telemetry
    let url = app.settings.remote_broker_url;
    let rUsername = app.settings.remote_broker_username;
    let rPassword = app.settings.remote_broker_password;

    if (!url) {
        url = process.env.GLOBAL_MQTT_URL || 'mqtts://mqtt.smappeegenius.com:8883';
        rUsername = rUsername || 'admin';
        rPassword = rPassword || 'admin123';
        app.logger.info(`No remote_broker_url configured, using cloud broker fallback: ${url}`);
    }

    // Auto-correct missing schema prefix
    if (!url.startsWith('mqtt://') && !url.startsWith('mqtts://') && !url.startsWith('ws://') && !url.startsWith('wss://')) {
        url = 'mqtt://' + url;
    }

    // Auto-correct protocol if port 8883 is specified but schema is mqtt://
    if (url.includes(':8883') && url.startsWith('mqtt://')) {
        url = url.replace('mqtt://', 'mqtts://');
    }

    app.logger.info(`Connecting to remote broker: ${url}`);
    remoteClient = mqtt.connect(url, {
        username: rUsername,
        password: rPassword,
        reconnectPeriod: 5000,
        rejectUnauthorized: false // Bypass self-signed cert issues for Edge deployments
    });

    remoteClient.on('connect', () => {
        app.logger.info(`Connected to remote broker ${url}`);
        const targetId = app.settings.target_device_id;
        if (targetId && targetId.trim() !== '' && targetId !== 'connect/DEMO_MAC') {
            remoteClient.subscribe(`${targetId}/data`);
            remoteClient.subscribe(`${targetId}/config/out`);
            app.logger.info(`Subscribed to remote target: ${targetId}/data and ${targetId}/config/out`);

            // Request the current Modbus mappings from the Connect box
            if (!hasInitialConfig) {
                remoteClient.publish(`${targetId}/config/in`, JSON.stringify({ ct_types: [0] }));
            }
        } else {
            remoteClient.subscribe(`connect/+/data`);
            remoteClient.subscribe(`connect/+/config/out`);
            app.logger.info(`Subscribed to remote wildcard for auto-discovery: connect/+/data and config/out`);
        }
    });

    remoteClient.on('message', (topic, message) => {
        let targetId = app.settings.target_device_id;

        if (!targetId || targetId.trim() === '' || targetId === 'connect/DEMO_MAC') {
            return; // Explicitly wait for UI target selection
        }

        if (topic === `${targetId}/data`) {
            const rawMsg = message.toString();
            if (rawMsg.trim() === "") return; // Ignore empty retained messages
            if (rawMsg.includes("[Modbus Dump]")) return; // Ignore legacy text spam
            try {
                const payload = JSON.parse(rawMsg);
                processIncomingData(payload);
            } catch (e) {
                app.logger.error("Error parsing Connect telemetry from remote", { error: e.message, raw: rawMsg });
            }
        }

        if (topic === `${targetId}/config/out`) {
            try {
                const payload = JSON.parse(message.toString());
                if (payload.type === 'ct_configuration' && Array.isArray(payload.ct_associations)) {
                    hasInitialConfig = true; // Unlock calibration writes
                    app.logger.info(`Received CT Phase Configuration state from remote.`);
                    payload.ct_associations.forEach((assoc, idx) => {
                        if (idx < 28) {
                            // Only update if not actively scanning/locked during this runtime
                            if (!ctDataMap[idx].phaseLocked && ctDataMap[idx].scanState === 0) {
                                if (ctDataMap[idx].currentAssoc !== assoc) {
                                    ctDataMap[idx].lastAssoc = ctDataMap[idx].currentAssoc;
                                    ctDataMap[idx].currentAssoc = assoc;
                                    ctDataMap[idx].lastAssocChange = Date.now();
                                }
                            }
                        }
                    });
                }
            } catch (e) {
                app.logger.error("Error parsing Connect config from remote", { error: e.message });
            }
        }
    });

    remoteClient.on('error', (err) => {
        app.logger.error(`Remote broker error: ${err.message}`);
    });
}

// Listen to the target Connect's high-frequency data
app.mqttClient.on('message', (topic, message) => {
    // Route Custom Commands targeting this App
    if (topic.startsWith(`${app.appName}/cmd/`)) {
        if (app.onCustomMessage) {
            app.onCustomMessage(topic.replace(`${app.appName}/`, ''), message);
        }
        return;
    }

    let targetId = app.settings.target_device_id;

    if (!targetId || targetId.trim() === '' || targetId === 'connect/DEMO_MAC') {
        return; // Explicitly wait for UI target selection
    }

    // e.g. connect/58:BF:25:DA:xx:xx/data
    if (topic === `${targetId}/data`) {
        const rawMsg = message.toString();
        if (rawMsg.trim() === "") return; // Ignore empty retained messages
        if (rawMsg.includes("[Modbus Dump]")) return; // Ignore legacy text spam
        try {
            const payload = JSON.parse(rawMsg);
            processIncomingData(payload);
        } catch (e) {
            app.logger.error("Error parsing Connect telemetry", e);
        }
    }

    if (topic === `${targetId}/config/out`) {
        try {
            const payload = JSON.parse(message.toString());
            if (payload.type === 'ct_configuration' && Array.isArray(payload.ct_associations)) {
                hasInitialConfig = true; // Unlock calibration writes
                app.logger.info(`Received CT Phase Configuration state from local broker.`);
                payload.ct_associations.forEach((assoc, idx) => {
                    if (idx < 28) {
                        // Only update if not actively scanning/locked during this runtime
                        if (!ctDataMap[idx].phaseLocked && ctDataMap[idx].scanState === 0) {
                            if (ctDataMap[idx].currentAssoc !== assoc) {
                                ctDataMap[idx].lastAssoc = ctDataMap[idx].currentAssoc;
                                ctDataMap[idx].currentAssoc = assoc;
                                ctDataMap[idx].lastAssocChange = Date.now();
                            }
                        }
                    }
                });
            }
        } catch (e) {
            app.logger.error("Error parsing Connect config locally", { error: e.message });
        }
    }
});


function processIncomingData(payload) {
    // We expect voltages. For CT data, we now support either the legacy 'channels' array of objects,
    // or the new firmware's separate arrays: 'currents', 'active_powers', 'apparent_powers'
    if (payload.L1N === undefined && !payload.voltages) return;
    latestTelemetry = payload;

    // Frequency key normalization
    if (payload.freq !== undefined) latestTelemetry.frequency = payload.freq;
    if (latestTelemetry.frequency === undefined) latestTelemetry.frequency = 0;

    // Support both the old array format and the new flat keys format
    const vArr = payload.voltages || [
        payload.L1N || 0, payload.L2N || 0, payload.L3N || 0,
        payload.L1L2 || 0, payload.L2L3 || 0, payload.L3L1 || 0
    ];

    // Accumulate Voltages
    for (let i = 0; i < 6; i++) {
        vAcc[i] += (vArr[i] || 0);
    }

    // Accumulate Currents and Powers
    for (let i = 0; i < 28; i++) {
        let currentVal = 0;
        let activeVal = 0;
        let apparentVal = 0;
        let reactiveVal = 0;

        // Check for flat keys format (I1, P1W)
        if (payload[`I${i + 1}`] !== undefined) {
            currentVal = payload[`I${i + 1}`] || 0;
            activeVal = payload[`P${i + 1}W`] || 0;
            apparentVal = payload[`P${i + 1}VA`] || 0;
            reactiveVal = payload[`P${i + 1}VAR`] || 0;
        }
        // Check for old flat array format
        else if (payload.currents && Array.isArray(payload.currents)) {
            currentVal = payload.currents[i] || 0;
            activeVal = payload.active_powers ? (payload.active_powers[i] || 0) : 0;
            apparentVal = payload.apparent_powers ? (payload.apparent_powers[i] || 0) : 0;
            reactiveVal = payload.reactive_powers ? (payload.reactive_powers[i] || 0) : 0;
        }
        // Fallback to old channels object array format
        else if (payload.channels && Array.isArray(payload.channels)) {
            const ct = payload.channels.find(c => c.id === i + 1);
            if (ct) {
                currentVal = ct.current || 0;
                activeVal = ct.active_power || 0;
                apparentVal = ct.apparent_power || 0;
                reactiveVal = ct.reactive_power || 0;
            }
        }

        // Only accumulate if the physical phase map matches our expected target
        let ctAssocMatches = true;
        if (payload.assoc && Array.isArray(payload.assoc)) {
            // New firmware: strictly match Modbus phase association
            if (payload.assoc[i] !== ctDataMap[i].currentAssoc) {
                ctAssocMatches = false;
            }
        }

        if (ctAssocMatches) {
            // Apply a 5-second dead-zone after any phase switch to let physical hardware DSP filters settle
            if (Date.now() - (ctDataMap[i].lastAssocChange || 0) > 5000) {
                iAcc[i] += currentVal;
                // Continuous power accumulation over the interval
                ctDataMap[i].activePowerAcc = (ctDataMap[i].activePowerAcc || 0) + activeVal;
                ctDataMap[i].apparentPowerAcc = (ctDataMap[i].apparentPowerAcc || 0) + apparentVal;
                ctDataMap[i].reactivePowerAcc = (ctDataMap[i].reactivePowerAcc || 0) + reactiveVal;
                ctDataMap[i].validAccCount = (ctDataMap[i].validAccCount || 0) + 1;
            }
        }
    }

    accCount++;

    // Fire calculations when interval is reached (default 15s)
    let calcInterval = app.settings && app.settings.calculation_interval ? parseInt(app.settings.calculation_interval) : 15;
    if (isNaN(calcInterval) || calcInterval < 2) calcInterval = 15;

    // Use a timestamp to evaluate interval instead of raw accCount because data arrives multiple times per second
    const now = Date.now();
    if (!app._lastIntervalTick) app._lastIntervalTick = now;

    if (now - app._lastIntervalTick >= (calcInterval * 1000)) {
        app._lastIntervalTick = now;
        perform15SecondCalculations();
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
        const ct = ctDataMap[i];

        // Detect currentAssoc changes to reset avgPF PREEMPTIVELY before merging new interval DSP stream
        if (ct.lastAssoc !== undefined && ct.lastAssoc !== ct.currentAssoc) {
            ct.avgPF = NaN;
            ct.pfCount = 0;
            ct.lastAssoc = ct.currentAssoc;
        }

        // Safely evaluate local CT accumulated samples
        let ctAccCount = ct.validAccCount || 0;
        if (ctAccCount === 0) ctAccCount = 1; // Prevent division by zero if all samples were discarded

        let meanI = iAcc[i] / ctAccCount;
        iAcc[i] = 0; // Reset accumulator

        ct.meanI = meanI;

        if (!ct.phaseLocked) {
            if (meanI > 0.05) {
                // If not locked, remains 'Analyzing' unless forced by Single Phase
                ct.matchPhase = "Analyzing";
            }
        }

        let p_inst = (ct.activePowerAcc || 0) / ctAccCount;
        let s_inst = (ct.apparentPowerAcc || 0) / ctAccCount;
        let r_inst = (ct.reactivePowerAcc || 0) / ctAccCount;

        // Reset accumulators for the next interval
        ct.activePowerAcc = 0;
        ct.apparentPowerAcc = 0;
        ct.reactivePowerAcc = 0;
        ct.validAccCount = 0;

        // Provide derived instantaneous values for UI publish and logic
        ct.activePower15s = p_inst;
        ct.apparentPower15s = s_inst;
        ct.reactivePower15s = r_inst;

        if (Math.abs(p_inst) > ct.maxActivePower) {
            ct.maxActivePower = Math.abs(p_inst);
        }

        // Custom PF Estimation
        if (!isNaN(p_inst) && !isNaN(s_inst) && s_inst > 10.0) {
            let pf = Math.abs(p_inst) / s_inst;
            pf = Math.min(Math.max(pf, 0.0), 1.0); // Clamp to 0..1

            ct.pf15s = pf;
            if (pf > ct.maxPF) ct.maxPF = pf;

            // Removing greedy early-exit pf locking so full sweep completes

            if (ct.pfCount === 0 || isNaN(ct.avgPF)) {
                ct.avgPF = pf;
                ct.pfCount = 1;
            } else {
                ct.avgPF = ((ct.avgPF * ct.pfCount) + pf) / (ct.pfCount + 1);
                ct.pfCount++;
            }
        } else {
            ct.pf15s = 0;
            // Reset PF averaging if there is no load, so we don't lock incorrectly later
            ct.avgPF = 0;
            ct.pfCount = 0;
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

        // State Machine: Auto Phase Calibration (Rotator)
        // If not locked, and there is meaningful load, trigger rotation sequence
        // Define valid explicit Modbus phases to rotate through (Normal + Reversed)
        const scanSequence400 = [1, 2, 4, 16, 32, 64]; // L1N, L2N, L3N, L1N_Rev, L2N_Rev, L3N_Rev
        const stringSequence400 = ["Scanning L1N", "Scanning L2N", "Scanning L3N", "Scanning L1N (Rev)", "Scanning L2N (Rev)", "Scanning L3N (Rev)"];

        const scanSequence230 = [33, 66, 65, 18, 36, 20]; // L1-L2, L2-L3, L3-L1, + Rev
        const stringSequence230 = ["Scanning L1-L2", "Scanning L2-L3", "Scanning L3-L1", "Scanning L1-L2 (Rev)", "Scanning L2-L3 (Rev)", "Scanning L3-L1 (Rev)"];

        let is3Phase400 = systemTopology.startsWith("3x400V");
        let is3Phase230 = systemTopology.startsWith("3x230V");
        let activeScanSequence = is3Phase400 ? scanSequence400 : scanSequence230;
        let activeStringSequence = is3Phase400 ? stringSequence400 : stringSequence230;

        let finalV = -1;

        if (systemTopology.startsWith("Single Phase (L1)")) {
            finalV = 0; ct.matchPhase = "L1N (Forced)"; ct.assignedVoltage = "L1N"; ct.phaseLocked = true; ct.currentAssoc = is3Phase400 ? 1 : 33;
        } else if (systemTopology.startsWith("Single Phase (L2)")) {
            finalV = 1; ct.matchPhase = "L2N (Forced)"; ct.assignedVoltage = "L2N"; ct.phaseLocked = true; ct.currentAssoc = is3Phase400 ? 2 : 66;
        } else if (systemTopology.startsWith("Single Phase (L3)")) {
            finalV = 2; ct.matchPhase = "L3N (Forced)"; ct.assignedVoltage = "L3N"; ct.phaseLocked = true; ct.currentAssoc = is3Phase400 ? 4 : 65;
        } else if (!ct.phaseLocked && (is3Phase400 || is3Phase230) && s_inst > 10.0 && isCalibrationActive) {

            let evaluateSweep = true;

            // 1. Enter Sweep Mode
            if (ct.scanState === 0) {
                ct.scanState = 1; // Start at index 1 (Scanning L1)
                ct.scanTick = 0;
                ct.phaseStats = [null, null, null, null, null, null]; // 6 states now

                if (ct.currentAssoc !== activeScanSequence[ct.scanState - 1]) {
                    ct.lastAssoc = ct.currentAssoc;
                    ct.currentAssoc = activeScanSequence[ct.scanState - 1]; // Instantly inject the first configuration
                    ct.lastAssocChange = Date.now();
                }
                evaluateSweep = false; // Must wait 15s interval for hardware DSP to settle
            }

            // 2. We wait 1 interval window (~15s) on the *current* `scanState` to let Modbus and DSP settle.
            if (evaluateSweep && ct.scanState >= 1 && ct.scanState <= activeScanSequence.length) {
                let absolutePF = s_inst > 0 ? Math.abs(p_inst) / s_inst : 0;

                let oldStats = ct.phaseStats[ct.scanState - 1] || null;
                let histCount = oldStats ? (oldStats.count || 0) : 0;
                let newAvgPf = oldStats ? ((oldStats.avgPf * histCount) + absolutePF) / (histCount + 1) : absolutePF;

                ct.phaseStats[ct.scanState - 1] = {
                    activeP: p_inst,     // Preserves sign (negative = export)
                    apparentS: s_inst,   // Always positive VA
                    pf: absolutePF,
                    avgPf: newAvgPf,
                    count: histCount + 1
                };

                // Check for immediate lock
                // We demand PF > 0.98 AND Power > 0 to lock early automatically to prevent flipping
                if (absolutePF > 0.98 && p_inst > 0) {
                    ct.phaseLocked = true;
                    ct.currentAssoc = activeScanSequence[ct.scanState - 1];
                } else {
                    // Advance the State Machine to test the NEXT config
                    ct.scanState++;
                    // Instantly apply next stage config so hardware adjusts during next 15s window
                    if (ct.scanState <= activeScanSequence.length) {
                        if (ct.currentAssoc !== activeScanSequence[ct.scanState - 1]) {
                            ct.lastAssoc = ct.currentAssoc;
                            ct.currentAssoc = activeScanSequence[ct.scanState - 1];
                            ct.lastAssocChange = Date.now();
                        }
                    }
                }
            }

            // 3. Execution Phase: If we just finished recording all phases, evaluate the best one
            if (ct.phaseLocked || ct.scanState > activeScanSequence.length) {

                // If we didn't lock via the greedy rule, we find the highest PF sequence that has POSITIVE power
                if (!ct.phaseLocked && ct.scanState > activeScanSequence.length) {
                    let bestIdx = -1;
                    let bestPF = -1;

                    for (let x = 0; x < ct.phaseStats.length; x++) {
                        if (ct.phaseStats[x] && ct.phaseStats[x].activeP > 0) {
                            if (ct.phaseStats[x].pf > bestPF) {
                                bestPF = ct.phaseStats[x].pf;
                                bestIdx = x;
                            }
                        }
                    }

                    // Fallback to pure highest PF if NO configuration produced positive power (i.e. pure solar export)
                    if (bestIdx === -1) {
                        for (let x = 0; x < ct.phaseStats.length; x++) {
                            if (ct.phaseStats[x] && ct.phaseStats[x].pf > bestPF) {
                                bestPF = ct.phaseStats[x].pf;
                                bestIdx = x;
                            }
                        }
                    }

                    if (bestIdx !== -1 && bestPF > 0.8) {
                        ct.phaseLocked = true;
                        if (ct.currentAssoc !== activeScanSequence[bestIdx]) {
                            ct.lastAssoc = ct.currentAssoc;
                            ct.currentAssoc = activeScanSequence[bestIdx];
                            ct.lastAssocChange = Date.now();
                        }
                        app.logger.info(`CT${i + 1} | Completed sweep. Locked to index ${bestIdx} with PF ${bestPF.toFixed(2)}.`);
                    } else {
                        app.logger.info(`CT${i + 1} | Completed sweep without finding valid candidate. Restarting scan...`);
                        ct.scanState = 1;
                        if (ct.currentAssoc !== activeScanSequence[0]) {
                            ct.lastAssoc = ct.currentAssoc;
                            ct.currentAssoc = activeScanSequence[0];
                            ct.lastAssocChange = Date.now();
                        }
                    }
                }

                // If locked (early lock), map Direction
                if (ct.phaseLocked) {
                    ct.scanState = 0; // Terminate scan

                    // Check index to evaluate if it locked into a Reverse mapped CT or not
                    const wIndex = activeScanSequence.indexOf(ct.currentAssoc);
                    let winnerStats = ct.phaseStats[wIndex];

                    if (!winnerStats) {
                        winnerStats = { activeP: p_inst, apparentS: s_inst };
                    }

                    // Tag based on final locked sign (after native reversal, power should be positive, unless it really is Solar overriding load)
                    ct.applianceTag = winnerStats.activeP < -20.0 ? "Solar/Exporting" : (winnerStats.activeP > 20.0 ? "Load/Importing" : "Idle");

                    ct.matchPhase = wIndex >= 3 ? "Locked (Reverse)" : "Locked";
                    ct.assignedVoltage = activeStringSequence[wIndex].replace("Scanning ", "");

                    app.logger.info(`CT${i + 1} | Locked Phase to [${ct.assignedVoltage}] Register ${ct.currentAssoc}.`);
                }

            } else {
                // Still sweeping: apply the explicitly determined Scan State so the physical Modbus responds
                if (ct.currentAssoc !== activeScanSequence[ct.scanState - 1]) {
                    ct.lastAssoc = ct.currentAssoc;
                    ct.currentAssoc = activeScanSequence[ct.scanState - 1];
                    ct.lastAssocChange = Date.now();
                }
                ct.matchPhase = "Analyzing";
                ct.assignedVoltage = activeStringSequence[ct.scanState - 1];
                app.logger.info(`CT${i + 1} | Sweep Cycle ${ct.scanState}/3 | Applies ${ct.assignedVoltage}`);
            }

            newCtPhases[i] = ct.currentAssoc;

        } else if (ct.phaseLocked) {
            // Keep the locked string for UI (We only need to rebuild if they booted naturally from physical flash)
            if (ct.assignedVoltage === 'Pending' || ct.assignedVoltage === 'Unknown' || ct.assignedVoltage === 'Unassigned') {
                const lockedIndex = activeScanSequence.indexOf(ct.currentAssoc);
                if (lockedIndex >= 0) {
                    ct.assignedVoltage = activeStringSequence[lockedIndex].replace("Scanning ", "");
                    ct.matchPhase = "Locked";
                } else {
                    // Check if Reverse CT
                    const rev400 = [16, 32, 64];
                    const rev230 = [18, 36, 20];
                    const rIndex400 = rev400.indexOf(ct.currentAssoc);
                    const rIndex230 = rev230.indexOf(ct.currentAssoc);

                    if (is3Phase400 && rIndex400 >= 0) {
                        ct.assignedVoltage = activeStringSequence[rIndex400].replace("Scanning ", "");
                        ct.matchPhase = "Locked (Reverse)";
                    } else if (is3Phase230 && rIndex230 >= 0) {
                        ct.assignedVoltage = activeStringSequence[rIndex230].replace("Scanning ", "");
                        ct.matchPhase = "Locked (Reverse)";
                    } else {
                        // Force assignment from unassigned 0 if somehow locked implicitly
                        ct.currentAssoc = is3Phase400 ? 1 : 33;
                        ct.matchPhase = "Locked (Fallback)";
                        ct.assignedVoltage = is3Phase400 ? "L1N" : "L1-L2";
                    }
                }
            }
            newCtPhases[i] = ct.currentAssoc;
        } else {
            // Idle, Unassigned State
            ct.currentAssoc = 0; // Scrub physical default `2` if completely idle
            ct.assignedVoltage = 'Unassigned';
            ct.matchPhase = 'Idle';
            newCtPhases[i] = ct.currentAssoc;
        }
    }


    // --- 4. Write Configuration back to Connect ---
    if (isCalibrationActive || ctDataMap.some(ct => ct.phaseLocked)) {
        publishConfig(newCtTypes, newCtPhases);
    }

    // --- 5. Publish Rich Data to Genius WebUI ---
    publishUiData(meanV);

    accCount = 0; // restart cycle
}

function publishConfig(typesArray, phasesArray) {
    const targetId = app.settings.target_device_id;
    if (!targetId || targetId.trim() === '') return;

    // Safety lock: DO NOT write if we haven't synced the current internal device state
    if (!hasInitialConfig) {
        if (Date.now() - startupTime > 15000) {
            app.logger.warn(`Initial sync timeout exceeded (15s). Unlocking Phase Calibration without gateway sync.`);
            hasInitialConfig = true;
        } else {
            app.logger.info(`Bypassing Config Write: Waiting for initial synchronization packet from local/remote broker.`);
            return;
        }
    }

    const payload = {
        ct_types: typesArray,
        ct_phases: phasesArray
    };

    app.publishData(`writeback`, payload); // Publishes internally to Genius App topics
    // Actually write to the specific target configure topic:
    if (remoteClient && remoteClient.connected) {
        remoteClient.publish(`${targetId}/config/in`, JSON.stringify(payload));
        app.logger.info(`Sent CT Config to Remote broker (${targetId}).`);
    } else {
        app.mqttClient.publish(`${targetId}/config/in`, JSON.stringify(payload));
        app.logger.info(`Sent CT Config to Local broker (${targetId}).`);
    }
}

function publishUiData(meanV) {
    const payload = {
        topology: systemTopology,
        active_target: app.settings.target_device_id || 'Waiting for auto-discovery...',
        voltages: {
            L1N: meanV[0], L2N: meanV[1], L3N: meanV[2],
            L1L2: meanV[3], L2L3: meanV[4], L3L1: meanV[5]
        },
        frequency: latestTelemetry ? latestTelemetry.frequency : 0,
        isCalibrationActive: isCalibrationActive,
        cts: ctDataMap.map(c => ({
            id: c.id,
            tag: c.applianceTag,
            phase: c.matchPhase,
            locked: c.phaseLocked,
            current: c.meanI.toFixed(2),
            activeP: c.activePower15s.toFixed(1),
            apparentP: c.apparentPower15s.toFixed(1),
            reactiveP: c.reactivePower15s.toFixed(1),
            pf: c.pf15s.toFixed(2),
            avgPf: isNaN(c.avgPF) ? 'N/A' : c.avgPF.toFixed(2),
            assoc: c.currentAssoc,
            assignedVoltage: c.assignedVoltage,
            scanState: c.scanState, // Provide visibility into which phase is currently being scanned
            phaseStats: c.phaseStats
        }))
    };

    app.publishData('ui_data', payload);
}

// Support Reset Callbacks from UI Settings
app.onCustomMessage = (topic, payload) => {
    if (topic === 'cmd/reset_calibration') {
        app.logger.warn("Received Reset Calibration command! Wiping saved phase states...");
        for (let i = 0; i < 28; i++) {
            ctDataMap[i].phaseLocked = false;
            ctDataMap[i].matchPhase = 'Analyzing';
            ctDataMap[i].scanState = 0;
            ctDataMap[i].avgPF = 0;
            ctDataMap[i].pfCount = 0;
            ctDataMap[i].assignedVoltage = 'Pending';
            ctDataMap[i].meanI = 0;
            ctDataMap[i].activePower15s = 0;
            ctDataMap[i].apparentPower15s = 0;
            ctDataMap[i].reactivePower15s = 0;
            ctDataMap[i].maxActivePower = 0;
            ctDataMap[i].pf15s = 0;
            ctDataMap[i].maxPF = 0;
            ctDataMap[i].longTermAvgPower = 0;
            ctDataMap[i].applianceTag = 'Idle';
            ctDataMap[i].currentAssoc = 0;
            ctDataMap[i].scanTick = 0;
            ctDataMap[i].phaseStats = [null, null, null, null, null, null];
        }

        // Also force a fresh config sync 
        hasInitialConfig = false;
        if (app.settings.target_device_id && app.settings.target_device_id.trim() !== '' && app.mqttClient) {
            app.mqttClient.publish(`${app.settings.target_device_id}/config/in`, JSON.stringify({ ct_types: [0] }));
        }
    } else if (topic === 'cmd/toggle_calibration') {
        const command = payload.toString().trim();
        if (command === 'start') {
            app.logger.info("User triggered: Local Calibration Engine STARTED (Acquiring Master Control)");
            isCalibrationActive = true;
        } else if (command === 'stop') {
            app.logger.info("User triggered: Local Calibration Engine STOPPED (Yielding Master Control)");
            isCalibrationActive = false;
            // Clear sweeping scan state visually so it doesn't look frozen
            ctDataMap.forEach(ct => {
                if (!ct.phaseLocked) {
                    ct.scanState = 0;
                    ct.matchPhase = 'Analyzing';
                    ct.assignedVoltage = 'Pending';
                }
            });
        }
        publishUiData([vAcc[0] / accCount || 0, vAcc[1] / accCount || 0, vAcc[2] / accCount || 0, vAcc[3] / accCount || 0, vAcc[4] / accCount || 0, vAcc[5] / accCount || 0]);
    } else if (topic === 'cmd/set_target') {
        let newTarget = payload.toString().trim();
        if (newTarget && !newTarget.startsWith('connect/')) {
            newTarget = 'connect/' + newTarget;
        }

        app.logger.info(`Received command to switch target device to: ${newTarget || 'Auto-Discovery'}`);

        // Reset state so we don't bleed old phase mappings visually into the new device
        hasInitialConfig = false;
        for (let i = 0; i < 28; i++) {
            ctDataMap[i].phaseLocked = false;
            ctDataMap[i].matchPhase = 'Analyzing';
            ctDataMap[i].scanState = 0;
            ctDataMap[i].avgPF = 0;
            ctDataMap[i].pfCount = 0;
            ctDataMap[i].assignedVoltage = 'Pending';
            ctDataMap[i].meanI = 0;
            ctDataMap[i].activePower15s = 0;
            ctDataMap[i].apparentPower15s = 0;
            ctDataMap[i].reactivePower15s = 0;
            ctDataMap[i].maxActivePower = 0;
            ctDataMap[i].pf15s = 0;
            ctDataMap[i].maxPF = 0;
            ctDataMap[i].longTermAvgPower = 0;
            ctDataMap[i].applianceTag = 'Idle';
            ctDataMap[i].currentAssoc = 0;
            ctDataMap[i].scanTick = 0;
            ctDataMap[i].phaseStats = [null, null, null, null, null, null];
        }

        app.settings.target_device_id = newTarget;

        if (newTarget) {
            app.mqttClient.subscribe(`${newTarget}/data`);
            app.mqttClient.publish(`${newTarget}/config/in`, JSON.stringify({ ct_types: [0] }));
        } else {
            app.mqttClient.subscribe(`connect/+/data`);
        }
        setupRemoteClient();
    }
};

// 6. Main App Logic
async function main() {
    await app.register('Dev-Connect-01', 'Owner-01');
    app.logger.info("Connect Powerbox Parser started.");

    // Ensure we are listening globally:
    app.mqttClient.subscribe('connect/+/data');
    app.mqttClient.subscribe(`${app.appName}/cmd/#`);

    // Also explicitly subscribe to the dynamic target ID if one is set
    if (app.settings.target_device_id) {
        app.mqttClient.subscribe(`${app.settings.target_device_id}/data`);
    }

    // Attempt to connect to remote broker using default parsed settings
    setupRemoteClient();
}

main();
