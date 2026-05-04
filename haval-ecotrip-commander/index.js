'use strict';

const axios = require('axios');
const mqtt  = require('mqtt');
const md5   = require('md5');
const fs    = require('fs');

// ── Config ────────────────────────────────────────────────────────────────────
const OPTIONS_FILE = '/data/options.json';
const cfg = JSON.parse(fs.readFileSync(OPTIONS_FILE, 'utf8'));

const GWM_LOGIN_URL  = 'https://br-front-service.gwmcloud.com/br-official-commerce/br-official-gateway/pc-api/api/v1.0/userAuth/loginAccount';
const GWM_BASE_URL   = 'https://br-app-gateway.gwmcloud.com/app-api/api/v1.0';
const DEVICE_ID      = 'haval_ecotrip_commander';

const PREFIX         = cfg.ecotrip_prefix || 'haval/ecotrip';
const WAKE_TIMEOUT   = (cfg.wake_timeout_s || 90) * 1000;
const CMD_TIMEOUT    = (cfg.cmd_timeout_s  || 30) * 1000;

// Topics
const STATUS_TOPIC        = `${PREFIX}/status`;
const CMD_CHARGE_LIMIT    = `${PREFIX}/cmd/charge_limit`;
const CMD_RESULT_TOPIC    = (cmd) => `${PREFIX}/cmd/${cmd}/result`;
const HA_NUMBER_STATE     = `${PREFIX}/ha/charge_limit/state`;
const HA_NUMBER_CMD       = `${PREFIX}/ha/charge_limit/set`;
const HA_DISCOVERY_NUMBER = `homeassistant/number/haval_ecotrip_charge_limit/config`;

log('Haval Ecotrip Commander iniciando...');

// ── State ─────────────────────────────────────────────────────────────────────
let gwmToken     = null;
let tokenExpiry  = 0;
let mqttClient   = null;
let ecotripOnline = false;
let pendingCmd   = null;   // { cmd, value, resolve, reject }

// ── Logging ──────────────────────────────────────────────────────────────────
function log(msg)  { console.log(`[INFO]  ${new Date().toISOString()} ${msg}`); }
function warn(msg) { console.log(`[WARN]  ${new Date().toISOString()} ${msg}`); }
function err(msg)  { console.error(`[ERROR] ${new Date().toISOString()} ${msg}`); }

// ── GWM Auth ─────────────────────────────────────────────────────────────────
async function getToken() {
    if (gwmToken && Date.now() < tokenExpiry) return gwmToken;
    log('Autenticando na GWM API...');
    const res = await axios.post(GWM_LOGIN_URL, {
        loginAccount: cfg.gwm_username,
        loginPassword: md5(cfg.gwm_password),
        loginType: '0',
        deviceId: DEVICE_ID,
    }, { headers: { 'Content-Type': 'application/json' } });

    if (!res.data?.data?.token) throw new Error('GWM login falhou: ' + JSON.stringify(res.data));
    gwmToken   = res.data.data.token;
    tokenExpiry = Date.now() + 25 * 60 * 1000; // 25 min
    log('Autenticado na GWM API.');
    return gwmToken;
}

function gwmHeaders(token) {
    return {
        'Content-Type': 'application/json',
        'token': token,
        'vin': cfg.gwm_vin,
    };
}

// ── GWM Commands ──────────────────────────────────────────────────────────────
async function sendGwmCommand(serviceCode, instructions) {
    const token = await getToken();
    const seqNo = `${Date.now()}${Math.floor(Math.random() * 9000 + 1000)}`;
    const body  = {
        vin: cfg.gwm_vin,
        pin: md5(cfg.gwm_pin),
        seqNo,
        serviceCode,
        instructions,
    };
    log(`GWM sendCmd serviceCode=${serviceCode} seqNo=${seqNo}`);
    const res = await axios.post(`${GWM_BASE_URL}/vehicle/T5/sendCmd`, body,
        { headers: gwmHeaders(token) });
    if (res.data?.code !== '0' && res.data?.returnCode !== '0') {
        throw new Error(`GWM sendCmd erro: ${JSON.stringify(res.data)}`);
    }
    return seqNo;
}

async function engineOn() {
    log('Ligando motor remotamente (engineOn)...');
    await sendGwmCommand('0x03', [{ name: 'powerMode', value: '1' }]);
}

async function engineOff() {
    log('Desligando motor remotamente (engineOff)...');
    try {
        await sendGwmCommand('0x03', [{ name: 'powerMode', value: '2' }]);
    } catch (e) {
        warn(`engineOff ignorado: ${e.message}`);
    }
}

// ── Wait for Ecotrip online ───────────────────────────────────────────────────
function waitEcotripOnline(timeoutMs) {
    if (ecotripOnline) return Promise.resolve();
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error('Timeout aguardando Ecotrip online')), timeoutMs);
        const check = setInterval(() => {
            if (ecotripOnline) {
                clearTimeout(t);
                clearInterval(check);
                resolve();
            }
        }, 1000);
    });
}

// ── Send command to Ecotrip via MQTT ─────────────────────────────────────────
function sendEcotripCommand(cmd, value, timeoutMs) {
    return new Promise((resolve, reject) => {
        const resTopic = CMD_RESULT_TOPIC(cmd);
        const t = setTimeout(() => {
            reject(new Error(`Timeout aguardando resposta do comando ${cmd}`));
        }, timeoutMs);

        pendingCmd = { cmd, resolve: (r) => { clearTimeout(t); resolve(r); }, reject };
        mqttClient.publish(`${PREFIX}/cmd/${cmd}`, String(value), { qos: 1 });
        log(`Comando enviado ao Ecotrip: ${cmd}=${value}`);
    });
}

// ── Main command orchestrator ─────────────────────────────────────────────────
async function executeRemoteCommand(cmd, value) {
    log(`=== Executando comando remoto: ${cmd}=${value} ===`);
    try {
        // 1. Acordar o carro
        await engineOn();

        // 2. Aguardar Ecotrip ficar online
        log(`Aguardando Ecotrip online (timeout ${WAKE_TIMEOUT / 1000}s)...`);
        await waitEcotripOnline(WAKE_TIMEOUT);
        log('Ecotrip online!');

        // 3. Dar um tempo extra para o serviço do carro estabilizar
        await new Promise(r => setTimeout(r, 5000));

        // 4. Enviar comando via MQTT
        const result = await sendEcotripCommand(cmd, value, CMD_TIMEOUT);
        log(`Resultado do comando ${cmd}: ${result}`);

        // 5. Publicar estado atual no HA
        if (cmd === 'charge_limit' && result.startsWith('ok:')) {
            const confirmed = result.split(':')[1];
            mqttClient.publish(HA_NUMBER_STATE, confirmed, { qos: 1, retain: true });
        }

        return result;
    } catch (e) {
        err(`Falha no comando remoto ${cmd}: ${e.message}`);
        throw e;
    } finally {
        // 6. Desligar o carro após 10s (tempo de o ECU gravar a config)
        log('Aguardando 10s antes de desligar o motor...');
        await new Promise(r => setTimeout(r, 10000));
        await engineOff();
        log('=== Comando remoto finalizado ===');
    }
}

// ── MQTT Discovery ────────────────────────────────────────────────────────────
function publishDiscovery() {
    const device = JSON.stringify({
        identifiers: ['haval_ecotrip'],
        name: 'Haval Ecotrip',
        manufacturer: 'Haval',
    });
    const payload = JSON.stringify({
        name: 'Limite de Carga da Bateria',
        unique_id: 'haval_ecotrip_charge_limit',
        command_topic: HA_NUMBER_CMD,
        state_topic: HA_NUMBER_STATE,
        min: 50,
        max: 100,
        step: 5,
        unit_of_measurement: '%',
        icon: 'mdi:battery-charging-80',
        device: JSON.parse(device),
        optimistic: false,
        retain: true,
    });
    mqttClient.publish(HA_DISCOVERY_NUMBER, payload, { qos: 1, retain: true });
    log('MQTT Discovery publicado: number.haval_ecotrip_charge_limit');
}

// ── MQTT setup ────────────────────────────────────────────────────────────────
function setupMqtt() {
    const url = cfg.mqtt_server.startsWith('mqtt://') ? cfg.mqtt_server : `mqtt://${cfg.mqtt_server}`;
    mqttClient = mqtt.connect(url, {
        username: cfg.mqtt_user,
        password: cfg.mqtt_pass,
        clientId: `haval_commander_${Date.now() % 10000}`,
        will: { topic: 'haval/ecotrip-commander/status', payload: 'offline', qos: 1, retain: true },
    });

    mqttClient.on('connect', () => {
        log('MQTT conectado.');
        mqttClient.publish('haval/ecotrip-commander/status', 'online', { qos: 1, retain: true });
        mqttClient.subscribe(STATUS_TOPIC,    { qos: 1 });
        mqttClient.subscribe(HA_NUMBER_CMD,   { qos: 1 });
        // Subscribe result topics to resolve pending promises
        mqttClient.subscribe(`${PREFIX}/cmd/+/result`, { qos: 1 });
        publishDiscovery();
    });

    mqttClient.on('message', (topic, message) => {
        const payload = message.toString().trim();

        // Ecotrip online/offline status
        if (topic === STATUS_TOPIC) {
            const prev = ecotripOnline;
            ecotripOnline = payload === 'online';
            if (ecotripOnline !== prev) log(`Ecotrip status: ${payload}`);
            return;
        }

        // HA number entity → usuário mudou o slider
        if (topic === HA_NUMBER_CMD) {
            const pct = parseInt(payload, 10);
            if (isNaN(pct) || pct < 50 || pct > 100) {
                warn(`Valor inválido para charge_limit: ${payload}`);
                return;
            }
            log(`HA solicitou charge_limit=${pct}%`);
            executeRemoteCommand('charge_limit', pct).catch(e => err(e.message));
            return;
        }

        // Resultado de comando do Ecotrip
        if (topic.startsWith(`${PREFIX}/cmd/`) && topic.endsWith('/result')) {
            if (pendingCmd) {
                log(`Resultado recebido para ${pendingCmd.cmd}: ${payload}`);
                if (payload.startsWith('error')) {
                    pendingCmd.reject(new Error(payload));
                } else {
                    pendingCmd.resolve(payload);
                }
                pendingCmd = null;
            }
        }
    });

    mqttClient.on('error', e => err(`MQTT erro: ${e.message}`));
    mqttClient.on('close', () => warn('MQTT desconectado.'));
}

// ── Start ─────────────────────────────────────────────────────────────────────
setupMqtt();
