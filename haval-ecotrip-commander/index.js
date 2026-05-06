'use strict';

const axios  = require('axios');
const mqtt   = require('mqtt');
const md5    = require('md5');
const fs     = require('fs');
const https  = require('https');

// ── Config ────────────────────────────────────────────────────────────────────
const OPTIONS_FILE = '/data/options.json';
const cfg = JSON.parse(fs.readFileSync(OPTIONS_FILE, 'utf8'));

const GWM_LOGIN_URL  = 'https://br-front-service.gwmcloud.com/br-official-commerce/br-official-gateway/pc-api/api/v1.0/userAuth/loginAccount';
const GWM_BASE_URL   = 'https://br-app-gateway.gwmcloud.com/app-api/api/v1.0';
const DEVICE_ID      = md5('haval_ecotrip_commander');

function makeSeqNo() {
    return 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'.replace(/x/g, () =>
        Math.floor(Math.random() * 16).toString(16)) + '1234';
}

const httpsAgent = new https.Agent({
    cert:               fs.readFileSync('./certs/gwm_general.cer'),
    key:                fs.readFileSync('./certs/gwm_general.key'),
    ca:                 fs.readFileSync('./certs/gwm_root.cer'),
    rejectUnauthorized: false,
    ciphers:            'DEFAULT:@SECLEVEL=0',
});

const PREFIX         = cfg.ecotrip_prefix || 'haval/ecotrip';
const WAKE_TIMEOUT   = (cfg.wake_timeout_s || 90) * 1000;
const CMD_TIMEOUT    = (cfg.cmd_timeout_s  || 30) * 1000;

// Topics
const STATUS_TOPIC        = `${PREFIX}/status`;
const CMD_CHARGE_LIMIT    = `${PREFIX}/cmd/charge_limit`;
const CMD_RESULT_TOPIC    = (cmd) => `${PREFIX}/cmd/${cmd}/result`;
const HA_SELECT_STATE     = `${PREFIX}/ha/charge_limit/state`;
const HA_NUMBER_CMD       = `${PREFIX}/ha/charge_limit/set`;
const HA_DISCOVERY_SELECT = `homeassistant/select/haval_ecotrip_charge_limit/config`;

log('Haval Ecotrip Commander iniciando...');

// ── State ─────────────────────────────────────────────────────────────────────
let accessToken          = null;
let refreshToken         = null;
let tokenExpiry          = 0;
let mqttClient           = null;
let ecotripOnline        = false;
let offlineDebounceTimer = null;   // debounce: evita marcar offline em reconexões rápidas
let pendingCmd           = null;   // { cmd, value, resolve, reject }
let commanderReady       = false;  // ignora mensagens retidas nos primeiros segundos após conectar
let cmdInProgress        = false;  // evita execuções paralelas do mesmo comando
let engineStartedByUs    = false;  // true enquanto fomos nós quem ligou o motor — garante desligamento

// ── Logging ──────────────────────────────────────────────────────────────────
function log(msg)  { console.log(`[INFO]  ${new Date().toISOString()} ${msg}`); }
function warn(msg) { console.log(`[WARN]  ${new Date().toISOString()} ${msg}`); }
function err(msg)  { console.error(`[ERROR] ${new Date().toISOString()} ${msg}`); }

// ── GWM Auth ─────────────────────────────────────────────────────────────────
const LOGIN_HEADERS = {
    'Content-Type':  'application/json',
    'appid':         '6',
    'brand':         '6',
    'brandid':       'CCZ001',
    'country':       'BR',
    'devicetype':    '0',
    'enterpriseid':  'CC01',
    'gwid':          '',
    'language':      'pt_BR',
    'rs':            '5',
    'terminal':      'GW_PC_GWM',
};

async function getTokens() {
    if (accessToken && Date.now() < tokenExpiry) return { accessToken, refreshToken };
    log('Autenticando na GWM API...');
    const res = await axios.post(GWM_LOGIN_URL, {
        account:  cfg.gwm_username,
        password: md5(cfg.gwm_password),
        deviceid: DEVICE_ID,
    }, { headers: LOGIN_HEADERS, httpsAgent });

    if (res.data?.description !== 'SUCCESS' || !res.data?.data?.accessToken) {
        throw new Error('GWM login falhou: ' + JSON.stringify(res.data));
    }
    accessToken  = res.data.data.accessToken;
    refreshToken = res.data.data.refreshToken;
    // Parse JWT exp claim for accurate expiry
    try {
        const payload = JSON.parse(Buffer.from(accessToken.split('.')[1], 'base64').toString());
        tokenExpiry = payload.exp ? payload.exp * 1000 - 60000 : Date.now() + 25 * 60 * 1000;
    } catch (_) {
        tokenExpiry = Date.now() + 25 * 60 * 1000;
    }
    log('Autenticado na GWM API.');
    return { accessToken, refreshToken };
}

function appHeaders(at, rt) {
    return {
        'Content-Type':  'application/json',
        'rs':            '2',
        'terminal':      'GW_APP_GWM',
        'brand':         '6',
        'language':      'pt_BR',
        'systemtype':    '2',
        'regioncode':    'BR',
        'country':       'BR',
        'accessToken':   at,
        'refreshToken':  rt,
    };
}

// ── GWM Commands ──────────────────────────────────────────────────────────────
async function sendGwmCommand(instructions) {
    const { accessToken: at, refreshToken: rt } = await getTokens();
    const seqNo = makeSeqNo();
    const body  = {
        instructions,
        remoteType:       0,
        securityPassword: md5(cfg.gwm_pin),
        seqNo,
        type:             2,
        vin:              cfg.gwm_vin.toUpperCase(),
    };
    log(`GWM sendCmd seqNo=${seqNo}`);
    const res = await axios.post(`${GWM_BASE_URL}/vehicle/T5/sendCmd`, body,
        { headers: appHeaders(at, rt), httpsAgent });
    const code = res.data?.code ?? res.data?.returnCode ?? '';
    if (code !== '0' && code !== '000000') {
        throw new Error(`GWM sendCmd erro: ${JSON.stringify(res.data)}`);
    }
    return seqNo;
}

async function engineOn() {
    log('Ligando motor remotamente (engineOn)...');
    await sendGwmCommand({ '0x03': { operationTime: '15', switchOrder: '1' } });
    engineStartedByUs = true;  // marca que fomos nós quem ligou
}

async function engineOff() {
    log('Desligando motor remotamente (engineOff)...');
    const MAX_ATTEMPTS = 3;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
            await sendGwmCommand({ '0x03': { operationTime: '15', switchOrder: '2' } });
            engineStartedByUs = false;  // desligou com sucesso
            log('Motor desligado com sucesso.');
            return;
        } catch (e) {
            if (attempt < MAX_ATTEMPTS) {
                warn(`engineOff tentativa ${attempt}/${MAX_ATTEMPTS} falhou: ${e.message} — tentando novamente em 5s...`);
                await new Promise(r => setTimeout(r, 5000));
            } else {
                err(`engineOff falhou após ${MAX_ATTEMPTS} tentativas: ${e.message}`);
                // engineStartedByUs permanece true para que handlers de saída tentem novamente
            }
        }
    }
}

// ── Garante desligamento do motor em qualquer saída do processo ───────────────
async function ensureEngineOff(signal) {
    if (!engineStartedByUs) return;
    warn(`[${signal}] Processo encerrando com motor ativo — tentando desligar...`);
    await engineOff();
}

process.on('SIGTERM', async () => { await ensureEngineOff('SIGTERM'); process.exit(0); });
process.on('SIGINT',  async () => { await ensureEngineOff('SIGINT');  process.exit(0); });
process.on('uncaughtException', async (e) => {
    err(`Exceção não tratada: ${e.message}`);
    await ensureEngineOff('uncaughtException');
    process.exit(1);
});
process.on('unhandledRejection', async (reason) => {
    err(`Promise rejeitada não tratada: ${reason}`);
    await ensureEngineOff('unhandledRejection');
    process.exit(1);
});

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
    // Se aparentemente offline, aguarda 8s para mensagens de status em trânsito chegarem.
    // Evita acordar o carro por um race condition de reconexão rápida do MQTT.
    if (!ecotripOnline) {
        log('Ecotrip aparentemente offline — aguardando 8s por possível atualização de status...');
        await new Promise(r => setTimeout(r, 8000));
    }
    // Se Ecotrip já está online (carro em uso), envia direto — nunca ligar/desligar o motor
    const needWake = !ecotripOnline;
    try {
        if (!needWake) {
            log('Ecotrip já está online — enviando comando diretamente (sem ligar/desligar motor).');
        } else {
            // 1. Acordar o carro
            await engineOn();  // sets engineStartedByUs = true

            // 2. Aguardar Ecotrip ficar online
            log(`Aguardando Ecotrip online (timeout ${WAKE_TIMEOUT / 1000}s)...`);
            await waitEcotripOnline(WAKE_TIMEOUT);
            log('Ecotrip online!');

            // 3. Dar um tempo extra para o serviço do carro estabilizar
            await new Promise(r => setTimeout(r, 5000));
        }

        // 4. Enviar comando via MQTT
        const result = await sendEcotripCommand(cmd, value, CMD_TIMEOUT);
        log(`Resultado do comando ${cmd}: ${result}`);

        // 5. Publicar estado atual no HA
        if (cmd === 'charge_limit' && result.startsWith('ok:')) {
            const confirmed = result.split(':')[1];
            mqttClient.publish(HA_SELECT_STATE, confirmed, { qos: 1, retain: true });
        }

        return result;
    } catch (e) {
        err(`Falha no comando remoto ${cmd}: ${e.message}`);
        throw e;
    } finally {
        if (needWake) {
            // 6. Desligar o carro após 10s (tempo de o ECU gravar a config)
            //    Sempre tenta, mesmo em caso de erro — engineOff tem retry interno (3x)
            log('Aguardando 10s antes de desligar o motor...');
            await new Promise(r => setTimeout(r, 10000));
            await engineOff();  // sets engineStartedByUs = false on success
        }
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
        state_topic: HA_SELECT_STATE,
        options: ['50', '60', '70', '80', '90', '100'],
        icon: 'mdi:battery-charging-80',
        device: JSON.parse(device),
        optimistic: false,
        retain: true,
    });
    mqttClient.publish(HA_DISCOVERY_SELECT, payload, { qos: 1, retain: true });
    log('MQTT Discovery publicado: select.haval_ecotrip_charge_limit');

    // Remove legacy number entity
    mqttClient.publish('homeassistant/number/haval_ecotrip_charge_limit/config', '', { qos: 1, retain: true });
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
        commanderReady = false;  // bloqueia comandos retidos até estar estável
        cmdInProgress  = false;
        mqttClient.publish('haval/ecotrip-commander/status', 'online', { qos: 1, retain: true });
        mqttClient.subscribe(STATUS_TOPIC,    { qos: 1 });
        mqttClient.subscribe(HA_NUMBER_CMD,   { qos: 1 });
        // Subscribe result topics to resolve pending promises
        mqttClient.subscribe(`${PREFIX}/cmd/+/result`, { qos: 1 });
        publishDiscovery();
        // Aguarda 3s para descartar mensagens retidas entregues logo após o subscribe
        setTimeout(() => {
            commanderReady = true;
            log('Commander pronto para receber comandos.');
        }, 3000);
    });

    mqttClient.on('message', (topic, message) => {
        const payload = message.toString().trim();

        // Ecotrip online/offline status
        if (topic === STATUS_TOPIC) {
            const isOnline = payload === 'online';
            if (isOnline) {
                // Online imediato — cancela qualquer debounce de offline pendente
                if (offlineDebounceTimer) {
                    clearTimeout(offlineDebounceTimer);
                    offlineDebounceTimer = null;
                }
                if (!ecotripOnline) {
                    ecotripOnline = true;
                    log('Ecotrip status: online');
                    // EcotripImpulse acabou de conectar e pode ter publicado seu próprio discovery
                    // sobrescrevendo o command_topic do Commander. Republicamos após 2s para garantir
                    // que o command_topic correto (ha/charge_limit/set) vença a corrida.
                    setTimeout(() => {
                        log('Republicando discovery para garantir command_topic correto...');
                        publishDiscovery();
                    }, 2000);
                }
            } else {
                // Offline com debounce de 10s — reconexões rápidas não alteram o estado
                if (offlineDebounceTimer) clearTimeout(offlineDebounceTimer);
                offlineDebounceTimer = setTimeout(() => {
                    offlineDebounceTimer = null;
                    if (ecotripOnline) {
                        ecotripOnline = false;
                        log('Ecotrip status: offline');
                    }
                }, 10000);
            }
            return;
        }

        // HA select entity → usuário mudou o limite de carga
        if (topic === HA_NUMBER_CMD) {
            if (!commanderReady) {
                warn(`Ignorando mensagem retida no startup: charge_limit=${payload}`);
                return;
            }
            const pct = parseInt(payload, 10);
            const validValues = [50, 60, 70, 80, 90, 100];
            if (isNaN(pct) || !validValues.includes(pct)) {
                warn(`Valor inválido para charge_limit: ${payload}`);
                return;
            }
            if (cmdInProgress) {
                warn(`Comando já em andamento — ignorando charge_limit=${pct}`);
                return;
            }
            log(`HA solicitou charge_limit=${pct}%`);
            cmdInProgress = true;
            executeRemoteCommand('charge_limit', pct)
                .catch(e => err(e.message))
                .finally(() => { cmdInProgress = false; });
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
