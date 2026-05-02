import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response


# =========================
# CONFIGURAÇÕES MQTT
# =========================

BROKER = "broker.hivemq.com"
PORT = 8883  # Porta MQTT com TLS
TOPIC = "aeroponia/sensores/dados"


# =========================
# VARIÁVEIS GLOBAIS
# =========================

event_loop = None
ultimo_dado = None
ultimos_alertas: List[str] = []
clientes_websocket: List[WebSocket] = []

thresholds = {
    "phMin": 5.5,
    "phMax": 6.5,
    "ecMin": 0.8,
    "ecMax": 1.8,
    "temperaturaAguaMax": 28,
    "temperaturaAmbienteMax": 32,
    "umidadeRelativaMin": 45,
    "nivelAguaMin": 35,
}


# =========================
# PROCESSAMENTO DOS DADOS
# =========================

def processar_dados(dados: Dict[str, Any]) -> List[str]:
    alertas = []

    if dados["ph"] < thresholds["phMin"]:
        alertas.append(f"pH abaixo do limite: {dados['ph']}")

    if dados["ph"] > thresholds["phMax"]:
        alertas.append(f"pH acima do limite: {dados['ph']}")

    if dados["ec"] < thresholds["ecMin"]:
        alertas.append(f"EC abaixo do limite: {dados['ec']}")

    if dados["ec"] > thresholds["ecMax"]:
        alertas.append(f"EC acima do limite: {dados['ec']}")

    if dados["temperaturaAgua"] > thresholds["temperaturaAguaMax"]:
        alertas.append(f"Temperatura da água alta: {dados['temperaturaAgua']}°C")

    if dados["temperaturaAmbiente"] > thresholds["temperaturaAmbienteMax"]:
        alertas.append(f"Temperatura ambiente alta: {dados['temperaturaAmbiente']}°C")

    if dados["umidadeRelativa"] < thresholds["umidadeRelativaMin"]:
        alertas.append(f"Umidade relativa baixa: {dados['umidadeRelativa']}%")

    if dados["nivelAgua"] < thresholds["nivelAguaMin"]:
        alertas.append(f"Nível de água baixo: {dados['nivelAgua']}%")

    return alertas


# =========================
# WEBSOCKET
# =========================

async def enviar_para_flutter(payload):
    clientes_desconectados = []

    for websocket in clientes_websocket:
        try:
            await websocket.send_json(payload)
        except Exception:
            clientes_desconectados.append(websocket)

    for websocket in clientes_desconectados:
        if websocket in clientes_websocket:
            clientes_websocket.remove(websocket)


# =========================
# MQTT CALLBACKS
# =========================

def on_connect(client, userdata, flags, rc):
    print(f"on_connect chamado. Código: {rc}", flush=True)

    if rc == 0:
        print("Backend conectado ao broker MQTT com TLS.", flush=True)
        client.subscribe(TOPIC)
        print(f"Backend inscrito no tópico: {TOPIC}", flush=True)
    else:
        print(f"Erro ao conectar no MQTT. Código: {rc}", flush=True)


def on_message(client, userdata, msg):
    global ultimo_dado, ultimos_alertas

    try:
        print("Mensagem MQTT recebida.", flush=True)

        payload = msg.payload.decode("utf-8")
        dados = json.loads(payload)

        alertas = processar_dados(dados)

        ultimo_dado = {
            "dados": dados,
            "alertas": alertas,
            "recebidoEm": datetime.now().isoformat()
        }

        ultimos_alertas = alertas

        print("Dados processados:", flush=True)
        print(ultimo_dado, flush=True)

        if event_loop is not None:
            asyncio.run_coroutine_threadsafe(
                enviar_para_flutter(ultimo_dado),
                event_loop
            )

    except Exception as e:
        print(f"Erro ao processar mensagem MQTT: {e}", flush=True)


# =========================
# CLIENTE MQTT
# =========================

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
mqtt_client.tls_set()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# =========================
# LIFESPAN FASTAPI
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_loop

    event_loop = asyncio.get_running_loop()

    print("Iniciando conexão MQTT...", flush=True)
    print(f"Broker: {BROKER}", flush=True)
    print(f"Porta: {PORT}", flush=True)
    print(f"Tópico: {TOPIC}", flush=True)

    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_start()

    yield

    print("Encerrando conexão MQTT...", flush=True)
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


app = FastAPI(lifespan=lifespan)


# =========================
# CORS HTTP
# =========================

@app.middleware("http")
async def cors_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        response = Response()
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


# =========================
# ROTAS WEBSOCKET
# =========================

@app.websocket("/ws/sensores")
async def websocket_sensores(websocket: WebSocket):
    await websocket.accept()
    clientes_websocket.append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in clientes_websocket:
            clientes_websocket.remove(websocket)


# =========================
# ROTAS HTTP
# =========================

@app.get("/")
def home():
    return {
        "message": "Backend Aeroponia rodando",
        "mqttTopic": TOPIC,
        "mqttBroker": BROKER,
        "mqttPort": PORT
    }


@app.get("/dados")
def get_dados():
    return ultimo_dado


@app.get("/thresholds")
def get_thresholds():
    return thresholds


@app.post("/thresholds")
def atualizar_thresholds(novos_thresholds: Dict[str, float]):
    thresholds.update(novos_thresholds)

    return {
        "message": "Thresholds atualizados com sucesso",
        "thresholds": thresholds
    }


@app.get("/alertas")
def get_alertas():
    return {
        "alertas": ultimos_alertas
    }
