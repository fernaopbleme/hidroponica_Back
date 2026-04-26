import asyncio

import json
from datetime import datetime
from typing import List, Dict, Any

import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request
from fastapi.responses import Response
from contextlib import asynccontextmanager
from fastapi import WebSocket, WebSocketDisconnect

event_loop = None
@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_loop

    event_loop = asyncio.get_running_loop()

    print("Iniciando conexão MQTT...")
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_start()

    yield

    print("Encerrando conexão MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

app = FastAPI(lifespan=lifespan)
# Lista de clientes WebSocket conectados
clientes_websocket: List[WebSocket] = []
# WebSocket para enviar dados em tempo real para o Flutter/front-end
@app.websocket("/ws/sensores")
async def websocket_sensores(websocket: WebSocket):
    await websocket.accept()
    clientes_websocket.append(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clientes_websocket.remove(websocket)

# Libera acesso do Flutter/front-end (apenas HTTP, não interfere com WebSocket)
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

# Configuração MQTT para receber dados dos sensores mockados
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "aeroponia/sensores/dados"

# Último dado recebido
ultimo_dado = None

# Thresholds iniciais
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

# Últimos alertas gerados
ultimos_alertas: List[str] = []


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


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Backend conectado ao broker MQTT.")
        client.subscribe(TOPIC)
        print(f"Backend inscrito no tópico: {TOPIC}")
    else:
        print(f"Erro ao conectar no MQTT. Código: {rc}")

#Mandando dados para o flutter/front-end em tempo real via WebSocket

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


def on_message(client, userdata, msg):
    global ultimo_dado, ultimos_alertas

    try:
        payload = msg.payload.decode("utf-8")
        dados = json.loads(payload)

        alertas = processar_dados(dados)

        ultimo_dado = {
            "dados": dados,
            "alertas": alertas,
            "recebidoEm": datetime.now().isoformat()
        }

        ultimos_alertas = alertas

        print("Dados recebidos do MQTT:")
        print(ultimo_dado)

        if event_loop is not None:
            asyncio.run_coroutine_threadsafe(
                enviar_para_flutter(ultimo_dado),
                event_loop
            )

    except Exception as e:
        print("Erro ao processar mensagem MQTT:", e)


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


@app.get("/")
def home():
    return {
        "message": "Backend Aeroponia rodando",
        "mqttTopic": TOPIC
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