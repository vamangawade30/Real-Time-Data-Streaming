# src/server.py
import json
import asyncio
import sys
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from kafka import KafkaConsumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, CRICKET_TOPIC, SERVER_HOST, SERVER_PORT
from src.consumer import calculate_metrics

app = FastAPI(title="Real-Time Cricket Analytics Engine")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/")
async def serve_dashboard(request: Request):
    """
    Serves the core web frontend user dashboard layout.
    FIXED: Passes 'request' as the first positional argument to satisfy Starlette's 
    signature, while keeping explicit keywords to keep Jinja2 stable.
    """
    return templates.TemplateResponse(request, name="index.html", context={"request": request})

@app.websocket("/ws/live-metrics")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("🔌 Web Client linked via WebSocket. Starting data broadcast pipeline...")

    try:
        consumer = KafkaConsumer(
            CRICKET_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"❌ Failed to attach Kafka consumer to WebSocket channel: {e}")
        await websocket.close()
        return

    ui_match_state = {
        "total_fantasy_points": 0,
        "event_counts": {},
        "batters_tracked": {},
        "bowlers_tracked": {}
    }

    try:
        while True:
            # 1. Yield control back to the event loop so WebSocket disconnects can be caught
            await asyncio.sleep(0.1)
            
            # 2. Use non-blocking polling (wait max 100ms for data, don't stall the main thread)
            msg_pack = consumer.poll(timeout_ms=100)
            
            # 3. If messages are found, iterate and push them out safely
            for tp, messages in msg_pack.items():
                for message in messages:
                    raw_packet = message.value
                    live_analytics = calculate_metrics(raw_packet, ui_match_state)
                    await websocket.send_json(live_analytics)
                    print(f"🚀 Pushed Analytics to UI Dashboard: {live_analytics.get('total_score', '0/0')}")
                    
    except WebSocketDisconnect:
        print("🔌 Web client disconnected from streaming channel.")
    except Exception as e:
        print(f"⚠️ Error encountered during WebSocket stream broadcast: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.server:app", host=SERVER_HOST, port=SERVER_PORT, reload=True)