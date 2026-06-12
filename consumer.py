# src/consumer.py
import json
import sys
import os
from kafka import KafkaConsumer

# Allow Python to import settings from the 'config' folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, CRICKET_TOPIC

# Open src/consumer.py and replace the calculate_metrics function with this:

def calculate_metrics(event_data, state):
    """
    Updates global match analytics on the fly with dynamic type self-healing.
    Forces all state attributes to be dictionaries, preventing any 'set' errors.
    """
    # 🛡️ HARD RESET: If these aren't dictionaries, FORCE them to be dictionaries right now
    if not isinstance(state, dict):
        state = {}
        
    state["batters_tracked"] = {} if not isinstance(state.get("batters_tracked"), dict) else state["batters_tracked"]
    state["bowlers_tracked"] = {} if not isinstance(state.get("bowlers_tracked"), dict) else state["bowlers_tracked"]
    state["event_counts"] = {} if not isinstance(state.get("event_counts"), dict) else state["event_counts"]
    
    if "total_fantasy_points" not in state:
        state["total_fantasy_points"] = 0

    # Track unique active players safely using dictionary mapping
    if event_data["batter"] not in state["batters_tracked"]:
        state["batters_tracked"][event_data["batter"]] = True
        
    if event_data["bowler"] not in state["bowlers_tracked"]:
        state["bowlers_tracked"][event_data["bowler"]] = True
        
    # Aggregate cumulative metrics securely
    state["total_fantasy_points"] += event_data["fantasy_points_earned"]
    
    current_evt = event_data["current_event"]
    state["event_counts"][current_evt] = state["event_counts"].get(current_evt, 0) + 1
    
    # Calculate an approximate Current Run Rate (CRR)
    try:
        over_parts = event_data["overs"].split('.')
        overs_decimal = int(over_parts[0]) + (int(over_parts[1]) / 6)
        runs_total = int(event_data["total_score"].split('/')[0])
        current_run_rate = round((runs_total / overs_decimal), 2) if overs_decimal > 0 else 0.0
    except (ValueError, IndexError):
        current_run_rate = 0.0

    # Build payload mapping
    analytics_payload = {
        "timestamp": event_data["timestamp"],
        "overs": event_data["overs"],
        "total_score": event_data["total_score"],
        "recent_event": event_data["current_event"],
        "active_batter": event_data["batter"],
        "active_bowler": event_data["bowler"],
        "runs_on_ball": event_data["runs_on_ball"],
        "current_run_rate": current_run_rate,
        "total_fantasy_points": state["total_fantasy_points"],
        "unique_players_involved": len(state["batters_tracked"]) + len(state["bowlers_tracked"])
    }
    return analytics_payload

def run_consumer():
    print("🧠 Initializing Real-Time Analytics Consumer Engine...")
    try:
        consumer = KafkaConsumer(
            CRICKET_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"📋 Listening for stream events on topic: '{CRICKET_TOPIC}'...")
    except Exception as e:
        print(f"❌ Consumer failed to connect to Kafka: {e}")
        return

    match_state = {
        "total_fantasy_points": 0,
        "event_counts": {},
        "batters_tracked": {},
        "bowlers_tracked": {}
    }

    try:
        for message in consumer:
            raw_event = message.value
            dashboard_update = calculate_metrics(raw_event, match_state)
            print(f"📊 Analytics Update | Score: {dashboard_update['total_score']} | CRR: {dashboard_update['current_run_rate']}")
    except KeyboardInterrupt:
        print("\n🛑 Analytics processor stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()