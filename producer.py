# src/producer.py
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import sys
import os

# Allow Python to import settings from the 'config' folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, CRICKET_TOPIC

def json_serializer(data):
    """Encodes Python dictionaries into a JSON string byte-format for Kafka."""
    return json.dumps(data).encode('utf-8')

def generate_match_event(ball_num):
    """Simulates a single dynamic delivery event with realistic sports match metrics."""
    batters = ["Virat Kohli", "Rohit Sharma", "Suryakumar Yadav", "Hardik Pandya"]
    bowlers = ["Jasprit Bumrah", "Rashid Khan", "Mitchell Starc", "Shaheen Afridi"]
    
    event_types = ["dot", "single", "boundary_4", "six", "wicket", "wide", "no_ball"]
    # Probabilities weighted to simulate a real T20 match context
    event = random.choices(event_types, weights=[35, 40, 12, 6, 4, 2, 1])[0]
    
    runs = 0
    wicket_event = False
    extra = 0
    
    if event == "single":
        runs = random.choice([1, 2, 3])
    elif event == "boundary_4":
        runs = 4
    elif event == "six":
        runs = 6
    elif event == "wicket":
        wicket_event = True
    elif event in ["wide", "no_ball"]:
        extra = 1
        runs = random.choice([0, 1])

    ball_increment = 0 if event in ["wide", "no_ball"] else 1
    
    # Custom fantasy league calculation system logic
    fantasy_points = (runs * 1) + (4 if runs == 4 else 0) + (8 if runs == 6 else 0) + (25 if wicket_event else 0)

    event_payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ball_number": ball_num + ball_increment,
        "current_event": event.upper(),
        "batter": random.choice(batters),
        "bowler": random.choice(bowlers),
        "runs_on_ball": runs + extra,
        "is_wicket": wicket_event,
        "fantasy_points_earned": fantasy_points
    }
    
    return event_payload, ball_increment

def run_producer():
    print("⚡ Initializing Kafka Producer... Connecting to Docker Broker.")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer
        )
        print(f"✅ Connection Established! Streaming data packets to topic: '{CRICKET_TOPIC}'")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    ball_count = 0
    cumulative_runs = 0
    cumulative_wickets = 0

    try:
        while cumulative_wickets < 10 and ball_count < 120:  # Cap at a full 20-over innings
            time.sleep(random.uniform(1.5, 3.0))  # Delay between balls to mimic live delivery pacing
            
            payload, valid_ball = generate_match_event(ball_count)
            ball_count = payload["ball_number"]
            cumulative_runs += payload["runs_on_ball"]
            if payload["is_wicket"]:
                cumulative_wickets += 1
                
            # Append global game scoreboard status metrics
            payload["total_score"] = f"{cumulative_runs}/{cumulative_wickets}"
            overs = f"{ball_count // 6}.{ball_count % 6}"
            payload["overs"] = overs

            # Push telemetry packet directly to Kafka broker container
            producer.send(CRICKET_TOPIC, value=payload)
            print(f"🏏 Ball {overs} | Event: {payload['current_event']} | Score: {payload['total_score']} ➡️ Sent to Kafka")
            
    except KeyboardInterrupt:
        print("\n🛑 Stream generation halted manually.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()