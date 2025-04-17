from kafka import KafkaProducer
import json
import time
import random

class CricketDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generate_batting_data(self):
        return {
            "player_name": f"Player_{random.randint(1, 11)}",
            "runs": random.randint(0, 6),
            "balls": 1,
            "match_id": "match_001",
            "timestamp": int(time.time()),
            "event_type": "batting"
        }

    def generate_bowling_data(self):
        return {
            "bowler_name": f"Bowler_{random.randint(1, 6)}",
            "wickets": random.randint(0, 1),
            "runs_conceded": random.randint(0, 6),
            "match_id": "match_001",
            "timestamp": int(time.time()),
            "event_type": "bowling"
        }

    def generate_match_stats(self):
        return {
            "match_id": "match_001",
            "total_score": random.randint(0, 300),
            "run_rate": round(random.uniform(4.0, 12.0), 2),
            "overs_completed": random.randint(1, 50),
            "timestamp": int(time.time()),
            "event_type": "match_stats"
        }

    def start_producing(self):
        while True:
            rand = random.random()
            if rand < 0.7:  # 70% batting events
                data = self.generate_batting_data()
                self.producer.send('cricket-batting-topic', data)
            elif rand < 0.9:  # 20% bowling events
                data = self.generate_bowling_data()
                self.producer.send('cricket-bowling-topic', data)
            else:  # 10% match stats events
                data = self.generate_match_stats()
                self.producer.send('cricket-match-stats-topic', data)
            
            print(f"Sent: {data}")
            time.sleep(2)  # Simulate 2-second delay between balls

if __name__ == "__main__":
    producer = CricketDataProducer()
    producer.start_producing()