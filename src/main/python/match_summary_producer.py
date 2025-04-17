from kafka import KafkaProducer
import json
import time
from faker import Faker
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

teams = ["RCB", "MI", "CSK", "KKR", "RR", "SRH", "DC", "PBKS", "LSG", "GT"]
results = ["normal", "tie", "super over", "no result"]

while True:
    team1, team2 = random.sample(teams, 2)
    data = {
        "match_id": fake.uuid4(),
        "date": fake.date(),
        "venue": fake.city(),
        "team1": team1,
        "team2": team2,
        "toss_winner": random.choice([team1, team2]),
        "match_winner": random.choice([team1, team2]),
        "result_type": random.choice(results),
        "umpires": [fake.name(), fake.name()]
    }
    print(f"Producing: {data}")
    producer.send("match_details_topic", value=data)
    time.sleep(2)  # every 2 seconds

