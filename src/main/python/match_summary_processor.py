from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'match_details_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Consuming from match_details_topic...")
for message in consumer:
    match = message.value
    print(f"Match ID: {match['match_id']} | {match['team1']} vs {match['team2']} at {match['venue']} | Winner: {match['match_winner']} ({match['result_type']})")

