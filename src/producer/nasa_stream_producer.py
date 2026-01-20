"# NASA API Kafka Producer Placeholder" 



import json
import time
import requests
from kafka import KafkaProducer

# --------------------------
# CONFIGURATION
# --------------------------

API_KEY = "b6WPfDRytMTSTDvvPFqO3qDUYtWVqE8SNg13fzfn"   
NASA_URL = f"https://api.nasa.gov/neo/rest/v1/feed?api_key={API_KEY}"
KAFKA_TOPIC = "asteroid_raw"
KAFKA_SERVER = "localhost:9092"

# --------------------------
# INITIALIZE KAFKA PRODUCER
# --------------------------

producer = KafkaProducer(
    bootstrap_servers=['10.0.0.5:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 NASA → Kafka Producer Started")
print(f"Sending asteroid data to Kafka topic: {KAFKA_TOPIC}")
print("=" * 60)

# --------------------------
# MAIN LOOP
# --------------------------

while True:
    try:
        # Fetch latest asteroid data
        response = requests.get(NASA_URL)
        data = response.json()

        # Extract only the "near_earth_objects" data
        if "near_earth_objects" in data:
            for date_key, items in data["near_earth_objects"].items():
                for asteroid in items:
                    message = {
                        "id": asteroid.get("id"),
                        "name": asteroid.get("name"),
                        "absolute_magnitude": asteroid.get("absolute_magnitude_h"),
                        "is_hazardous": asteroid.get("is_potentially_hazardous_asteroid"),
                        "velocity_kph": asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"],
                        "miss_distance_km": asteroid["close_approach_data"][0]["miss_distance"]["kilometers"],
                        "approach_date": date_key
                    }

                    producer.send(KAFKA_TOPIC, message)
                    print(f"Sent to Kafka: {message}")

        print("Cycle completed. Waiting 10 seconds...\n")
        time.sleep(10)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(5)
