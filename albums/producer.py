
from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='concise-tortoise-8904-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='Y29uY2lzZS10b3J0b2lzZS04OTA0JFRJRHcSBjUvE9uNHUuc7M3P1BZsP-klAZU',
    sasl_plain_password='NGJhY2VmMzItZmNjNi00ODI2LTkwYjUtZDVlMTA0MTI1NjM5',
    api_version_auto_timeout_ms=100000
)

tracks = pd.read_csv('albums.csv')

for dt in tracks.to_dict(orient='records'):
    data = json.dumps(dt).encode('utf-8')

    try:
        result = producer.send('album', data).get(timeout = 60)    
        print("Message produced:", result)
    except Exception as e:
        print(f"Error producing message: {e}")
producer.close()