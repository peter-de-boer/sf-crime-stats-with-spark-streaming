from kafka import KafkaConsumer
import json

def consume():
    
    consumer = KafkaConsumer(
        'nd.project.crimestats',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        if message is None:
            print("...no message")
        elif message.value is None:
            print("...empty message")
        else:
            print(message.value)
        
if __name__ == "__main__":
    consume()