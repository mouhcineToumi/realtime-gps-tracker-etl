from kafka import KafkaConsumer
import json




if __name__ == "__main__":
    consumer = KafkaConsumer( 'traffic', 
                            bootstrap_servers='localhost:9092', 
                            
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for msg in consumer:
        print (msg.value)
        print('-------')