import datetime
import time
from kafka import KafkaProducer

class TrafficProducer:
    
    def __init__(self, 
                     device_id: int,     
                     volume: int = 100,
                     lat: float = 1111,
                     long: float = 2222,
                     topic: str="traffic",
                     delay: int = 1):
        self.volume = volume
        self.lat = lat
        self.long = long
        self.device_id = device_id
        self.topic = topic
        self.delay = delay
        
        # connect to Kafka
        # TODO : fail gracefully
        self.producer = KafkaProducer( bootstrap_servers='localhost:9092', 
                                       value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    
    
    def run(self):
        for i in range(self.volume):
            data = {
                'device_id': self.device_id,
                'lat': self.lat+i,
                'long': self.long-i,
                'date': datetime.datetime.now().isoformat() 
                }
            self.producer.send(self.topic, data)
            time.sleep(self.delay)




if __name__ == '__main__':
    producer = TrafficProducer(device_id=0, volume=10)
    producer.run()