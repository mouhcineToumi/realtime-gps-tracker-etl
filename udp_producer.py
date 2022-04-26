import datetime
from kafka import KafkaProducer
import json
import socket





def parse_payload( data ):
    
    def decode_bytes( s ):
        if isinstance(s, bytes):
            s = s.decode()
            s = float(s.strip().replace("b", "").replace("'", ""))
        return s


    def rename_keys( data ):
        mapper = {
            1: "gps_position",
            3: "accelerometer",
            4: "gyroscope",
            5: "magnetic_field",
            81: "orientation",
            82: "linear_acceleration",
            83: "gravity",
            84: "rotation_vector",
            85: "pressure",
            86: "battery_temperature",
        }
        d = {}
        for k in data:
            if type(k) == str:
                kk = int(k)
            else:
                kk = k
            
            if kk in range(1, 6):
                d[mapper[kk]] = data[k]
        return d
    
    def string_to_dictionary( data ):
        d = {}
        i = 0
        while i < len(data):
            if data[i] in (85, 86):
                step=2
                d[data[i]] = data[i+1:i+step]
            else:
                step = 4
                d[data[i]] = data[i+1:i+step]
            i += step
        return d


    data = data.decode()
    data = list(map( decode_bytes, data.split(',') ))
    data = string_to_dictionary(data[1:])
    
    return rename_keys( data )





class UdpReceiver:
    def __init__(self, port=5555):
        self.UDP_IP = ""
        self.UDP_PORT = port
        self.sock = socket.socket(socket.AF_INET, # Internet
                          socket.SOCK_DGRAM) # UDP
        self.sock.bind((self.UDP_IP, self.UDP_PORT))
        
        # Kafka producer
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda x: json.dumps(x).encode("utf-8"))
        
    
    
    def run( self, duration_s=5*60):
        start = datetime.datetime.now()
        end = datetime.datetime.now()
        while (end-start).seconds < duration_s:
            data, addr = self.sock.recvfrom(1024)
            end = datetime.datetime.now()
            d = {}
            d['address'] = addr
            d['payload'] = parse_payload(data)
            d['time'] = end.isoformat() 
            
            print(d['payload'])
            self.producer.send('traffic', d)
            
    
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.sock.close()

        
if __name__ == "__main__":
    udp_receiver = UdpReceiver()
    udp_receiver.run()