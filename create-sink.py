import requests
from urllib.parse import quote_plus

payload = {
          "name": "mongo-sink",
          "config": {
             "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
             "connection.uri":"mongodb://%s:%s@%s:27017" % (quote_plus("root"), 
                                                         quote_plus("example"), 
                                                         "mongo-db"),
             "database":"kafka_topics",
             "collection":"traffic",
             "topics":"traffic",
             
             "value.converter": "org.apache.kafka.connect.storage.StringConverter",
             "value.converter.schemas.enable": "true"
              
             #"change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler"
             }
         }


url = "http://localhost:8083/connectors"




if __name__ == '__main__':
    r = requests.post( url, json=payload, headers={"Content-Type": "application/json" })
    print(r)
    print(r.content)
