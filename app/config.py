# MASTER = 'local[*]'
LOCAL_HOST = '192.168.137.1'
ES_NODES = '192.168.137.1:9200'
KAFKA_BROKER_CONSUMER = "localhost:9093" # submit to spark local
# KAFKA_BROKER_PRODUCER = "localhost:9093" # submit to spark local
CONNECTION_STRING = 'mongodb://localhost:60000'

MASTER = 'spark://127.0.0.1:7077'

# MASTER = 'spark://spark-master:7077'
# ES_NODES = 'elasticsearch:9200'
# KAFKA_BROKER_CONSUMER = "broker-1:9092" # submit to spark cluster
KAFKA_BROKER_PRODUCER = "broker-1:9092" # submit to spark cluster
# CONNECTION_STRING = "mongodb://router_mongos:27017"

AMAZON_TOPIC = "amazon"