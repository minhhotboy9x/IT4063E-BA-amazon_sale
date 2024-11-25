# IT4063E-BA-amazon_sale
Course project of IT4063E Hust

This project is a small simulating system of data storing and analysis using Spark. The data is from [Kaggle](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset).

### Install (window local machine)
Docker, MongoDB, pyspark==3.3.1

#### Run docker-compose
```
IT4063E-BA-amazon_sale\docker> docker-compose up -d
```
Spark cluster and Kafka cluster are set. Only setup MongoDB cluster manually.

#### Create sharding DB for MongoDB cluster 
(Use cmd of local machine)

##### Config server replicaset
Access config server 
```
> mongosh localhost:40001
```
Create replicaSet for config server
```
> rs.initiate({_id: "cfgrs", configsvr: true, members:[{ _id: 0, host: "cfgsvr1:27017" }]})
```

##### Sharding server replicaset
Access shard1svr1
```
> mongosh localhost:50001
```
Create replicaSet for shard1
```
> rs.initiate({_id: "shard1rs",members: [{ _id : 0, host : "shard1svr1:27017" },{ _id : 1, host : "shard1svr2:27017" },{ _id : 2, host : "shard1svr3:27017" }]})
```

##### Add shard server to router and create sharding DB
Access mongos router
```
> mongosh localhost:60000
```
Add shard1
```
> sh.addShard("shard1rs/shard1svr1:27017,shard1svr2:27017,shard1svr3:27017")
```
Create sharding database `IT4063E`
```
> use IT4063E
> sh.enableSharding("IT4063E")
```
Create sharding collection
```
> db.Amazon.createIndex({_id: 'hashed'})
> sh.shardCollection("IT4063E.Amazon", { _id: 'hashed' })
```

### Run application
Move to app
```
> cd app
```

Insert `amazon.csv` into MongoDB
```
> python insert_mongodb.py
```

Run producer and consumer to start streaming (this is the simulated streaming since the hurdle between the network of docker containers and network of window local machine makes Spark streaming fail to connect to Kafka when submitting jobs to the Spark cluster)
```
> python producer.py
```
```
> python consumer.py
```

Access `localhost:5601` and create diagram on Kibana

