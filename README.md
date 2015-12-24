rocketmq-storm.jar

DemoTopology see test/java/com.alibaba.rocketmq.storm.topology.DemoTopology

kafka.config.properties:
kafka.broker.list=10.1.15.41:9092,10.1.15.40:9092
kafka.serializer.class=kafka.serializer.StringEncoder
kafka.key.serializer.class=kafka.serializer.StringEncoder
kafka.producer.type=sync
kafka.topic=yeahmobi_vncc_lbs
。。。

redis.config.properties:
#最大能够保持idle状态的对象数
redis.pool.maxIdle=200
#当池内没有返回对象时，最大等待时间
redis.pool.maxWait=1000
#当调用borrow Object方法时，是否进行有效性检查
redis.pool.testOnBorrow=true
#当调用return Object方法时，是否进行有效性检查
redis.pool.testOnReturn=true
#IP
redis.ip=eager.t8cuil.0001.use1.cache.amazonaws.com
#redis.ip=172.30.30.9
#Port
redis.port=6379
redis.pool.minIdle=20
redis.pool.maxTotal=500

storm.config.properties:
rocketmq.spout.consumer.group=CG_EAGLE_FLUME_NGINX
rocketmq.spout.topic=T_EAGLE_FLUME_NGINX
rocketmq.spout.topic.tag=*
topology.name=EAGLE_FLUME_EVENT
topology.spout.parallel=5
topology.bolt.parallel=1
topology.acker.executors=1
topology.workers=1
topology.max.spout.pending=100
topology.message.timeout.secs=60
topology.jvm=-Duser.timezone=UTC -Xms1g -Xmx1g
rocketmq.spout.ordered=false
rocketmq.enable_ssl=true
rocketmq.client.pwd=4R4MNv54OnDeoHSRnngFkA==
#rocketmq.namesrv.domain=54.169.203.57
# version 1.3
rocketmq.log.home=/dianyi/app/logs