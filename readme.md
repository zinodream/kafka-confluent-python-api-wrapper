# This is a Confluent Kafka Producer Wrapper Python script.

kafka producer sample for how transaction works.
The transactional producer operates on top of the idempotent producer, and provides full exactly-once semantics (EOS)
    for Apache Kafka when used with the transaction aware consumer (isolation.level=read_committed).

    kafkaWrapper = KafkaWrapper.KafkaWrapper()
    kafkaWrapper.load_config('kafka_config.xml')
    kafkaWrapper.kafka_connect()
    kafkaWrapper.kafka_begin_transaction()
    for i in range(10):
        # 'TEST' is an Interface ID, and topic names can be mapped for each Interface ID in the kafka_config.xml file.
        kafkaWrapper.kafka_put('TEST', 'hello world')
    kafkaWrapper.kafka_commit()
    #kafkaWrapper.kafka_rollback()
    kafkaWrapper.kafka_disconnect()
    kafkaWrapper.logging("info", "program end")
    del(kafkaWrapper)


kafka producer sample for how non transaction works

    kafkaWrapper = KafkaWrapper.KafkaWrapper()
    kafkaWrapper.load_config('kafka_config.xml')
    kafkaWrapper.kafka_connect()
    for i in range(10):
        # 'TEST' is an Interface ID, and topic names can be mapped for each Interface ID in the kafka_config.xml file.
        kafkaWrapper.kafka_put('TEST', 'hello world')
    kafkaWrapper.kafka_flush()
    kafkaWrapper.kafka_disconnect()
    kafkaWrapper.logging("info", "program end")
    del(kafkaWrapper)


# kafka_config.xml format
```
<!-- basic config setting -->
<config>
	<!-- log setting -->
	<common>
		<logger path="./kafkaapi.log" file_size="10485760" count="10" level="info" data_dump="0" console_log="1"/>
	</common>
	<!-- connection setting -->
	<connection>
		<kafka bootstrap.servers="10.10.19.99:9092"
			transactional.id="TID_"
			transaction.timeout.ms="60000"
			client.id="iguazio_producer"
			internal.commit.timeout="60000"
			internal.transaction.timeout="60000"
		/>
	</connection>
	<!-- interface setting -->
	<interfaces>
		<interface intf_id="TEST" topic_name="test"/>
	</interfaces>
</config>
```
```
<!-- advanced config setting for connection/kafka -->
	<connection>
		<kafka bootstrap.servers="10.10.19.99:9092"
			transactional.id="TID_"
			transaction.timeout.ms="60000"
			client.id="iguazio_producer"
			acks="all"
			batch.size="1000000"
			batch.num.messages="10000"
			connections.max.idle_ms="540000"
			request.timeout.ms="30000"
			enable.idempotence="true"
			queue.buffering.max.messages="10000000"
			queue.buffering.max.kbytes="1048576"
			queue.buffering.max.ms="5"
			message.send.max.retries="2147483647"
			message.timeout.ms="60000"
			partitioner="consistent_random"
			compression.codec="none"
			compression.type="none"
			compression.level="-1"
			internal.commit.timeout="60000"
			internal.transaction.timeout="60000"
		/>
	</connection>
```
