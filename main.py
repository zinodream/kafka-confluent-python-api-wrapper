import KafkaWrapper
import time

if __name__ == '__main__':

    '''
    kafka producer sample for how transaction works
    '''
    kafkaWrapper = KafkaWrapper.KafkaWrapper()
    kafkaWrapper.load_config('kafka_config.xml')
    kafkaWrapper.kafka_connect()
    kafkaWrapper.kafka_begin_transaction()
    for i in range(1):
        kafkaWrapper.kafka_put('TEST', 'hello world')

    kafkaWrapper.kafka_commit()
    #kafkaWrapper.kafka_rollback()
    kafkaWrapper.kafka_disconnect()

    kafkaWrapper.logging("info", "program end")
    del(kafkaWrapper)

