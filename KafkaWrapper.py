# This is a Confluent Kafka Producer Wrapper Python script.
"""
'''
kafka producer sample for how transaction works.
The transactional producer operates on top of the idempotent producer, and provides full exactly-once semantics (EOS)
    for Apache Kafka when used with the transaction aware consumer (isolation.level=read_committed).
'''
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

'''
kafka producer sample for how non transaction works
'''
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
"""
from confluent_kafka import Producer, KafkaError, KafkaException
import CommonLogger
import ConfigManager
import time
import sys
from version import __version__

class KafkaWrapper:
    _logger = None
    _config_manager = None
    _kafka_connection_info = None
    _interface_list = []
    _commit_timeout = 60
    _transaction_timeout = 60

    def __init__(self):

        self._logger = CommonLogger.CommonLogger()
        self._config_manager = ConfigManager.ConfigManager()
        self._delivered_records = 0
        self._rollback_records = 0

    def __del__(self):
        del self._logger

    def kafka_connect(self, auto_commit=False):
        """
        kafka connection function.
        By default, auto-commit works as false.
        For configuration for connection, input dict type parameters.
        To use the transaction function, transaction.id and transaction.timeout.ms must be included in the configuration.
        If auto_commit is set to true as a parameter of this function, transaction does not work,
            kafka_commit and kafka_rollback functions cannot be used, and kafka_flush function must be used.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            #self._producer = Producer({'bootstrap.servers': '10.10.19.99:9092'})
            self._logger.debug(f"[{myfunc}] Kafka connection start")
            temp_kafka_connection_info = self._kafka_connection_info
            if auto_commit == True:
                del temp_kafka_connection_info['transactional.id']
                del temp_kafka_connection_info['transaction.timeout.ms']
                self._producer = Producer(temp_kafka_connection_info)
            else:
                self._producer = Producer(temp_kafka_connection_info)
            self._logger.info(f"[{myfunc}] Kafka connected. config:({temp_kafka_connection_info})")

        except Exception as err:
            self._logger.error(f"[{myfunc}] Kafka connection error. err: ({err}), config:({temp_kafka_connection_info})")
            raise

    def kafka_begin_transaction(self):
        """
        To use the transaction function, it must be declared at the beginning of the transaction.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            self._logger.debug(f"[{myfunc}] Kafka init transaction start")
            self._producer.init_transactions(self._transaction_timeout)
            self._logger.debug(f"[{myfunc}] Kafka begin transaction start")
            self._producer.begin_transaction()
            self._delivered_records = 0
            self._rollback_records = 0
            self._logger.info(f"[{myfunc}] Kafka begin transaction success")
        except Exception as err:
            self._logger.error(f"[{myfunc}] Kafka init transaction error. err: ({err}), transaction timeout: {self._transaction_timeout}")
            raise

    def kafka_put(self, intf_id, message):
        """
        This is a function that puts a message into kafka.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            for lst in self._interface_list:
                if lst.intf_id == intf_id:
                    topic_name = lst.topic_name
                    record_key = lst.record_key
                else:
                    self._logger.error(f"[{myfunc}] not found intf_id:{intf_id}")
                    raise Exception("NotFoundInterfaceID")
        except Exception as err:
            self._logger.error(f"[{myfunc}] {err}")
            raise

        try:
            put_message = str.encode(message)
            self._producer.produce(topic_name, put_message, callback=self.kafka_put_acked)
            self._logger.debug(f"[{myfunc}] Kafka put success. topic:{topic_name}, msg:[{message}]")
            self._logger.dump(put_message)

        except Exception as err:
            self._logger.error(f"[{myfunc}] {err}")
            raise

    def kafka_put_acked(self, err, msg):
        """
        Delivery report handler called on successful or failed delivery of message
        """
        myfunc = sys._getframe(0).f_code.co_name
        myfunc_parent = sys._getframe(1).f_code.co_name
        if err is not None:
            self._rollback_records += 1
            self._logger.debug(f"[{myfunc_parent}][{myfunc}] Failed to deliver message: {err}")
        else:
            self._delivered_records += 1
            self._rollback_records += 1
            self._logger.debug(f"[{myfunc_parent}][{myfunc}] Message delivered to topic: {msg.topic()}, partition: {msg.partition()}")

    def kafka_flush(self):
        """
        A function that flushes messages when no transaction is used.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            remained_message_count = len(self._producer)
            self._logger.info(f"[{myfunc}] Kafka flush start. remained message count: {remained_message_count}, timeout: {self._commit_timeout} sec")
            ret = self._producer.flush(self._commit_timeout)
            self._logger.info(f"[{myfunc}] Kafka flush success. flush message count: {remained_message_count-ret}/{remained_message_count}")

        except Exception as err:
            self._logger.error(f"[{myfunc}] {err}")
            raise

    def kafka_rollback(self):
        """
        rollback function.
        After calling the kafka_begin_transaction() function, rollback the put message
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            self._producer.abort_transaction()
            self._logger.info(f"[{myfunc}] Kafka rollback success. message count: {self._rollback_records}")
            self._rollback_records = 0
        except Exception as err:
            self._logger.error(f"[{myfunc}] {err}")
            raise

    def kafka_commit(self):
        """
        This function commits the put message after the begin_transaction() function is declared.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            self._logger.info(f"[{myfunc}] Kafka commit start. commit timeout: {self._commit_timeout} sec")
            s_time = time.time()
            self._producer.commit_transaction(self._commit_timeout)
            e_time = time.time()
            self._logger.info(f"[{myfunc}] Kafka commit success. message count: {self._delivered_records}, elapsed time: {round(e_time-s_time, 3)} sec")
            self._delivered_records = 0

        except Exception as err:
            try:
                if err.__class__.__name__ == "KafkaException":
                    if err.args[0].code() == KafkaError._TIMED_OUT:
                        self._logger.info(f"[{myfunc}] {err}")
                        if err.args[0].retriable() == True:
                            self._logger.info(f"[{myfunc}] commit retry. count: {len(self._producer)}, commit timeout: {self._commit_timeout} sec")
                            s_time = time.time()
                            self._producer.commit_transaction(self._commit_timeout)
                            e_time = time.time()
                            self._logger.debug(f"[{myfunc}] end of commit retry. {len(self._producer)}")
                            if len(self._producer) == 0:
                                self._logger.info(f"[{myfunc}] Kafka commit success. message count: {self._delivered_records}, elapsed time: {round(e_time-s_time, 3)} sec")
                            else:
                                raise
                        else:
                            raise
                    elif err.args[0].retriable() == True:
                        self._logger.info(f"[{myfunc}] {err}")
                        self._logger.info(f"[{myfunc}] Wait process messages. commit timeout: {self._commit_timeout} sec")
                        #self._producer.poll(0)
                        self._producer.commit_transaction(self._commit_timeout)
                        self._logger.debug(f"[{myfunc}] end of wait process messages.")
                        if len(self._producer) == 0:
                            self._logger.info(f"[{myfunc}] Kafka wait process success. message count: {self._delivered_records}")
                        else:
                            raise
                    elif err.args[0].txn_requires_abort() == True:
                        self._logger.error(f"[{myfunc}] {err}")
                        self._logger.error(f"[{myfunc}] txn_requires_abort():{err.args[0].txn_requires_abort()}. Try rollback")
                        self._producer.abort_transaction()
                    else:
                        raise
                else:
                    raise
            except Exception as err:
                self._logger.error(f"[{myfunc}] Exception {err}")
                raise

    def kafka_disconnect(self):
        """
        This function terminates the kafka producer instance. Must be called for correct disconnect
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            del self._producer
            self._logger.info(f"[{myfunc}] Kafka disconnected")
        except Exception as err:
            self._logger.error(f"[{myfunc}] {err}")
            raise

    def load_config(self, config_path):
        """
            This function reads the configuration values used in this KafkaWrapper class from the config xml file.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            ## LOAD CONFIG
            self._config_manager.load_config(config_path)

            #logger
            # config - logger
            config_logger = self._config_manager.get_log_info()
            self._logger.set_initialize(config_logger.path, config_logger.level, config_logger.max_size, config_logger.file_count, config_logger.console_log, config_logger.data_dump)
            self.version()
            self._logger.info(f"[{myfunc}] Config file: ({config_path})")
            self._logger.info(f"[{myfunc}] Log path: {config_logger.path}, level: {config_logger.level}, max size: {config_logger.max_size}, file count: {config_logger.file_count}, console log: {config_logger.console_log}")

            #kafka
            self._kafka_connection_info = self._config_manager.get_connection_info(self._config_manager.CONN_KAFKA)
            self._logger.debug(f"[{myfunc}] Connection Config {self._kafka_connection_info}")

            self._commit_timeout = self._config_manager.get_commit_timeout(self._config_manager.CONN_KAFKA)
            self._transaction_timeout = self._config_manager.get_transaction_timeout(self._config_manager.CONN_KAFKA)
            self._logger.debug(f"[{myfunc}] Internal Config. commit timeout: {self._commit_timeout} sec, transaction timeout: {self._transaction_timeout} sec")

            #interface
            self._interface_list = self._config_manager.get_interface_info()
            for lst in self._interface_list:
                self._logger.debug(f"[{myfunc}] Interface Config. interface id:{lst.intf_id}, topic name: {lst.topic_name}, record key: {lst.record_key}")

        except Exception as err:
            print("load config error: {}".format(err))
            raise

    def logging(self, type, value):
        if type == "info":
            self._logger.info(f"[USERLOG] {value}")
        elif type == "debug":
            self._logger.debug(f"[USERLOG] {value}")
        elif type == "error":
            self._logger.error(f"[USERLOG] {value}")
        else:
            pass

    def version(self):
        self._logger.info(f"API version: {__version__}")
