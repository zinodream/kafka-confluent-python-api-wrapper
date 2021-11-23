import xml.etree.ElementTree as ET
import ConfigObject
import ConfigLogger
import random_string

class ConfigManager:

    CONN_KAFKA = 101

    _kafka_dict = {}
    def __init__(self):
        # load config xml document
        self._rootDocument = None
        self._kafka_dict['bootstrap.servers'] = None

    def get_kafka_dict(self):
        return self._kafka_dict

    def load_config(self, path):
        '''
        load configuration from file
        '''
        self._rootDocument = ET.parse(path)

    def load_config_by_string(self, xml):
        '''
        load configuration from string
        '''
        self._rootDocument = ET.fromstring(xml)


    def get_attrubute(self, element, attr) :
        '''
        getting attribute from configuration xml
        '''
        if attr in element.attrib :
            return element.get(attr)
        else :
            return 0

    def get_connection_info(self, conn_type):

        if (conn_type == ConfigManager.CONN_KAFKA):
            root_find_path = "./connection/kafka"
            for element in self._rootDocument.findall(root_find_path):
                bootstrap_servers = self.get_attrubute(element, 'bootstrap.servers')
                transactional_id = self.get_attrubute(element, 'transactional.id')
                transaction_timeout_ms = self.get_attrubute(element, 'transaction.timeout.ms')
                client_id = self.get_attrubute(element, 'client.id')
                acks = self.get_attrubute(element, 'acks')
                batch_num_messages = self.get_attrubute(element, 'batch.num.messages')
                batch_size = self.get_attrubute(element, 'batch.size')
                connections_max_idle_ms = self.get_attrubute(element, 'connections.max.idle_ms')
                request_timeout_ms = self.get_attrubute(element, 'request.timeout.ms')
                enable_idempotence = self.get_attrubute(element, 'enable.idempotence')
                queue_buffering_max_messages = self.get_attrubute(element, 'queue.buffering.max.messages')
                queue_buffering_max_kbytes = self.get_attrubute(element, 'queue.buffering.max.kbytes')
                queue_buffering_max_ms = self.get_attrubute(element, 'queue.buffering.max.ms')
                message_send_max_retries = self.get_attrubute(element, 'message.send.max.retries')
                request_required_acks = self.get_attrubute(element, 'request.required.acks')
                message_timeout_ms = self.get_attrubute(element, 'message.timeout.ms')
                partitioner = self.get_attrubute(element, 'partitioner')
                compression_codec = self.get_attrubute(element, 'compression.codec')
                compression_type = self.get_attrubute(element, 'compression.type')
                compression_level = self.get_attrubute(element, 'compression.level')
                debug = self.get_attrubute(element, 'debug')

                ssl_context = self.get_attrubute(element, 'ssl.context')


                self._kafka_dict['bootstrap.servers'] = str(bootstrap_servers)
                if transactional_id != 0 and transactional_id != "":
                    appendix = random_string.generate()
                    self._kafka_dict['transactional.id'] = str(transactional_id)+appendix
                if transaction_timeout_ms != 0 and transaction_timeout_ms != "":
                    self._kafka_dict['transaction.timeout.ms'] = str(transaction_timeout_ms)
                if client_id != 0 and client_id != "":
                    self._kafka_dict['client.id'] = str(client_id)
                if acks != 0 and acks != "":
                    self._kafka_dict['acks'] = str(acks)
                if batch_size != 0 and batch_size != "":
                    self._kafka_dict['batch.size'] = str(batch_size)
                if batch_num_messages != 0 and batch_size != "":
                    self._kafka_dict['batch.num.messages'] = str(batch_num_messages)
                if connections_max_idle_ms != 0 and connections_max_idle_ms != "":
                    self._kafka_dict['connections.max.idle.ms'] = str(connections_max_idle_ms)
                if request_timeout_ms != 0 and request_timeout_ms != "":
                    self._kafka_dict['request.timeout.ms'] = str(request_timeout_ms)
                if ssl_context != 0 and ssl_context != "":
                    self._kafka_dict['ssl.context'] = str(ssl_context)
                if enable_idempotence != 0 and enable_idempotence != "":
                    self._kafka_dict['enable.idempotence'] = str(enable_idempotence)
                if queue_buffering_max_messages != 0 and queue_buffering_max_messages != "":
                    self._kafka_dict['queue.buffering.max.messages'] = str(queue_buffering_max_messages)
                if queue_buffering_max_kbytes != 0 and queue_buffering_max_kbytes != "":
                    self._kafka_dict['queue.buffering.max.kbytes'] = str(queue_buffering_max_kbytes)
                if queue_buffering_max_ms != 0 and queue_buffering_max_ms != "":
                    self._kafka_dict['queue.buffering.max.ms'] = str(queue_buffering_max_ms)
                if message_send_max_retries != 0 and message_send_max_retries != "":
                    self._kafka_dict['message.send.max.retries'] = str(message_send_max_retries)
                if request_required_acks != 0 and request_required_acks != "":
                    self._kafka_dict['request.required.acks'] = str(request_required_acks)
                if message_timeout_ms != 0 and message_timeout_ms != "":
                    self._kafka_dict['message.timeout.ms'] = str(message_timeout_ms)
                if partitioner != 0 and partitioner != "":
                    self._kafka_dict['partitioner'] = str(partitioner)
                if compression_codec != 0 and compression_codec != "":
                    self._kafka_dict['compression.codec'] = str(compression_codec)
                if compression_type != 0 and compression_type != "":
                    self._kafka_dict['compression.type'] = str(compression_type)
                if compression_level != 0 and compression_level != "":
                    self._kafka_dict['compression.level'] = str(compression_level)
                if debug != 0 and debug != "":
                    self._kafka_dict['debug'] = str(debug)

        return self._kafka_dict

    def get_commit_timeout(self, conn_type):

        if (conn_type == ConfigManager.CONN_KAFKA):
            root_find_path = "./connection/kafka"
            for element in self._rootDocument.findall(root_find_path):
                commit_timeout = int(int(self.get_attrubute(element, 'internal.commit.timeout'))/1000)

        return commit_timeout

    def get_transaction_timeout(self, conn_type):

        if (conn_type == ConfigManager.CONN_KAFKA):
            root_find_path = "./connection/kafka"
            for element in self._rootDocument.findall(root_find_path):
                transaction_timeout = int(int(self.get_attrubute(element, 'internal.transaction.timeout'))/1000)

        return transaction_timeout

    def get_log_info(self):
        '''
        read logger configuration
        '''
        config_logger = ConfigLogger.ConfigLogger()
        root_find_path = "./common/logger"

        logger = self._rootDocument.findall(root_find_path)[0]
        path = self.get_attrubute(logger, 'path')
        max_size = self.get_attrubute(logger, 'file_size')
        file_count = self.get_attrubute(logger, 'count')
        level = self.get_attrubute(logger, 'level')
        if level == 0:
            level = "info"
        data_dump = self.get_attrubute(logger, 'data_dump')
        console_log = self.get_attrubute(logger, 'console_log')

        config_logger.path = path
        config_logger.max_size = max_size
        config_logger.file_count = file_count
        config_logger.level = level
        config_logger.console_log = int(console_log)
        config_logger.data_dump = int(data_dump)

        return config_logger

    def get_interface_info(self):
        '''
        read interface configuration
        '''
        config_interface_list = []

        root_find_path = "./interfaces/interface"
        # xml to assing table field
        for interface in self._rootDocument.findall(root_find_path):
            config_interface = ConfigObject.ConfigInterface()
            config_interface.intf_id = self.get_attrubute(interface, 'intf_id')
            config_interface.topic_name = self.get_attrubute(interface, 'topic_name')
            config_interface.record_key = self.get_attrubute(interface, 'record_key')
            config_interface_list.append(config_interface)
        #endfor
        return config_interface_list








