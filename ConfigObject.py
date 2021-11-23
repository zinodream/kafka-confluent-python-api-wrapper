

class ConfigInterface:
    _intf_id = None
    _topic_id = None
    _record_key = None

    def __init__(self):
        '''
        interface configuration
        '''
        self._interface_list = []

    def set_interface_list(self, interface_list):
        self._interface_list = interface_list

    def get_interface_list(self):
        return self._interface_list

    @property
    def intf_id(self):
        return self._intf_id

    @intf_id.setter
    def intf_id(self, value):
        self._intf_id = value

    @property
    def topic_name(self):
        return self._topic_name

    @topic_name.setter
    def topic_name(self, value):
        self._topic_name = value

    @property
    def record_key(self):
        return self._record_key

    @record_key.setter
    def record_key(self, value):
        self._record_key = value
