from gevent import Greenlet

class GameServer(Greenlet):
    def __init__(self, code_name='Ark', version='dev', global_code_name=None,http_response_handler=None):
        Greenlet.__init__(self)
        pass