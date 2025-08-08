
import os
from gevent import monkey
import simplejson as json
import base64
import datetime
import falcon
from simplejson import JSONEncoder
monkey.patch_all()
from SlotServer.Network import Engine
from SlotServer.Network.BaseServer import BaseServer
from SlotServer.Network.Web import WebAPI
import traceback

class ServerKey:
    def __init__(self):
        pass

    def on_get(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = "7323abe983eaa00bbbab1ced787261c9"

    def on_post(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = "7323abe983eaa00bbbab1ced787261c9"

class Login:
    def __init__(self):
        pass

    def on_get(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = "eyJhdXRvX2lkIjoiMTAwMDAwMiIsImludml0ZV9jb2RlIjoiUERVVUJUUFJEIn0="

    def on_post(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = "eyJhdXRvX2lkIjoiMTAwMDAwMiIsImludml0ZV9jb2RlIjoiUERVVUJUUFJEIn0="

class Auth:
    def __init__(self):
        pass

    def on_get(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = "eyJhcmtfaWQiOiIxMDAwMDAyIiwiYXJrX3Rva2VuIjoiNmRiMWI3N2IwODc0MDhlNGY1N2M1YTNmYTZiYjVmZTUifQ=="

class CommonSystem:
    def __init__(self):
        pass

    def asset(self, req, resp, kwargs):
        doc = {
            'ThirdPartyNick': "1000000001",
            'ThirdPartyName': "1000000001",
            'LineCode': "",
            'Balance': 100000
        }
        result = {
            'cmd_data': {
                'Code': 0,
                'Coin': 100000,
            }
        }

        result = getResult(result)
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = result


    def get_user_info(self, req, resp, kwargs):
        doc = {
            'ThirdPartyNick': "1000000001",
            'ThirdPartyName': "1000000001",
            'LineCode': "",
            'Balance': 100000
        }
        result = {
            'cmd_data': {
                'Code': 0,
                'status': {'id': 0},
                'data': doc,
                'ts': datetime.datetime.now().timestamp()
            }
        }

        result = getResult(result)
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.body = result

def Command(req, resp, kwargs):
    body = req.stream.read()
    cmd_id, cmd_name = get_cmd(body)
    if cmd_id is None or cmd_name is None:
        resp.status = falcon.HTTP_500
        resp.body = None
        raise Exception("WebAPI:on_get Exception : " + str(traceback.format_exc()))
    func = CMD_MAP.get(cmd_name)
    if func is None:
        resp.status = falcon.HTTP_500
        resp.body = None
        raise Exception("WebAPI:on_get Exception : " + str(traceback.format_exc()))
    func(req, resp, kwargs)

class LoginServer(BaseServer):
    def __init__(self, code_name='LoginService', env='local', role='Game', channel=None):
        version = env if channel is None else (env + '-' + channel)
        version = version if version is not None else 'local'
        super(LoginServer, self).__init__(code_name, version, global_code_name=None)
        # self.init_variable(code_name)
        serverKey = ServerKey()
        login = Login()
        auth = Auth()
        self.app.add_route('/', serverKey)
        self.app.add_route('/login', login)
        self.app.add_route('/auth', auth)



def getResult(rData):
    data = json.dumps(rData, separators=(',', ':'), cls=DatesToStrings)
    result = base64.standard_b64encode(data.encode('utf-8'))
    return result

class DatesToStrings(JSONEncoder):
    def _encode(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {self._encode(k): self._encode(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._encode(v) for v in obj]
        else:
            return obj

def addRoute(route, controller, service):
    web_api = WebAPI(controller)
    url = route
    service.addWebAPI(url, web_api)

def get_cmd(body):
    data = base64.standard_b64decode(body)
    form = json.loads(data, encoding='utf-8')
    data = form['ark_data']
    data = base64.standard_b64decode(data)
    data = json.loads(data, encoding='utf-8')
    cmd_id = data['cmd_id']
    cmd_name = data['cmd_name']
    return cmd_id, cmd_name

codename = "LoginService"
globalCodeName = os.environ.get("GLOBAL_CODE_NAME")
globalCodeName = "LoginService" if globalCodeName is None else globalCodeName
env = os.environ.get("PROJ_ENV")
channel = os.environ.get("PROJ_CHANNEL")
loginService = LoginServer(globalCodeName, env=env, role="Game", channel=channel)

commonSystem = CommonSystem()
addRoute('/command', Command, loginService)

CMD_MAP = {
    'get_user_info': commonSystem.get_user_info,
    'GET_ASSET_SETTING': commonSystem.asset,
    'GET_ASSET': commonSystem.asset,
}

Engine.instance().start(loginService)
httpApp = loginService.app

if __name__ == '__main__':
    Engine.instance().startHTTPWSGI(httpApp, host='0.0.0.0', port=8080)
    Engine.instance().run()