import time
import falcon
from Game.Server.Network import Engine
from Game.Server.Network.BaseServer import BaseServer
from Game.Server.Network.Web import  WebAPI
import  os
import json
import datetime
from simplejson import JSONEncoder
import base64
from Game.SlotService import SlotService

class MockCommonSystem():
    def __init__(self, server):
        self.server = server
        self.logger = server.logger
        self.MongoFactory = server.MongoFactory
        self.walletMgr = server.walletMgr
        self.GetPlayerDataFunc = server.GetPlayerDataFunc
        self.cmdDict = {
            "GET_ASSET": self.onGetAsset,
            "get_user_info": self.onGetUserInfo,
            "GET_ASSET_SETTING": self.onGetAsset
        }

    def onGetAsset(self, ark_id, cmd_data):
        r = self.getAsset(ark_id)
        return r

    def onGetUserInfo(self, ark_id, cmd_data):
        r = self.GetPlayerDataFunc(ark_id)
        doc = {
            'ThirdPartyNick': r.get('ThirdPartyNick'),
            'ThirdPartyName': r.get('ThirdPartyName'),
            'LineCode': "",
            'Balance': 100000
        }
        result = {
            'Code': 0,
            'status': {'id': 0},
            'data': doc,
            'ts': datetime.datetime.now().timestamp()
        }
        return result

    def getAsset(self, ark_id):
        result = self.server.walletMgr.GetCredit(ark_id, ["Coin"])
        return result

class ServerKey:
    def __init__(self):
        pass

    def on_get(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.text = "7323abe983eaa00bbbab1ced787261c9"

class Login:
    def __init__(self):
        pass

    def on_post(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.text = "eyJhdXRvX2lkIjoiMTAwMDAwMiIsImludml0ZV9jb2RlIjoiUERVVUJUUFJEIn0="

class Auth:
    def __init__(self):
        pass

    def on_post(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.text = "eyJhcmtfaWQiOiIxMDAwMDAyIiwiYXJrX3Rva2VuIjoiNmRiMWI3N2IwODc0MDhlNGY1N2M1YTNmYTZiYjVmZTUifQ=="

class Uuid:
    def __init__(self):
        pass

    def on_get(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.text = "ImMxZmMyOTdiLWQzZWQtMTFlZi1iZDhkLWE0YmI2ZDVlZjQ2YiI="

    def on_post(self, req, resp):
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.status = falcon.HTTP_200
        resp.text = "ImMxZmMyOTdiLWQzZWQtMTFlZi1iZDhkLWE0YmI2ZDVlZjQ2YiI="

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

def ServerInit(server):
    serverKey = ServerKey()
    login = Login()
    auth = Auth()
    uuid = Uuid()
    server.app.add_route('/', serverKey)
    server.app.add_route('/login', login)
    server.app.add_route('/auth', auth)
    server.app.add_route('/drtcmd', uuid)

    server.MockCommonSystem = MockCommonSystem(server)
    handler = Handler(server)
    server.addWebAPI("/command", WebAPI(handler.CommandHandler))

class Handler:
    def __init__(self, server):
        self.logger = server.logger
        self.systemDict = server.systemDict
        self.MockCommonSystem = server.MockCommonSystem
        self.codeName = server.codeName

    def on_options(self, req, resp, **kwargs):
        """處理 CORS 預檢請求"""
        resp.set_header('Access-Control-Allow-Origin', '*')
        resp.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        resp.set_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        resp.status = falcon.HTTP_204  # 204 No Content

    def CommandHandler(self, req, resp, kwargs):
        body = req.stream.read()
        # print(data)
        data = base64.standard_b64decode(body)
        json_string = data.decode('utf-8')  # 將字節序列解碼為字串
        form = json.loads(json_string)

        data = form['ark_data']
        data = base64.standard_b64decode(data)
        json_string = data.decode('utf-8')  # 將字節序列解碼為字串
        data = json.loads(json_string)
        if isinstance(data, dict) and 'cmd_data' in data and isinstance(data['cmd_data'], dict):
            start = time.time()
            ark_id = data['ark_id']
            cmd_id = data['cmd_id']
            cmd_name = data["cmd_name"]
            cmd_data = data['cmd_data']
            if cmd_id == "SlotGame":
                result = self.systemDict[cmd_id].function_dict[cmd_name](ark_id, cmd_data, data)
                # result = self.systemDict[cmd_id]._command(ark_id, cmd_data, data)
            else:
                result = self.MockCommonSystem.cmdDict[cmd_name](ark_id, cmd_data)
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Access-Control-Allow-Origin', '*')
            resp.status = falcon.HTTP_200
            # resp.text = json.dumps({"cmd_sn": data.get("cmd_sn", 0), "cmd_data": result})
            rs = json.dumps({"cmd_sn": data.get("cmd_sn", 0), "cmd_data": result}, separators=(',', ':'), cls=DatesToStrings)
            resp.text = base64.standard_b64encode(rs.encode('utf-8'))
            self.logger.debug(str(self.codeName) + ' Command' + " %s sec" % (time.time() - start) + ' ark_data:' + str(data) + " result:%s" % result)

class CORSMiddleware:
    """允許 CORS 設定"""
    def process_request(self, req, resp):
        """處理 OPTIONS 預檢請求，避免回應 404"""
        if req.method == 'OPTIONS':
            resp.status = falcon.HTTP_200  # 確保 OPTIONS 回應 200
            resp.set_header("Access-Control-Allow-Origin", "*")
            resp.set_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
            resp.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
            resp.complete = True  # 阻止 Falcon 繼續處理，避免 404

    def process_response(self, req, resp, resource, req_succeeded):
        """處理一般 CORS"""
        resp.set_header("Access-Control-Allow-Origin", "*")
        resp.set_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        resp.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

# 建立 Falcon 應用並添加 Middleware
app = falcon.App(middleware=[CORSMiddleware()])

class HelloResource:
    def on_get(self, req, resp):
        resp.media = {"message": "Hello, Falcon!"}

app.add_route("/", HelloResource())  # 確保 `/` 路由存在

if __name__ == '__main__':

    os.chdir("Game")
    code_name = "SlotLobby"
    env = "local"  # local, dev, test, uat, prod
    global_code_name = "pixiu"
    Role = "Simple"  # Simple: 不用ArkEngine
    httpAppPort = 8081

    SlotService.__bases__ = (BaseServer,)
    gameServer = SlotService(code_name, global_code_name, env, Role)
    ServerInit(gameServer)
    # gameServer = MultiGameServer("MultiGame", 'pixiu', env, role, channel)
    Engine.instance().start(gameServer)
    Engine.instance().startHTTPWSGI(gameServer.app, host='0.0.0.0', port=httpAppPort)
    Engine.instance().run()