import simplejson as json
import base64
from gevent import monkey
import falcon
import traceback
import requests
monkey.patch_all()

from SlotServer.Network import Engine
from SlotServer.Network.Web import WebAPI
from SlotServer.Network.BaseServer import BaseServer
from SlotServer.Network.Web import Controller

def Command(req, resp, kwargs):
    body = req.stream.read()
    cmd_id, cmd_name, cmd_sn = get_cmd(body)
    if cmd_id is None or cmd_name is None:
        resp.status = falcon.HTTP_500
        resp.body = None
        raise Exception("WebAPI:on_get Exception : " + str(traceback.format_exc()))
    url = CMD_MAP.get(cmd_name)

    if url is None:
        resp.status = falcon.HTTP_500
        resp.body = None
        raise Exception("WebAPI:on_get Exception : " + str(traceback.format_exc()))
    result = requests.post(url=url ,json=get_data(body))
    gameResp = result.json()
    cmdResp = {
        'cmd_sn': cmd_sn,
        'cmd_data': gameResp
    }
    cmdResp = json.dumps(cmdResp).encode('utf8')
    cmdResp = base64.standard_b64encode(cmdResp)
    resp.text = cmdResp

def addRoute(route, controller, service):
    web_api = WebAPI(controller)
    url = route
    service.addWebAPI(url, web_api)

def get_data(body):
    data = base64.standard_b64decode(body)
    form = json.loads(data, encoding='utf-8')
    data = form['ark_data']
    data = base64.standard_b64decode(data)
    data = json.loads(data, encoding='utf-8')
    cmd_data = data['cmd_data']
    data['game_name'] = cmd_data.pop('GameName', None)
    data['platform_data'] = {
        "GameRatio": "1"
    }
    data['game_data'] = {
        "bet_value": cmd_data.pop('BetValue', None),
        "bet_lines": cmd_data.pop('BetLines', None),
        "extra_bet": cmd_data.pop('ExtraBet', None),
        "dev_mode": cmd_data.pop('dev_mode', 0),
    }
    data['fs_setting'] = {
        "EnableTestMode": True,
    }
    return data

def get_cmd(body):
    data = base64.standard_b64decode(body)
    form = json.loads(data, encoding='utf-8')
    data = form['ark_data']
    data = base64.standard_b64decode(data)
    data = json.loads(data, encoding='utf-8')
    cmd_id = data['cmd_id']
    cmd_name = data['cmd_name']
    cmd_sn = data['cmd_sn']
    return cmd_id, cmd_name, cmd_sn

lobbyServer = BaseServer("SlotLobby", 'local')
lobbyServer.addController = Controller("SlotLobby", lobbyServer)

addRoute('/command', Command, lobbyServer)
CMD_MAP = {
    'START_GAME': 'http://127.0.0.1:8082/SlotMachine/start_game',
    'SPIN': 'http://127.0.0.1:8082/SlotMachine/spin'
}

Engine.instance().start(lobbyServer)
httpApp = lobbyServer.app

if __name__ == '__main__':
    Engine.instance().startHTTPWSGI(httpApp, host='0.0.0.0', port=8081)
    Engine.instance().run()