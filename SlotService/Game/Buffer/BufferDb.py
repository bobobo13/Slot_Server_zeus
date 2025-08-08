# -*- coding: utf8 -*-
__author__ = 'jhengsian'

import pymongo
import csv, os

class BufferDb:
    field = ['Version', 'Group', 'GameName', 'Enable', 'MaxWin', 'BufferGateValue1', 'BufferGate1', 'NoWinGate1', 'BufferGateValue2', 'BufferGate2', 'NoWinGate2', 'BufferGateValue3', 'BufferGate3', 'NoWinGate3']
    @staticmethod
    def Initialize(logger, strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None, bDropDb=False, **kwargs):
        if DataSource is None:
            logger.error("[BufferDb] Initialize failed")
            return None

        write_func_map = {
            "BufferSetting": BufferDb.write_buffer_setting,
        }

        file_path = kwargs.get("FilePath")
        buffer_name = kwargs.get("buffer_name")
        col_setting = '{}BufferSetting'.format(buffer_name)
        col_value = '{}BufferValue'.format(buffer_name)

        if bDropDb:
            DataSource[col_setting].drop()

        # BufferSetting
        coll_buffer_setting = DataSource[col_setting]
        coll_buffer_setting.create_index([('GameName', pymongo.ASCENDING), ('Version', pymongo.ASCENDING), ('Group', pymongo.ASCENDING)], unique=True)
        BufferDb.upd_data(logger, buffer_name, coll_buffer_setting, file_path, write_func=write_func_map['BufferSetting'])

        # BufferValue
        coll_buy_bonus_buffer_value = DataSource[col_value]
        coll_buy_bonus_buffer_value.create_index([('GameName', pymongo.ASCENDING), ('Version', pymongo.ASCENDING), ('Group', pymongo.ASCENDING)], unique=True)

    @staticmethod
    def upd_data(logger, buffer_name, col, file, write_func=None):
        if write_func is None:
            print('[SlotBufferDb] write_func:{} not found'.format(write_func))
        if type(file) is str and os.path.isfile(file):
            try:
                with open(file, "r", encoding="utf-8-sig") as file:
                    write_func(logger, col, file)
            except Exception as e:
                logger.error("[SlotBufferDb] %s failed", buffer_name, exc_info=True)
        else:
            logger.error('[SlotBufferDb] File:{} not found'.format(os.getcwd() + "/" + file))

    @staticmethod
    def write_buffer_setting(logger, col, file):
        reader = csv.DictReader(file)
        for row in reader:
            for key in BufferDb.field:
                if key not in row:
                    logger.warn("[BufferDb] {} not in {}".format(key, row))
                    continue

            (
                ver, gp, game_name, enable, max_win,
                buffer_gate_value_lv1, buffer_gate_lv1, no_win_gate_lv1,
                buffer_gate_value_lv2, buffer_gate_lv2, no_win_gate_lv2,
                buffer_gate_value_lv3, buffer_gate_lv3, no_win_gate_lv3
            ) = (row[key] for key in BufferDb.field)

            group = None
            if gp != 'default' and gp.isdigit():
                group = int(gp)

            enable = (enable == "TRUE")
            max_win = float(max_win)
            buffer_gate_value_lv1 = float(buffer_gate_value_lv1)
            buffer_gate_lv1 = float(buffer_gate_lv1)
            no_win_gate_lv1 = float(no_win_gate_lv1)
            buffer_gate_value_lv2 = float(buffer_gate_value_lv2)
            buffer_gate_lv2 = float(buffer_gate_lv2)
            no_win_gate_lv2 = float(no_win_gate_lv2)
            buffer_gate_value_lv3 = float(buffer_gate_value_lv3)
            buffer_gate_lv3 = float(buffer_gate_lv3)
            no_win_gate_lv3 = float(no_win_gate_lv3)

            qry = {'GameName': game_name, 'Version': ver}
            if group is not None:
                qry.update({'Group': group})

            upd = {}
            upd['Enable'] = enable
            upd['MaxWin'] = max_win
            upd['CtrlLevel'] = [{
                    "BufferGateValue": buffer_gate_value_lv1,
                    "BufferGate": buffer_gate_lv1,
                    "NoWinGate": no_win_gate_lv1
                },
                {
                    "BufferGateValue": buffer_gate_value_lv2,
                    "BufferGate": buffer_gate_lv2,
                    "NoWinGate": no_win_gate_lv2
                },
                {
                    "BufferGateValue": buffer_gate_value_lv3,
                    "BufferGate": buffer_gate_lv3,
                    "NoWinGate": no_win_gate_lv3
                }
            ]
            col.update_one(qry, {'$setOnInsert': upd}, upsert=True)