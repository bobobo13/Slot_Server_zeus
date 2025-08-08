# -*- coding: utf-8 -*-
__author__ = 'jhengsian'

import traceback
import pymongo
import os
import csv

class BuyBonusSlotConfigDb:
    FILEPATH = "init_data/{}/{}.csv"
    @staticmethod
    def Initialize(strDbName='', strHost='localhost', nPort=27017, strUser='', strPassword='', DataSource=None, bDropDb=False, **kwargs):
        if DataSource is None:
            return None

        write_func_map = {
            "SlotConfig": BuyBonusSlotConfigDb.write_slot_config
        }

        FilePath = kwargs.get("FilePath")
        slot_config_name = kwargs.get("slot_config_name")
        col_setting = '{}SlotConfig'.format(slot_config_name)

        if bDropDb:
            DataSource[col_setting].drop()

        # BonusSlotConfig
        coll_slot_rtp = DataSource[col_setting]
        coll_slot_rtp.create_index([('GameName', pymongo.ASCENDING), ('Version', pymongo.ASCENDING), ('Group', pymongo.ASCENDING)], unique=True)
        BuyBonusSlotConfigDb.upd_data(coll_slot_rtp, FilePath.format(col_setting), write_func=write_func_map['SlotConfig'])

    @staticmethod
    def upd_data(col, file, write_func=None):
        if write_func is None:
            print('[BuyBonusSlotConfigDb] write_func:{} not found'.format(write_func))
        if type(file) is str and os.path.isfile(file):
            try:
                with open(file, "r") as file:
                    write_func(col, file)
            except Exception as e:
                print(traceback.format_exc())
        else:
            print('[BuyBonusSlotConfigDb] File:{} not found'.format(os.getcwd() + "/" + file))

    @staticmethod
    def write_slot_config(col, file):
        reader = csv.reader(file)
        reader.next()
        for row in reader:
            if len(row) <=0:
                continue
            ver,gp,game_name,dec_game_rate = row
            group = None
            if gp != 'default' and gp.isdigit():
                group = int(gp)
            dec_game_rate = float(dec_game_rate)

            qry = {'GameName': game_name, 'Version': ver}
            if group is not None:
                qry.update({'Group': group})

            upd = {}
            upd['DecGameRate'] = dec_game_rate
            col.update(qry, {'$setOnInsert': upd}, upsert=True)


if __name__ == '__main__':
    import logging

    print os.getcwd()
    os.chdir("../../common/")
    print os.getcwd()

    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    format = '%(asctime)s - %(levelname)s -%(name)s : %(message)s'
    formatter = logging.Formatter(format)
    streamhandler = logging.StreamHandler()
    logger.addHandler(streamhandler)

    strConfigPath = os.getcwd()+"/init"

    # file:{Buffer Name}BufferSetting{opt}.csv
    # col: {Buffer Name}BufferSetting
    # dbc = DbConnector.Connect('BuyBonus')
    dbc = None
    opt = ['h5sea', 'macross', 'kiosk']
    msg = 'Channel: '
    for idx, value in enumerate(opt):
        msg += "[{}]{} ".format(idx, value)
    ans = raw_input(msg)
    channel = opt[int(ans)]
    # slot_config_name = raw_input('Slot Config Name:')
    slot_config_name = 'Bonus'
    bDropDb = raw_input('Drop Collection[Y/N]?')
    bDropDb = bDropDb in ["Y", 'y']
    BuyBonusSlotConfigDb.Initialize(DataSource=dbc, Channel=channel, slot_config_name=slot_config_name)
