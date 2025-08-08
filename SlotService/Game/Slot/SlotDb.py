# -*- coding: utf8 -*-
__author__ = 'jhengsian'

import csv, os, ast

class SlotDb:
    slot_machine_field = ['GameName','GameUrl','CodeName','Cost','BatchFeverId','BatchFeverEnable']
    # prob_db_field = ['GameName', 'RTP', 'Prob']

    @staticmethod
    def Initialize(logger, DataSource=None, **kwargs):
        if DataSource is None:
            logger.error("[SlotDb] Initialize failed")
            return None

        write_func_map = {
            "SlotMachine": {'func': SlotDb.write_slot_machine_setting, 'file_path': 'Script/Init/SlotMachine.csv'},
            # "ProbDb": {'func': SlotDb.write_prob_db, 'file_path': 'Script/Init/ProbDb.csv'}
        }

        for collection_name, info in write_func_map.items():
            write_func = info['func']
            file_path = info['file_path']

        # SlotMachine
            coll_setting = DataSource[collection_name]
            SlotDb.upd_data(logger, collection_name, coll_setting, file_path, write_func=write_func)


    @staticmethod
    def upd_data(logger, buffer_name, col, file, write_func=None):
        if write_func is None:
            print('[SlotDb] write_func:{} not found'.format(write_func))
        if type(file) is str and os.path.isfile(file):
            try:
                with open(file, "r", encoding="utf-8-sig") as file:
                    write_func(logger, col, file)
            except Exception as e:
                logger.error("[SlotDb] %s failed", buffer_name, exc_info=True)
        else:
            logger.error('[SlotDb] File:{} not found'.format(os.getcwd() + "/" + file))

    @staticmethod
    def write_slot_machine_setting(logger, col, file):
        reader = csv.DictReader(file)
        for row in reader:
            for key in SlotDb.slot_machine_field:
                if key not in row:
                    logger.warn("[SlotDb] {} not in {}".format(key, row))
                    continue

            (
                game_name, game_url, code_name,
                cost, batch_fever_id, batch_fever_enable,
            ) = (row[key] for key in SlotDb.slot_machine_field)

            if batch_fever_id:
                # batch_fever_ids = [int(i.strip()) for i in batch_fever_id.split(',') if i.strip()]
                batch_fever_ids = SlotDb.parse_csv_list(batch_fever_id)
            else:
                batch_fever_ids = []

            qry = {'GameName': game_name}
            upd = {
                'GameUrl': game_url,
                'CodeName': code_name,
                'Cost': int(cost),
                'BatchFeverId': [i for i in batch_fever_ids if isinstance(i, int)],
                'BatchFeverEnable': batch_fever_enable == "TRUE",
            }
            col.update_one(qry, {'$setOnInsert': upd}, upsert=True)

    # @staticmethod
    # def write_prob_db(logger, col, file):
    #     reader = csv.DictReader(file)
    #     for row in reader:
    #         for key in SlotDb.prob_db_field:
    #             if key not in row:
    #                 logger.warn("[SlotDb] {} not in {}".format(key, row))
    #                 continue
    #
    #         (
    #             game_name, rtp , prob,
    #         ) = (row[key] for key in SlotDb.prob_db_field)
    #
    #         if prob:
    #             # batch_fever_ids = [int(i.strip()) for i in batch_fever_id.split(',') if i.strip()]
    #             prod_list = SlotDb.parse_csv_list(prob)
    #         else:
    #             prod_list = []
    #         qry = {'GameName': game_name, 'RTP': rtp}
    #         upd = {
    #             'Prob': [str(i) for i in prod_list],
    #         }
    #         col.update_one(qry, {'$setOnInsert': upd}, upsert=True)

    @staticmethod
    def parse_csv_list(value):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [int(i) if isinstance(i, int) else i for i in parsed]
        except (ValueError, SyntaxError):
            pass
        return []