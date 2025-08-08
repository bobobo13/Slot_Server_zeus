# -*- coding: utf-8 -*-
import json
import re
import openpyxl


class ExcelToJsonConfig:
    def __init__(self, path):
        self.path = path
        self.workbook = None
        self.sheet = None

    def open(self, sheet_name):
        self.workbook = openpyxl.load_workbook(self.path)
        self.sheet = self.workbook[sheet_name]

    def get_json_config(self, sheet_name, output, topic_partition=2, indent=4):
        self.open(sheet_name)
        rows = list(self.sheet.rows)
        json_obj = dict()
        for row in rows[1:]:
            if row[0].value is None:
                break
            obj = self.get_obj(row, topic_partition)
            json_obj.update(obj)
        self.save_json_file(json_obj, output, indent)
        self.close()

    @staticmethod
    def save_json_file(obj, output, indent):
        with open(output, 'w') as f:
            json_str = json.dumps(obj, sort_keys=True, indent=indent)
            json_str = re.sub(r'[0-9][0-9]_', '', json_str)
            f.write(json_str)
            f.close()

    @staticmethod
    def get_obj(row, topic_partition=2):
        index = int(row[0].value)
        log_name = row[1].value
        mongo = True if row[3].value == 'Y' else False
        splunk = True if row[4].value == 'Y' else False
        big_query = True if row[5].value == 'Y' else False
        elk = True if row[6].value == 'Y' else False
        # msg = str.format('Index: {}, LogName: {}, Mongo: {}, Splunk: {}, BigQuery: {}, ELK: {}',
        #                  index, log_name, mongo, splunk, big_query, elk)
        obj = {
            str.format('{:02d}_{}', index, log_name): {
                "01_Id": index,
                "02_KafkaTopicPartition": topic_partition,
                "03_Mongo": mongo,
                "04_Splunk": splunk,
                "05_ELK": elk,
                "06_BigQuery": big_query
            }
        }
        return obj

    def close(self):
        if self.workbook is not None:
            self.workbook.close()
            self.workbook = None
            self.sheet = None


if __name__ == '__main__':
    excel_path = '/Users/weichiehchen/Downloads/DataCollection_Schema_Standard_20220215.xlsx'
    sheet = u'總表'
    output_path = '/Users/weichiehchen/Documents/SVN/ArkEngine_3_2_3/branches/ArkDataTransModule/yak/Game/config/dev'
    output_filename = 'trans.json'
    json_output = str.format('{}/{}', output_path, output_filename)
    excel_to_json_config = ExcelToJsonConfig(excel_path)
    excel_to_json_config.get_json_config(sheet, json_output, topic_partition=1, indent=4)
