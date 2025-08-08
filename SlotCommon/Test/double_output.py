# -*- coding: utf-8 -*-

import logging


class FakeLogger(object):
    def log(self, level, s):
        if level == logging.DEBUG:
            self.debug(s)
        elif level == logging.INFO:
            self.info(s)
        elif level == logging.WARNING:
            self.warning(s)
        elif level == logging.ERROR:
            self.error(s)

    def warning(self, str):
        pass
    def error(self, str):
        pass
    def debug(self, str):
        pass
    def info(self, str):
        pass

class FakeSplunkSender():

    def __init__(self, logger):
        self.logger = logger
        self.protect_times = {
            0: 0,  # MainGame
            1: 0,  # FreeGame
        }
        self.protect_wins = {
            0: 0,  # MainGame
            1: 0,  # FreeGame
        }
        self.result_wins = {
            0: 0,  # MainGame
            1: 0,  # FreeGame
        }



class DoubleOutput(FakeLogger):
    def __init__(self, std_file_name, err_file_name, log_level=1):
        self.std_file_name = std_file_name
        self.err_file_name = err_file_name
        self.log_level = log_level
        self.print_or_write = ["p", "w"]
        self._init_summary()

    def _init_summary(self):
        self.main_hit_limit = 0
        self.fever_hit_limit = 0
        self.make_up_times = 0
        self.make_up_wins = 0.0


    # def make_up(self, new_win):
    #     self.make_up_times += 1
    #     self.make_up_wins += new_win

    def out(self, out_str, err=False):
        out_str = str(out_str)
        if "p" in self.print_or_write:
            print(out_str)
        if "w" in self.print_or_write:
            if err:
                with open(self.err_file_name, "a+") as f:
                    f.write(out_str + "\n")
            else:
                with open(self.std_file_name, "a+") as f:
                    f.write(out_str + "\n")

    def debug(self, out_str):
        # self.protection_parse(out_str)
        out_str = str(out_str)
        if out_str.find("new_win") >= 0:
            self.make_up_times += 1
            self.make_up_wins += float(out_str.split("=")[-1])
        if self.log_level < 1:
            out_str = '[DEBUG] ' + out_str
            self.out(out_str)

    def info(self, out_str):
        out_str = str(out_str)
        if self.log_level < 2:
            out_str = '[INFO] ' + out_str
            self.out(out_str)

    def warning(self, out_str):
        out_str = str(out_str)

        # self.protection_parse(out_str)
        if self.log_level < 3:
            out_str = '[WARN] ' + out_str
            self.out(out_str)

    def error(self, out_str):
        out_str = str(out_str)
        out_str = '[ERROR] ' + out_str
        self.out(out_str)

    def protect_count_print_and_clean(self, total_rounds, total_bet):
        self.out("Protect: main:  reached {} times".format(self.main_hit_limit))
        self.out("Protect: fever: reached {} times".format(self.fever_hit_limit))
        self.out("Make up: freq={}, wins={}, rtp={}".format(float(self.make_up_times)/float(total_rounds), self.make_up_wins, float(self.make_up_wins)/float(total_bet)*100))
        self._init_summary()