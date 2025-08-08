#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random

class SlotMath(object):

    @staticmethod
    def gamble(odds, scale=1, count=1, game_rate=0.98):
        base = 10000000 * scale
        chance_gate = base * game_rate * scale
        random_base = base * odds * count
        random_base = int(round(random_base, 0))
        chance_gate = int(round(chance_gate, 0))
        if SlotMath.randrange(0,random_base) < chance_gate:
            return True
        return False

    @staticmethod
    def guess(win_rate): # win_rate: floating point (Ex. 0.1 => 10% win)
        gate = 10000000 * win_rate
        #print "guess", gate
        if SlotMath.randrange(0,10000000) < gate:
            return True
        return False

    @staticmethod
    def guess_new(win_rate):
        if random.random() < win_rate:
            return True
        else:
            return False

    @staticmethod
    def randrange(begin, end): #   begin <= X < end
        return random.randrange(begin,end)

    @staticmethod
    def choice(list=[0]):
        return random.choice(list)

    @staticmethod
    def get_weights_index(weights=[100]):
        total_value = SlotMath.get_total_weights(weights)
        number = random.randrange(0, total_value)
        index = 0
        weight_value = 0
        for value in weights:
            weight_value = weight_value + value
            if (weight_value > number):
                return index
            else:
                index = index + 1
        return -1

    @staticmethod
    def get_total_weights(weights=[100]):
        total_value = 0
        for value in weights:
            total_value = total_value + value
        return total_value

    @staticmethod
    def get_count_index(counts=[100]):
        index = SlotMath.get_weights_index(counts)
        counts[index] = counts[index] - 1
        return index

    # From MatchGameMath
    @staticmethod
    def get_result_by_gate(gate):
        return gate[0] >= random.randint(1, gate[1])

    @staticmethod
    def get_result_by_gate_extra(sussess, total):
        return sussess >= random.randint(1, total)

    @staticmethod
    def get_result_by_weight(award_list, weight_list):
        if len(award_list) <= 0:
            raise("get_result_by_weight, len(award_list) = 0")
        if len(award_list) == 1:
            if len(weight_list) <= 0 or weight_list[0] <= 0:
                raise ("get_result_by_weight, weight_list={}".format(weight_list))
            return 0, award_list[0]
        award = None
        award_index = -1
        total_weight = sum(weight_list)
        rand_num = random.randint(1, total_weight)
        for index, weight in enumerate(weight_list):
            rand_num -= weight
            if rand_num <= 0:
                award = award_list[index]
                award_index = index
                break
        return award_index, award