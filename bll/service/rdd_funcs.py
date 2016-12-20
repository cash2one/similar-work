"""
# @file rdd_action_funcs.py
# @Synopsis  rdd functions
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-14
"""

import sys
sys.path.append('../..')
import os
from conf.env_config import EnvConfig
import math
import time
from datetime import datetime
import re
from urllib import unquote

class RddFuncs(object):
    """
    # @Synopsis  rdd operation functions
    """

    @staticmethod
    def parsePlayLog(line):
        """
        # @Synopsis  parse long video play log line
        #
        # @Args line
        #
        # @Returns   (uid, vid, playType), where uid and vid are string, playType
        # is one of ['search', 'browse', ...]
        """
        fields = line.strip().split('\t')
        if len(fields) == 5:
            try:
                uid = fields[0]
                sub_fields = fields[4].split(';')
                vid = None
                play_type = None
                for sub_field in sub_fields:
                    k, v = sub_field.split(':')
                    if k == 'id':
                        vid = v
                    elif k == 'playType':
                        play_type = v
                if vid is not None and play_type is not None:
                    if len(vid) > 0 and len(vid) <= 10:
                        return (uid, vid, play_type)
            except Exception as e:
                pass

    @staticmethod
    def parseShortPlayLog(line):
        """
        # @Synopsis  pass short video play log line
        #
        # @Args line
        #
        # @Returns   (uid, vid, playType)
        """
        fields = line.strip().split('\t')
        if len(fields) == 5:
            uid = fields[0]
            sub_fields = fields[4].split(';')
            if len(sub_fields) == 2:
                try:
                    playType = sub_fields[0].split(':')[1]
                    url = unquote(sub_fields[1].split(':')[1])
                    return (uid, url, playType)
                except Exception as e:
                    pass

    @staticmethod
    def parseSearchLog(line, platform='Mobile'):
        """
        # @Synopsis  parse search log
        #
        # @Args line   A line in log file
        # @Args platform   Mobile or PC
        #
        # @Returns   [uid, query]
        """
        # 0C651E4AD40172B36CB3BE2D9DFA0583|952935720980668 \t
        # %E5%A4%9A%E6%83%85%E6%B1%9F%E5%B1%B1
        fields = line.strip().split('\t')
        if len(fields) == 2:
            uid = fields[0]
            query = fields[1]
            return [uid, query]

    @staticmethod
    def parseSimWorkClickLog(line):
        """
        # @Synopsis  parse similar work click log
        # @Args line
        # @Returns   parsed dict
        """
        # uid \t works_type:src_workid \t click_type \t works_type:dst_worksid
        # \t index \t timestamp
        fields = line.strip('\n').split('\t')
        if len(fields) == 6:
            uid = fields[0]
            src = fields[1]
            click_type = fields[2]
            if click_type == '\\N':
                click_type = ''
            dst = fields[3]
            index = fields[4]
            if index == '\\N':
                index = ''
            timestamp = fields[5]
            try:
                works_type, src_id = src.split(':')
                dst_id = dst.split(':')[1]
            except Exception as e:
                return None
            click_record = dict({
                'uid': uid,
                'works_type': works_type,
                'click_type': click_type,
                'src_id': src_id,
                'dst_id': dst_id,
                'index': index,
                'timestamp': timestamp
                })
            return click_record



if __name__ == '__main__':
    group_size = 2
    user_dict1 = dict({'a': 1, 'b': 1})
    user_dict2 = dict({'a': 1, 'c': 1})
    result_dict = rdd_action_funcs.user_dict_nomalized_sum(user_dict1, user_dict2)
    print result_dict
    print rdd_action_funcs.norm_cal(result_dict)/group_size
