#encoding=utf-8
"""
# @file category_process.py
# @Synopsis  process category string and gene string, applying some
# transformation and/or filter
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2016-03-10
"""

import sys
sys.path.append('../..')
from conf.env_config import EnvConfig

class CategoryProcess(object):
    """
    # @Synopsis  process category and gene string with some filter/rerank/replace
    # operations
    """

    @staticmethod
    def filter_(category_str, works_type):
        """
        # @Synopsis  filter useless categories/genes
        #
        # @Args category_str
        # @Args works_type
        #
        # @Returns   filtered category/gene string
        """
        categories = category_str.split('$$')
        BLACK_LIST = dict()
        BLACK_LIST['comic'] = [u'动画', u'动漫', u'动漫电视', u'电视',
                u'新番动画', u'其他', u'完结动画']
        BLACK_LIST['show'] = [u'综艺', u'男生聚会', u'女生聚会', u'内地']
        BLACK_LIST['tv'] = [u'剧情']
        BLACK_LIST['movie'] = []
        categories = filter(lambda x: x not in BLACK_LIST[works_type], categories)
        return '$$'.join(categories)

    @staticmethod
    def rerank(category_str):
        """
        # @Synopsis  rerank category by importance
        # @Args category_str
        # @Returns   reranked category string
        """
        categories = category_str.split('$$')
        top_categories = [u'动画', u'古装']
        tail_category = [u'剧情']
        key_func = lambda x: 0 - 1 * (x in top_categories) + 1 * (x in tail_category)
        categories.sort(key=key_func)
        return '$$'.join(categories)

    @staticmethod
    def synonymReplace(category_str):
        """
        # @Synopsis  replace synonym, so that they would be consider to be same
        # when calcalating similarity
        #
        # @Args category_str
        #
        # @Returns   processed category string
        """
        SYNONYM_DICT = {
                u'仙侠': u'奇幻',
                u'魔幻': u'奇幻',
                u'玄幻': u'奇幻',
                u'爆笑': u'喜剧',
                u'幽默': u'喜剧',
                u'搞笑': u'喜剧',
                u'游戏互动': u'游戏',
                u'古代': u'古装',
                u'全家': u'家庭',
                }
        for origin_word, synonym in SYNONYM_DICT.iteritems():
            category_str = category_str.replace(origin_word, synonym)
        return category_str

