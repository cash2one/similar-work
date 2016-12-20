#encoding: utf-8
"""
# @file tokenizer.py
# @Synopsis  tokenize text, using the Baidu wordseg and postag libarary
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""

import os
import sys
import wordseg
import postag


class Tokenizer(object):
    """
    # @Synopsis  tokenize text, only Chinese is configured, meaning other language
    # won't raise exception, but will be tokenized character-wise
    """
    SEG_TYPE_DICT = {
            'WPCOMP': wordseg.SCW_WPCOMP,
            'BASIC': wordseg.SCW_BASIC
            }
    def __init__(self, dict_path):
        print >> sys.stderr, 'WordSegUtil constructed'
        self.MAX_TERM_CNT = 2048
        self.scw_worddict = wordseg.scw_load_worddict(
                os.path.join(dict_path, 'wordseg/chinese_gbk'))
        self.scw_tagdict = postag.tag_create(
                os.path.join(dict_path, 'postag'))
        self.scw_out = wordseg.scw_create_out(self.MAX_TERM_CNT * 10)

        # token
        self.tokens = wordseg.create_tokens(self.MAX_TERM_CNT)
        self.tokens = wordseg.init_tokens(self.tokens, self.MAX_TERM_CNT)

    def __del__(self):
        wordseg.destroy_tokens(self.tokens)
        wordseg.scw_destroy_out(self.scw_out)
        wordseg.scw_destroy_worddict(self.scw_worddict)
        print 'Tokenize destroied'

    def tokenizeString(self, text, encoding='utf8', seg_type='WPCOMP'):
        """
        # @Synopsis  tokenize a given text string, return token and its position of
        # sentence(pos)
        #
        # @Args text string to be tokenized
        # @Args encoding support utf8, gbk and unicode
        # @Args seg_type basic or complex mode
        #
        # @Returns   dict{'errno': error number, 'data': [(token, pos)]}
        """
        ret={
            'errno': 0,
            'data': [],
            }

        if len(text) == 0:
            return ret
        try:
            if encoding == 'utf8':
                text = text.decode('utf8', errors='ignore').encode('gbk')
            elif encoding == 'unicode':
                text = text.encode('gbk')
            data = []
            wordseg.scw_segment_words(self.scw_worddict, self.scw_out, text,
                    len(text), 1)
            token_cnt = wordseg.scw_get_token_1(self.scw_out,
                    self.SEG_TYPE_DICT[seg_type],
                    self.tokens, self.MAX_TERM_CNT)
            tokens = wordseg.tokens_to_list(self.tokens, token_cnt)

            token_cnt = postag.tag_postag(self.scw_tagdict, self.tokens,
                    token_cnt)
            postag_ret = postag.print_tags(self.tokens, token_cnt)

            for token, pos in postag_ret:
                token = token.decode('gbk', 'ignore')
                data.append([token, pos])
            ret['data'] = data
            return ret

        except Exception as e:
            print e.message
            if encoding == 'unicode':
                print text.encode('utf8')
            else:
                print text.decode(encoding).encode('utf8')
            ret['errno'] = 1
            return ret

    def tokenizeAndFilter(self, text, encoding='unicode',
            seg_type='WPCOMP'):
        """
        # @Synopsis  tokenize and filter tokens with unwanted pos(now is 'w',
        # stand for punctuations)
        #
        # @Args text
        # @Args encoding
        # @Args seg_type
        #
        # @Returns  [tokens] if tokenizer works right, [] otherwise
        """
        POS_BLACK_LIST = ['w', 'u']
        ret = self.tokenizeString(text, encoding, seg_type)
        if ret['errno'] != 0:
            return []
        else:
            data = ret['data']
            data_filtered = filter(lambda x: x[1] not in POS_BLACK_LIST, data)
            tokens = map(lambda x: x[0], data_filtered)
            return tokens


if __name__ == "__main__":
    # test_str = 'CCTV5在直播中国队比赛, 赵丽颖出演花千骨, 达芬奇密码'
    test_str = '三国演义，新三国演义'
    tokenizer = Tokenizer('/home/video/guming02/tools/dict')
    r = tokenizer.tokenizeString(test_str, encoding='utf8', seg_type='BASIC')
    # r = tokenizer.tokenizeString(test_str, encoding='utf8')
    for e in r['data']:
        print '%s\t%s' % (e[0].encode('utf8'), e[1])
