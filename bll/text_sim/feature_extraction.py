#encoding=utf-8
"""
# @file feature_extraction.py
# @Synopsis  extract feature from category, actor, director, area, these
# features are stored in form 'feature1$$feature2', and the weight of the
# feature decreases with its position in the list
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-21
"""
from scipy.sparse import csr_matrix
from sklearn.preprocessing import Normalizer
import numpy as np
from conf.env_config import EnvConfig
import math

class FeatureExtraction(object):
    """
    # @Synopsis  extract feature from string, with format
    # 'feature1$$feature2$$..', and the importance decreases by the postion
    """
    def __init__(self):
        self.vocabulary_ = dict()
        self.idf_dict_ = dict()

    def fit(self, feature_list):
        """
        # @Synopsis  fit the given feature list, generate vocabulary
        #
        # @Args feature_list
        #
        # @Returns   nothing
        """
        document_cnt = len(feature_list)
        df_dict = dict()
        vocabulary_set = set()
        for feature_str in feature_list:
            features = feature_str.split('$$')
            features = filter(lambda x: x.strip() != '', features)
            vocabulary_set = vocabulary_set.union(set(features))
            for feature in set(features):
                df_dict[feature] = df_dict.get(feature, 0) + 1

        self.idf_dict_ = dict((k, math.log(document_cnt / v)) \
                for k, v in df_dict.iteritems())

        self.vocabulary_ = dict((v, k) for k, v in enumerate(vocabulary_set))

    def transform(self, feature_list, use_idf=True, decay_ratio=1):
        """
        # @Synopsis  transform the feature list to csr_matrix
        #
        # @Args feature_list
        #
        # @Returns   feature matrix
        """
        # this is a impirical value, can be altered. Even better to make this
        # variable a input parameter of the function, to be done.
        row_dim = len(feature_list)
        col_dim = len(self.vocabulary_)
        row_ind = []
        col_ind = []
        data = []
        emphasis_features = [u'动画', u'古装']
        for row_id, feature_str in enumerate(feature_list):
            features = feature_str.split('$$')
            deduplicated_features = []
            for feature in features:
                if feature not in deduplicated_features:
                    deduplicated_features.append(feature)
            for rank, feature in enumerate(deduplicated_features):
                weight = decay_ratio ** (-rank)
                if feature in emphasis_features:
                    weight *= 2
                if feature in self.vocabulary_:
                    row_ind.append(row_id)
                    col_ind.append(self.vocabulary_[feature])
                    if use_idf:
                        weight *= self.idf_dict_[feature]
                    data.append(weight)

        feature_matrix = csr_matrix((data, (row_ind, col_ind)),
                shape=(row_dim, col_dim))
        normalizer = Normalizer(copy=False)
        normalizer.transform(feature_matrix)
        return feature_matrix

    def fit_transform(self, feature_list, use_idf=True, decay_ratio=1):
        """
        # @Synopsis  fit and transform the given feature list, equal to fit then
        # transform
        #
        # @Args feature_list
        #
        # @Returns   feature matrix
        """
        self.fit(feature_list)
        return self.transform(feature_list, use_idf, decay_ratio)

if __name__ == '__main__':
    feature_list = ['a$$b', 'c$$a']
    feature_extractor = FeatureExtraction()
    feature_matrix = feature_extractor.fit_transform(feature_list)
    print feature_matrix.toarray()
