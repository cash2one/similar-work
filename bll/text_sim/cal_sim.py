#encoding=utf-8
"""
# @file cal_sim.py
# @Synopsis  calculate text similarity of two videos, including title,
# introduction and type
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""

from scipy.sparse import csr_matrix
import numpy as np
import os
import logging
from datetime import datetime
import time
import math
from sklearn.externals import joblib
from conf.env_config import EnvConfig
from bll.data.data_source_conf import long_videos_dict
from bll.service.util import Util
from bll.text_sim.feature_extraction import FeatureExtraction
from bll.text_sim.category_process import CategoryProcess
from bll.text_sim.site_decay import SiteDecay

class CalTextSim(object):
    """
    # @Synopsis  calculate text similarity between videos
    """
    @staticmethod
    def calSim(work_type):
        """
        # @Synopsis  calculate text similarity, including title, director, actor,
        # area, category. Similarity is decayed if video is old. Similarity list
        # doesn't include any non-free video, however, non-free video do recall a
        # similar work list
        #
        # @Returns  nothing
        """
        logger = logging.getLogger(EnvConfig.LOG_NAME)
        videos = long_videos_dict[work_type].load()
        directors_list = map(lambda x: x['directors'], videos)
        actors_list = map(lambda x: x['actors'], videos)

        def trim_mapper(actors_str):
            """
            # @Synopsis  leave only the first 4 actor
            # @Args actors_str
            # @Returns   
            """
            try:
                actors = actors_str.split('$$')
                return '$$'.join(actors[:4])
            except Exception as e:
                return actors_str
        actors_list = map(trim_mapper, actors_list)

        if work_type == 'comic':
            author_list = map(lambda x: x['author'], videos)
        categories_list = map(lambda x: x['categories'], videos)
        categories_list = map(CategoryProcess.synonymReplace, categories_list)
        categories_list = map(CategoryProcess.rerank, categories_list)
        filter_func = lambda x: CategoryProcess.filter_(x, work_type)
        categories_list = map(filter_func, categories_list)

        available_list = map(lambda x: x['available'], videos)

        data_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, work_type, 'intermediate')

        category_sim_matrix = CalTextSim.calSingleSimMatrix(categories_list, use_idf=False,
                decay_ratio=1.3)
        joblib.dump(category_sim_matrix, os.path.join(data_path, 'category_sim_matrix'))

        # category_sim_filter_matrix = CalTextSim.transform2NoneZeroFilter(category_sim_matrix)
        # joblib.dump(category_sim_matrix, os.path.join(data_path, 'category_sim_filter_matrix'))

        director_sim_matrix = CalTextSim.calSingleSimMatrix(directors_list,
                use_idf=False, decay_ratio=1)
        # director_sim_filter_matrix = CalTextSim.transform2NoneZeroFilter(director_sim_matrix)
        joblib.dump(director_sim_matrix, os.path.join(data_path, 'director_sim_matrix'))

        actor_sim_matrix = CalTextSim.calSingleSimMatrix(actors_list, use_idf=False,
                decay_ratio=1.1)
        # actor_sim_filter_matrix = CalTextSim.transform2NoneZeroFilter(actor_sim_matrix)
        joblib.dump(actor_sim_matrix, os.path.join(data_path, 'actor_sim_matrix'))

        free_filter_matrix = Util.getFilterMatrix(available_list)
        joblib.dump(free_filter_matrix, os.path.join(data_path, 'free_filter_matrix'))

        logger.debug('category_sim_matrix.shape={0}'.format(category_sim_matrix.shape))
        logger.debug('category_sim_matrix, max: {0}, min: {1}, mean: {2}'.format(
            category_sim_matrix.max(), category_sim_matrix.min(),
            category_sim_matrix.mean()))

        logger.debug('director_sim_matrix.shape={0}'.format(director_sim_matrix.shape))
        logger.debug('director_sim_matrix, max: {0}, min: {1}, mean: {2}'.format(
            director_sim_matrix.max(), director_sim_matrix.min(),
            director_sim_matrix.mean()))

        logger.debug('actor_sim_matrix.shape={0}'.format(actor_sim_matrix.shape))
        logger.debug('actor_sim_matrix, max: {0}, min: {1}, mean: {2}'.format(
            actor_sim_matrix.max(), actor_sim_matrix.min(),
            actor_sim_matrix.mean()))
        return True

    @staticmethod
    def transform2NoneZeroFilter(sim_matrix):
        """
        # @Synopsis  change all non zero element in a matrix to 1
        # @Args sim_matrix
        # @Returns   
        """
        matrix_shape = sim_matrix.shape
        nonzero_ind = sim_matrix.nonzero()
        nonzero_cnt = nonzero_ind[0].size
        data = [1] * nonzero_cnt
        filter_matrix = csr_matrix((data, nonzero_ind), shape=matrix_shape)
        return filter_matrix


    @staticmethod
    def calSingleSimMatrix(feature_list, use_idf=False, decay_ratio=1):
        """
        # @Synopsis  cal similarity on a single feature dimension
        #
        # @Args feature_list
        #
        # @Returns  simlarity matrix, csr_matrix
        """
        feature_extractor = FeatureExtraction()
        feature_matrix = feature_extractor.fit_transform(feature_list, use_idf,
                decay_ratio)
        sim_matrix = feature_matrix * feature_matrix.T
        return sim_matrix


if __name__ == '__main__':
    start_time = datetime.now()
    CalTextSim.calSim('movie')
    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    print 'spend {0} minutes'.format(minutes)
