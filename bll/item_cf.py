"""
# @file item_cf.py
# @Synopsis  calculate itemcf-iuf similarity of movies and get video uv rank
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""

import sys
import os
import logging
import pyspark as ps
import pickle
import math
from scipy.sparse import csr_matrix
from sklearn.externals import joblib
from conf.env_config import EnvConfig
from conf.init_logger import initLogger
from bll.service.util import Util
from bll.service.rdd_funcs import RddFuncs
from bll.data.video_info_util import VideoInfoUtil

def main():
    """
    # @Synopsis  calculate itemcf-iuf similarity
    #
    # @Returns   nothing
    """
    initLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    sc = ps.SparkContext()
    works_type = 'tv'

    play_log = Util.genLogList(7, 1, works_type=works_type)
    # play_log = Util.getOneHourSampleLog(works_type)

    play_log_rdd = sc.textFile(play_log)\
            .map(RddFuncs.parsePlayLog)\
            .filter(lambda x: x is not None)\
            .filter(lambda x: x[2] == 'search')\
            .map(lambda x: (x[0], x[1]))\
            .distinct()\
            .cache()
    Util.debugRdd(play_log_rdd, 'play_log_rdd', logger)

    # (vid, uv)
    item_uv_rdd = play_log_rdd \
            .map(lambda x: (x[1], 1)) \
            .reduceByKey(lambda a, b: a + b)
    Util.debugRdd(item_uv_rdd, 'item_uv_rdd', logger)

    # (uid, uv)
    user_uv_rdd = play_log_rdd \
            .map(lambda x: (x[0], 1)) \
            .reduceByKey(lambda a, b: a + b)
    Util.debugRdd(user_uv_rdd, 'user_uv_rdd', logger)

    # 'coplay' means the two movies were watched by the same audience in the
    # time window
    #(uid, (vid1, vid2)) where vid1<vid2
    user_coplay_rdd = play_log_rdd.join(play_log_rdd) \
            .filter(lambda x: x[1][0] < x[1][1])
    Util.debugRdd(user_coplay_rdd, 'user_coplay_rdd', logger)


    #(uid, ((vid1, vid2), user_uv)) => ((vid1, vid2), contribute_sum)
    user_contribute_rdd = user_coplay_rdd.join(user_uv_rdd) \
            .map(lambda x: (x[1][0], 1.0 / math.log(1 + x[1][1]))) \
            .reduceByKey(lambda a, b: a + b)
    Util.debugRdd(user_contribute_rdd, 'user_contribute_rdd', logger)

    #((vid1, vid2), contribute_sum) => (vid1, (vid2, contribute_sum)) =>
    #(vid1, (((vid2, contribute_sum), uv1))) =>
    #(vid2, (vid1, contribute_sum, uv1)) =>
    #(vid2, ((vid1, contribute_sum, uv1), uv2)) =>
    #((vid1,vid2), contribute_sum, (uv1, uv2)) =>
    #((vid1,vid2), cos_similarity)
    cos_sim_rdd = user_contribute_rdd \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .join(item_uv_rdd) \
            .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))) \
            .join(item_uv_rdd) \
            .map(lambda x: ((x[1][0][0], x[0]), x[1][0][1], (x[1][0][2], x[1][1]))) \
            .map(lambda x: (x[0], x[1]/math.sqrt((x[2][0]) * (x[2][1]))))
    Util.debugRdd(cos_sim_rdd, 'cos_sim_rdd', logger)

    video_rowid_dict = VideoInfoUtil.getVideoRowIdDict(works_type)
    dim = len(video_rowid_dict)
    cos_sim_list = cos_sim_rdd.collect()
    cos_sim_list = filter(lambda x: x[0][0] in video_rowid_dict, cos_sim_list)
    cos_sim_list = filter(lambda x: x[0][1] in video_rowid_dict, cos_sim_list)
    data = map(lambda x: x[1], cos_sim_list)
    row = map(lambda x: video_rowid_dict[x[0][0]], cos_sim_list)
    col = map(lambda x: video_rowid_dict[x[0][1]], cos_sim_list)
    cos_sim_matrix = csr_matrix((data, (row, col)), shape=(dim, dim))
    output_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'intermediate', 'item_cf.pkl')
    joblib.dump(cos_sim_matrix, output_path)
    logger.debug('user_behavior_sim_matrix.shape={0}'.format(cos_sim_matrix.shape))
    logger.debug('user_behavior_sim_matrix, max: {0}, min: {1}, mean: {2}'\
            .format(cos_sim_matrix.max(), cos_sim_matrix.min(),
                cos_sim_matrix.mean()))

    # vids sorted by uv desc
    sorted_vids = item_uv_rdd\
            .repartition(1)\
            .sortBy(lambda x: x[1], ascending=False)\
            .map(lambda x: x[0])\
            .collect()
    logger.debug('len(sorted_vids) is {0}'.format(len(sorted_vids)))
    output_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'result', 'uv_rank.txt')
    output_obj = open(output_path, 'w')
    with output_obj:
        output_obj.write('\n'.join(sorted_vids))



if __name__ == '__main__':
    main()

