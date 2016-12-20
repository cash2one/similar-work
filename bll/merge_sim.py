"""
# @file merge_sim.py
# @Synopsis  merge different types of similarity
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""
import os
from datetime import datetime
import heapq
from bll.data.video_info_util import VideoInfoUtil
from sklearn.externals import joblib
from conf.env_config import EnvConfig
from bll.deduplication import Deduplication

class MergeSim(object):
    """
    # @Synopsis  merge text similarity and user behaivior similarity
    """
    @staticmethod
    def mergeSim(works_type, max_sim_cnt):
        """
        # @Synopsis  merge text similarity and user behaivor similarity, generate
        # final similar work recommendation list
        #
        # @Args max_sim_cnt
        #
        # @Returns  succeeded
        """
        free_filter_matrix = joblib.load(os.path.join(
            EnvConfig.LOCAL_DATA_PATH, works_type, 'intermediate', 'free_filter_matrix'))
        deduplicator = Deduplication(works_type)
        deduplicate_filter = deduplicator.getDeduplicateMatrix()
        item_cf_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'intermediate',
                'item_cf.pkl')
        half_item_cf_matrix = joblib.load(item_cf_path)
        item_cf_matrix = half_item_cf_matrix + half_item_cf_matrix.T

        intermeidate_data_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'intermediate')
        for sim_type in ['category', 'director', 'actor']:
            text_sim_filter_path = os.path.join(intermeidate_data_path,
                    '{0}_sim_matrix'.format(sim_type))
            text_sim_filter_matrix = joblib.load(text_sim_filter_path)
            sim_matrix = item_cf_matrix.multiply(text_sim_filter_matrix) + \
                    text_sim_filter_matrix * 1e-7 + item_cf_matrix * 1e-8

            sim_matrix = sim_matrix.dot(free_filter_matrix)
            sim_matrix = sim_matrix.dot(deduplicate_filter)

            dim = sim_matrix.shape[0]
            video_rowid_dict = VideoInfoUtil.getVideoRowIdDict(works_type)
            rowid_video_dict = dict((v, k) for k, v in video_rowid_dict.items())
            output_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'result',
                    '{0}_sim_result'.format(sim_type))
            output_obj = open(output_path, 'w')
            indptr = sim_matrix.indptr
            data = sim_matrix.data
            indices = sim_matrix.indices
            for rowid in xrange(0, dim):
                origin_worksid = rowid_video_dict[rowid]
                col_ids = indices[indptr[rowid]: indptr[rowid + 1]]
                values = data[indptr[rowid]: indptr[rowid + 1]]
                works_ids = map(lambda x: rowid_video_dict[x], col_ids)
                worksid_values = zip(works_ids, values)
                worksid_values = filter(lambda x: x[0] != origin_worksid,
                        worksid_values)
                worksid_values = heapq.nlargest(max_sim_cnt, worksid_values,
                        key=lambda x: x[1])
                worksid_value_strs = map(lambda x: '{0}:{1:.3f}'.format(x[0], x[1]),
                        worksid_values)
                combined_str = '$$'.join(worksid_value_strs)
                output_obj.write('{0}\t{1}\n'.format(origin_worksid, combined_str))
        return True

if __name__ == '__main__':
    start_time = datetime.now()
    MergeSim.mergeSim('movie', 20)
    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    print 'spend {0} minutes'.format(minutes)

