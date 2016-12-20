"""
# @file debug.py
# @Synopsis  to show detailed similarity parameters
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2016-01-20
"""
from scipy.sparse import csr_matrix
import sys
import os
sys.path.append('..')
from sklearn.externals import joblib
from conf.env_config import EnvConfig
from bll.data.video_info_util import VideoInfoUtil

works_type = sys.argv[1]
vid1 = sys.argv[2]
data_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'intermediate')
# title_sim_matrix = joblib.load(os.path.join(data_path, 'title_sim_matrix'))
# title_seg_sim_matrix = joblib.load(os.path.join(data_path, 'title_seg_sim_matrix'))
# director_sim_matrix = joblib.load(os.path.join(data_path, 'director_sim_matrix'))
# if works_type is not 'show':
#     actor_sim_matrix = joblib.load(os.path.join(data_path, 'actor_sim_matrix'))
category_sim_matrix = joblib.load(os.path.join(data_path, 'category_sim_matrix'))
# tag_sim_matrix = joblib.load(os.path.join(data_path, 'tag_sim_matrix'))

# year_decay_matrix = joblib.load(os.path.join(data_path, 'year_decay_matrix'))
half_item_cf_matrix = joblib.load(os.path.join(data_path, 'item_cf.pkl'))
item_cf_matrix = half_item_cf_matrix + half_item_cf_matrix.T
video_row_id_dict = VideoInfoUtil.getVideoRowIdDict(works_type)
while(1):
    try:
        vid2 = str(input('vid2: '))
        rowid1 = video_row_id_dict[vid1]
        rowid2 = video_row_id_dict[vid2]

        # title_sim = title_sim_matrix[rowid1, rowid2]
        # title_seg_sim = title_seg_sim_matrix[rowid1, rowid2]
        # director_sim = director_sim_matrix[rowid1, rowid2]
        # actor_sim = 0 if works_type == 'show' else actor_sim_matrix[rowid1, rowid2]
        category_sim = category_sim_matrix[rowid1, rowid2]
        # tag_sim = tag_sim_matrix[rowid1, rowid2]
        # year_decay = year_decay_matrix[rowid2, rowid2]
        # text_sim = text_sim_matrix[rowid1, rowid2]
        user_behavior_sim = item_cf_matrix[rowid1, rowid2]

        # col_names = ['text_sim', 'title_sim', 'seg_sim', 'dir_sim', 'act_sim',
        #         'cate_sim', 'tag_sim', 'year_decay', 'item_cf']
        # col_values = [text_sim, title_sim, title_seg_sim, director_sim,
        #         actor_sim, category_sim, tag_sim, year_decay, user_behavior_sim]
        # col_value_strs = ['{:.3f}'.format(i) for i in col_values]

        col_names = ['cate_sim', 'item_cf']
        col_values = [category_sim, user_behavior_sim]
        col_value_strs = ['{:.3f}'.format(i) for i in col_values]

        row_format = '{:>10}' * len(col_names)
        print row_format.format(*col_names)
        print row_format.format(*col_value_strs)

    except Exception as e:
        print e.message
