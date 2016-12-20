# encoding=utf8
"""
# @file deduplication.py
# @Synopsis  deduplication in similar work recommendation list. The logic is as
# following:
# tv: if two available videos share the same trunk and season, and 
# case1: both videos' 'version' field are NULL or Abridged or TV station:
# keep the work with higher uv, filter the other
# case2: at least one of the two videos has 'version' fields that is not in case
# 1:
# keep both
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2016-01-06
"""
import os
import logging
import itertools
from scipy.sparse import csr_matrix
from conf.env_config import EnvConfig
from bll.service.util import Util
from bll.data.data_source_conf import long_videos_dict

class Deduplication(object):
    """
    # @Synopsis  deduplication. Sometimes, one video has different version and
    # language, list them all in the recommendation list would significantly
    # waste page space. Thus deduplication logic is needed.
    """
    def __init__(self, works_type):
        self.__works_type = works_type

    def loadVersionDedupList(self):
        """
        # @Synopsis  load list of version that should be considered in
        # deduplication process from file
        #
        # @Returns   list of version
        """
        input_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, 'persistent',
                '{0}_version_dedup.txt'.format(self.__works_type))
        input_obj = open(input_path)
        with input_obj:
            return map(lambda x: x.decode('utf8').strip('\n'), input_obj)

    def loadVideoUVRankDict(self):
        """
        # @Synopsis  load video uv rank from file
        #
        # @Returns   list of (vid, rank)
        """
        input_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, self.__works_type, 'result',
                'uv_rank.txt')
        input_obj = open(input_path)
        with input_obj:
            video_list = map(lambda x: x.strip(), input_obj)
        return dict((vid, rank) for rank, vid in enumerate(video_list))

    def getDeduplicateMatrix(self):
        """
        # @Synopsis  deduplication logic. Detail is as described in file DocString
        #
        # @Returns   list of sign, which indicates whether the video should or not
        # appear in the recommendation result. The sign is int(0) or int(1)
        """
        videos = long_videos_dict[self.__works_type].load()
        # unavailable videos will be filtered afterwards, thus do not need
        # to be considered in dedunplication procedure
        filtered_videos = filter(lambda x: x['available'] == 1, videos)
        version_dedup_list = self.loadVersionDedupList()
        filtered_videos = filter(lambda x: x['version'] in version_dedup_list,
                filtered_videos)
        # ((trunk, season), works_id)
        filtered_videos = map(lambda x: ((x['trunk'], x['season']), x['work_id']),
                filtered_videos)
        filtered_videos.sort(key=lambda x: x[0])
        inferior_vid_list = []
        video_rank_dict = self.loadVideoUVRankDict()
        for key, duplicate_group in itertools.groupby(filtered_videos,
                key=lambda x: x[0]):
            # groupby function merely generate a iterable, however, we need to
            # iter over it more than one times, so first make a list of it
            duplicate_group_list = list(duplicate_group)
            superior_video = min(duplicate_group_list,
                    key=lambda x: video_rank_dict.get(x[1], 0))
            inferior_videos = filter(lambda x: x != superior_video,
                    duplicate_group_list)
            inferior_vids = map(lambda x: x[1], inferior_videos)
            inferior_vid_list += inferior_vids

        vids = map(lambda x: x['work_id'], videos)
        superior_sign_list = map(lambda x: x not in inferior_vid_list, vids)
        superior_sign_list = map(lambda x: int(x), superior_sign_list)
        return Util.getFilterMatrix(superior_sign_list)


if __name__ == '__main__':
    deduplicator = Deduplication('tv')
    deduplicate_filter = deduplicator.getDeduplicateMatrix()
    # print deduplicate_filter
