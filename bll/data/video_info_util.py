#encoding=utf8
"""
# @file video_info_util.py
# @Synopsis  some util functions
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
import os
from data_source_conf import long_videos_dict
from conf.env_config import EnvConfig
from urllib import quote

class VideoInfoUtil(object):
    """
    # @Synopsis  some video info utils
    """

    @staticmethod
    def getLongVideoTrunkDict(work_type):
        """
        # @Synopsis  get dict of {work_id, trunk}
        #
        # @Args work_type
        #
        # @Returns   result dict
        """
        video_list = long_videos_dict[work_type].load()
        video_list = filter(lambda x: (x['work_id'] and x['trunk']) is not None, video_list)
        video_trunk_list = map(lambda x: (x['work_id'], x['trunk']), video_list)
        video_trunk_list = map(lambda x: (x[0], u'《{0}》'.format(x[1])), video_trunk_list)
        video_trunk_dict = dict(video_trunk_list)
        return video_trunk_dict


    @staticmethod
    def getLongVideoTrunk2IdDict(work_type):
        """
        # @Synopsis  get trunk to work_ids(list) dict
        #
        # @Args work_type
        #
        # @Returns   dict({trunk: [work_ids]})
        """
        trunk2id_dict = dict()
        video_list = long_videos_dict[work_type].load()
        video_list = filter(lambda x: (x['work_id'] and x['trunk']) is not None, video_list)
        video_trunk_list = map(lambda x: (x['work_id'], x['trunk']), video_list)
        vid_list = map(lambda x: x[0], video_trunk_list)
        input_path = os.path.join(EnvConfig.LOCAL_DATA_BASE_PATH, work_type,
                'source', 'hot_video_alias')
        if os.path.isfile(input_path):
            input_obj = open(input_path)
            for line in input_obj:
                line = line.decode('utf8')
                fields = line.strip('\n').split('\t')
                work_id = fields[0]
                alias = fields[1]
                if work_id in vid_list:
                    video_trunk_list.append((work_id, alias))
            input_obj.close()
        video_trunk_list = map(lambda x: (x[0], u'《{0}》'.format(x[1])), video_trunk_list)
        for vid, trunk in video_trunk_list:
            if trunk not in trunk2id_dict:
                trunk2id_dict[trunk] = [vid]
            else:
                trunk2id_dict[trunk].append(vid)

        return trunk2id_dict

    @staticmethod
    def getVideoRowIdDict(work_type):
        """
        # @Synopsis  read the <works_type>_final file and load the row number of
        # each video, for the usage of spars matrix, this dict should include
        # all lines that include works_id and title
        # before merging
        #
        # @Args works_type
        #
        # @Returns   dict({works_id: rowid}), where works_id is string, rowid is
        # int
        """
        video_list = long_videos_dict[work_type].load()
        video_rowid_dict = dict()
        for rowid, video in enumerate(video_list):
            video_rowid_dict[video['work_id']] = rowid

        return video_rowid_dict


    @staticmethod
    def loadListFromStr(field):
        """
        # @Synopsis  deserialize list, with saperator '$$'
        #
        # @Args field
        #
        # @Returns   ret list
        """
        try:
            values = field.strip().split('$$')
            values = map(lambda x: x.strip(), values)
            values = filter(lambda x: x != '', values)
            return values
        except Exception as e:
            return []

