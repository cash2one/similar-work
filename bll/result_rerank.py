"""
# @file result_rerank.py
# @Synopsis  rerank the final result, mainly to satisfy PM request
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2016-02-23
"""
import os
from conf.env_config import EnvConfig
from bll.data.data_source_conf import long_videos_dict


class ResultRerank(object):
    """
    # @Synopsis  rerank result to fit PM's request
    """

    @staticmethod
    def resultRerank(works_type):
        """
        # @Synopsis  rerank result to fit PM's request, in this particular case,
        # rerank the result by release year desc, and unfinished works get on the
        # top
        #
        # @Args works_type
        #
        # @Returns   succeeded or not(to be done)
        """
        video_list = long_videos_dict[works_type].load()
        works_id_list = map(lambda x: x['work_id'], video_list)
        video_info_dict = dict(zip(works_id_list, video_list))

        same_director_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'result',
                'director_sim_result')
        reranked_same_director_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'result',
                'reranked_director_sim_result')
        result_obj = open(same_director_path)
        reranked_obj = open(reranked_same_director_path, 'w')

        def rerank_key_func(vid):
            """
            # @Synopsis  key function for the use in sort
            #
            # @Args vid
            #
            # @Returns   value designated to the given key
            """
            if vid not in video_info_dict:
                return 0
            else:
                return (1 - int(video_info_dict[vid]['finish'])) * 10000 + \
                        int(video_info_dict[vid]['release_year'] or 1970)

        with result_obj, reranked_obj:
            for line in result_obj:
                fields = line.strip('\n').split('\t')
                origin_id = fields[0]
                if fields[1] != '':
                    vid_value_strs = fields[1].split('$$')
                    vid_value_tuples = map(lambda x: x.split(':'),
                            vid_value_strs)
                    vid_value_dict = dict(vid_value_tuples)
                    vids = map(lambda x: x[0], vid_value_tuples)
                    vids.sort(key=rerank_key_func, reverse=True)
                    reranked_values = map(lambda x: vid_value_dict[x], vids)
                    vid_value_tuples = zip(vids, reranked_values)
                    vid_value_strs = map(lambda x: '{0}:{1}'.format(x[0], x[1]),
                            vid_value_tuples)
                    combined_str = '$$'.join(vid_value_strs)
                    reranked_obj.write('{0}\t{1}\n'.format(origin_id,
                        combined_str))
                else:
                    reranked_obj.write('{0}\t\n'.format(origin_id))
        return True

if __name__ == '__main__':
    ResultRerank.resultRerank('tv')
