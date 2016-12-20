"""
# @file update_db.py
# @Synopsis  insert/update similar work result to mysql db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-22
"""
import os
import time
import logging
from bll.data.data_source_conf import related_works_dict
from conf.env_config import EnvConfig
from dao.mysql import MySQLConn
from bll.service.util import Util

logger = logging.getLogger(EnvConfig.LOG_NAME)

class UpdateDb(object):
    """
    # @Synopsis  update mysql db similar_works table with calculated similar work
    # results
    """
    @staticmethod
    def updateRelatedWork(sim_type, works_type):
        """
        # @Synopsis  update tables in ns_video
        # @Args sim_type
        # @Args works_type
        # @Returns   succeeded
        """
        SIMILAR_WORK_CNT_LOWER_BOUND_DICT = dict({
            'category': 4,
            'director': 0,
            'actor': 0
            })
        ALTER_LINE_ALARM_THRESHOLD = 1
        related_works_dict[sim_type][works_type].update()
        related_works = related_works_dict[sim_type][works_type].load()
        old_similar_dict = dict([(x['work_id'], x['similar_works']) for x in related_works])

        # exist_ids = set(map(lambda x: x['work_id'], related_works))

        filename = '{0}_sim_result'.format(sim_type)
        if sim_type == 'director':
            filename = 'reranked_{0}'.format(filename)

        input_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'result', filename)
        sim_obj = open(input_path)
        sim_results = map(UpdateDb.parseSimilarLine, sim_obj)

        def enoughWorkFilter(x):
            """
            # @Synopsis  if result is too short, it's likely that sth goes wrong
            # @Args x
            # @Returns   
            """
            return x['similar_works'].count('$$') >=\
                    SIMILAR_WORK_CNT_LOWER_BOUND_DICT[sim_type] - 1

        valid_results = filter(enoughWorkFilter, sim_results)
        if len(valid_results) < ALTER_LINE_ALARM_THRESHOLD:
            logger.critical('Less than {0} lines altered in {1} {2}'.format(
                ALTER_LINE_ALARM_THRESHOLD, works_type, sim_type))

        for row in valid_results:
            row['works_id'] = row.pop('work_id')
            row['works_type'] = EnvConfig.WORKS_TYPE_NUM_DICT[works_type]

        insert_rows = filter(lambda x: x['works_id'] not in old_similar_dict, valid_results)

        def updateFilter(x):
            """
            # @Synopsis  only update diff
            # @Args x
            # @Returns   needs update or not
            """
            return x['works_id'] in old_similar_dict and \
                    x['similar_works'] != old_similar_dict[x['works_id']]

        update_rows = filter(updateFilter, valid_results)
        condition_dicts = []
        for row in update_rows:
            condition_dict = dict({
                'works_id': row.pop('works_id'),
                'works_type': row.pop('works_type')
                })
            condition_dicts.append(condition_dict)

        conf_filename = 'test.cfg' if EnvConfig.DEBUG else 'online.cfg'
        mysql_conn = Util.getMySQLConn(conf_filename, 'Attr')
        if sim_type == 'category':
            table_name = 'similar_works'
        else:
            table_name = 'same_{0}_works'.format(sim_type)
        insert_cnt = mysql_conn.insert(table_name, insert_rows)
        update_cnt = mysql_conn.update(table_name, zip(condition_dicts, update_rows))
        logger.debug('{0} {1} insert_lines: {2}, update_lines: {3}'.format(
            sim_type, works_type, insert_cnt, update_cnt))
        return True


    @staticmethod
    def parseSimilarLine(line):
        """
        # @Synopsis  parse calculated similar work line, to match the format in
        # mysql database
        #
        # @Args line 'work_id\tsim_work1:sim_score1$$...'
        #
        # @Returns   (work_id, similar_work_ids, similar_work_scores)
        """
        fields = line.strip('\n').split('\t')
        origin_id = fields[0]
        if fields[1] != '':
            vid_value_strs = fields[1].split('$$')
            vid_value_tuples = map(lambda x: x.split(':'), vid_value_strs)
            vids = map(lambda x: x[0], vid_value_tuples)
            values = map(lambda x: x[1], vid_value_tuples)
            vids_str = '$$'.join(vids)
            values_str = '$$'.join(values)
        else:
            vids_str = ''
            values_str = ''
        return dict({
            'work_id': origin_id,
            'similar_works': vids_str,
            'similar_score': values_str
            })


if __name__ == '__main__':
    UpdateDb.updateSimilarWork('tv')
