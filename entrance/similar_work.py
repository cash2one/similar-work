"""
# @file similar_work.py
# @Synopsis  calculate similar works
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-22
"""

import sys
sys.path.append('..')
import commands
from datetime import datetime
from conf.env_config import EnvConfig
from conf.init_logger import initLogger
from dao.spark_submit import SparkSubmitter
from bll.data.data_source_conf import long_videos_dict
from bll.text_sim.cal_sim import CalTextSim
from bll.merge_sim import MergeSim
from bll.result_rerank import ResultRerank
from bll.update_db import UpdateDb
import logging

if __name__ == '__main__':
    start_time = datetime.now()
    initLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    works_type = sys.argv[1]

    logger.debug('start calculating similar {0}'.format(works_type))

    # dump final table from final database
    long_videos_dict[works_type].update()

    # cal text sim
    succeeded = CalTextSim.calSim(works_type)
    if not succeeded:
        logger.critical('text similarity computation failed')
        sys.exit(1)
    else:
        logger.debug('text similarity generated')


    # cal itemcf
    main_program_path = '../bll/item_cf.py'
    bash_cmd = 'sed -i "s/^\s*works_type = .*$/    works_type = \'{0}\'/" {1}'\
            .format(works_type, main_program_path)
    status, output = commands.getstatusoutput(bash_cmd)
    logger.debug('Returned {0}: {1}\n{2}'.format(status, bash_cmd, output))
    spark_submitter = SparkSubmitter(EnvConfig.SPARK_CLIENT_PATH, EnvConfig.LOG_NAME)
    succeeded = spark_submitter.submit_with_modules(main_program_path, '..', run_locally=False)
    if not succeeded:
        logger.critical('user behavior similarity computation failed')
        sys.exit(1)
    else:
        logger.debug('item cf generated')

    # merge similarity
    MAX_REC_CNT = 20
    succeeded = MergeSim.mergeSim(works_type, MAX_REC_CNT)
    if not succeeded:
        logger.critical('merge similarity failed')
        sys.exit(1)
    else:
        logger.debug('similar work file generated')

    # rerank result by PM request
    succeeded = ResultRerank.resultRerank(works_type)
    if not succeeded:
        logger.critical('result rerank failed')
        sys.exit(1)
    else:
        logger.debug('result reranked')

    # update mysql database
    for sim_type in EnvConfig.SIM_TYPE_LIST:
        succeeded = UpdateDb.updateRelatedWork(sim_type, works_type)
    if not succeeded:
        logger.critical('update mysql db failed')
    else:
        logger.debug('mysql database updated')

    end_time = datetime.now()
    time_span = end_time - start_time
    minutes = time_span.total_seconds() / 60
    logger.debug('similar works spent {0} minutes'.format(minutes))
