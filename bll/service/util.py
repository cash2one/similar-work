# coding=utf-8
"""
# @file util.py
# @Synopsis  some utility functions
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-08
"""
import datetime as dt
import ConfigParser
from conf.env_config import EnvConfig
from dao.hdfs import HdfsClient
from dao.mysql import MySQLConn
from dao.get_instance_by_service import getInstanceByService
import os
from scipy.sparse import csr_matrix

class Util(object):
    """
    # @Synopsis  some utility functions that would be used repeatedly
    """

    @staticmethod
    def getMySQLConn(conf_filename, db_name):
        """
        # @Synopsis  get mysql connection
        # @Args conf_filename
        # @Args db_name
        # @Returns   connection obj
        """
        conf_path = os.path.join(EnvConfig.CONF_PATH, 'mysql', conf_filename)
        config = ConfigParser.RawConfigParser()
        config.read(conf_path)

        try:
            bns = config.get(db_name, 'bns')
            host, port = getInstanceByService(bns)
        except ConfigParser.NoOptionError as e:
            host = config.get(db_name, 'host')
            port = config.getint(db_name, 'port')

        user = config.get(db_name, 'user')
        passwd = config.get(db_name, 'passwd')
        db = config.get(db_name, 'db')

        config_dict = {
                'host': host,
                'port': port,
                'user': user,
                'passwd': passwd,
                'db': db,
                }

        conn = MySQLConn(config_dict, EnvConfig.LOG_NAME)
        return conn

    @staticmethod
    def genLogList(day_cnt, days_before,
            works_type='movie', log_type='play', platform='PC'):

        """
        # @Synopsis  get hdfs log file list, to be the input to spark, already
        # filtered blank hdfs path which would cause error
        #
        # @Args day_cnt
        # @Args days_before how many days the time window is before today, e.g. if
        # you want to get the log up to yesterday, days_before is 1
        # @Args works_type
        # @Args platform
        #
        # @Returns  log path joined by ','
        # TODO: should add argument to determine which kind of log to get(play,
        # view, search, etc)
        """
        today = dt.date.today()
        deltas = range(days_before, days_before + day_cnt)
        days = [(today - dt.timedelta(i)) for i in deltas]
        time_window = map(lambda x: x.strftime('%Y%m%d'), days)

        if platform == 'PC':
            if log_type in ['play', 'view']:
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]\
                        [works_type]
            elif log_type == 'sim-work-click':
                hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['PC'][log_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_LOG_PATH_DICT['Mobile'][log_type]
        file_list = [(hdfs_path + date) for date in time_window]

        hdfs_client = HdfsClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.LOG_NAME)
        return ','.join(filter(lambda x: hdfs_client.exists(x), file_list))


    @staticmethod
    def getOneHourSampleLog(works_type='movie', platform='PC'):
        """
        # @Synopsis get a hourly play log file hdfs path, for test only
        #
        # @Args works_type
        # @Args platform
        #
        # @Returns  hdfs path
        """
        sample_day = dt.date.today() - dt.timedelta(2)
        sample_day_str = sample_day.strftime('%Y%m%d')
        hdfs_path = ''
        if platform == 'PC':
            hdfs_path = EnvConfig.HDFS_PC_PLAY_LOG_PATH_DICT[works_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_MOBILE_PLAY_LOG_PATH_DICT[works_type]

        log_file = '{0}{1}/{1}20'.format(hdfs_path, sample_day_str)
        return log_file


    @staticmethod
    def saveRdd(rdd, hdfs_path, local_path):
        """
        # @Synopsis  save a rdd both to hdfs and local file system
        #
        # @Args rdd
        # @Args hdfs_path
        # @Args local_path
        #
        # @Returns  succeeded or not
        """

        succeeded = True
        hdfs_client = HdfsClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.LOG_NAME)
        if hdfs_client.exists(hdfs_path):
            succeeded = hdfs_client.rmr(hdfs_path)
        if succeeded:
            try:
                rdd.saveAsTextFile(hdfs_path)
            except Exception as e:
                print 'failed to save {0} to {1}'.format(
                    rdd, hdfs_path)
                print e.message
                return False
            if os.path.exists(local_path):
                print 'local path {0} exists, deleting it'\
                        .format(local_path)
                try:
                    os.remove(local_path)
                except Exception as e:
                    print 'failed to save {0} to {1}'.format(
                        hdfs_path, local_path)
                    return False
            succeeded = hdfs_client.getmerge(hdfs_path, local_path)
            return succeeded
        else:
            return False


    @staticmethod
    def savePickle(rdd, hdfs_path):
        """
        # @Synopsis  save a rdd as a pickle file to a hdfs url, if the path exits,
        # overwrite it
        #
        # @Args rdd
        # @Args hdfs_path
        #
        # @Returns   succeeded or not(to be done)
        """
        hdfs_client = HdfsClient(EnvConfig.HADOOP_CLIENT_PATH, EnvConfig.LOG_NAME)
        if hdfs_client.exists(hdfs_path):
            hdfs_client.rmr(hdfs_path)
        rdd.repartition(1).saveAsPickleFile(hdfs_path)


    @staticmethod
    def debugRdd(rdd, rdd_name, logger):
        """
        # @Synopsis  a debug function, send the infomation of a rdd to logger
        #
        # @Args rdd
        # @Args logger
        #
        # @Returns  nothing
        """
        logger.debug('{0}: {1}\t{2}'.format(rdd_name, rdd.count(),
            rdd.take(5)))

    @staticmethod
    def saveCsrMatrix(matrix, vocabulary, file_path, row_labels=None):
        """
        # @Synopsis  save csr_matrix in a readable way
        #
        # @Args matrix csr_matrix
        # @Args vocabulary column vocabulary
        # @Args file_path output path
        # @Args row_labels row label if any
        #
        # @Returns   nothing
        """
        line_cnt = matrix.shape[0]
        if row_labels is not None  and line_cnt != len(row_labels):
            raise Exception('Value Error',
                    'Matrix row count inequal to row lable count')
        output_obj = open(file_path, 'w')
        with output_obj:
            for i in xrange(0, matrix.shape[0]):
                begin_index = matrix.indptr[i]
                next_begin_index = matrix.indptr[i + 1]
                column_id_values = []
                if row_labels is not None:
                    output_obj.write(u'{0}\t'.format(row_labels[i])\
                            .encode('utf8'))
                for index in xrange(begin_index, next_begin_index):
                    column_id_values.append((matrix.indices[index],
                        matrix.data[index]))
                column_id_values.sort(key=lambda x: x[1], reverse=True)
                for column_id, value in column_id_values:
                    token = vocabulary[column_id]
                    output_obj.write(u'({0},{1}) '.format(token, value)\
                            .encode('utf8'))
                output_obj.write('\n')

    @staticmethod
    def getFilterMatrix(filter_list):
        """
        # @Synopsis  transform a filter_list to a diagonal filter matrix,
        # whoes elements are 0 if the correspond element is filtered, 1
        # otherwise
        #
        # @Args filter_list, a list of 0 or 1, denoting whether the video
        # should be filtered
        #
        # @Returns   filter matrix(csr_matrix)
        """
        dim = len(filter_list)
        row_ind = []
        col_ind = []
        data = []
        for row_id, filtered in enumerate(filter_list):
            row_ind.append(row_id)
            col_ind.append(row_id)
            data.append(filtered)
        filter_matrix = csr_matrix((data, (row_ind, col_ind)), shape=(dim, dim))
        return filter_matrix


if __name__ == '__main__':
    print Util.genLogList(7, 1, works_type='tv')

