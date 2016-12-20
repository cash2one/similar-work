"""
##
# @file env_config.py
# @Synopsis  config environment
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-12-07
"""
import sys
sys.path.append('..')
import os
import datetime as dt

class EnvConfig(object):
    """
    # @Synopsis  config environment
    """
    DEBUG = True
    SMS_RECEIVERS = ['18612861842']
    MAIL_RECEIVERS = ['guming@itv.baidu.com']
    # tv corresponds to tvplay in some cases, to tv itself in others...wtf, and
    # there is not such a word as 'tvplay', should be 'tvshow', I use 'tv' as the
    # only key correspond to the meaning of tvshow in this project
    WORKS_TYPE_ALIAS_DICT = dict({
        'movie': 'movie',
        'tv': 'tvplay',
        'comic': 'comic',
        'show': 'show',
        })

    WORKS_TYPE_NUM_DICT = dict({
        'movie': 0,
        'tv': 1,
        'show': 2,
        'comic': 3,
        })

    SIM_TYPE_LIST = ['category', 'director', 'actor']

    PLATFORM_LIST = ['PC', 'Mobile']
    PC_LOG_TYPE_LIST = ['play', 'view']
    MOBILE_LOG_TYPE_LIST = ['play', 'browse']

    #find project root path according to the path of this config file
    CONF_PATH = os.path.split(os.path.realpath(__file__))[0]
    # if the 'conf' module is provided to spark-submit script in a .zip file,
    # the real path of this file would be project_path/conf.zip/conf(refer to
    # the dal.spark_submit module), while the
    # real path of config file we wanna locate is project_path/conf, thus the
    # following transformation would be neccessary.
    if '.zip' in CONF_PATH:
        path_stack = CONF_PATH.split('/')
        CONF_PATH = '/'.join(path_stack[:-2]) + '/conf'
    PROJECT_PATH = os.path.join(CONF_PATH, '../')
    LOG_PATH = os.path.join(PROJECT_PATH, 'log')
    GENERAL_LOG_FILE = os.path.join(LOG_PATH, 'general.log')
    #script path

    #tool path
    if DEBUG:
        TOOL_PATH = '/home/video/guming02/tools/'
    else:
        TOOL_PATH = '/home/video/guming/tools'

    HADOOP_CLIENT_PATH = os.path.join(TOOL_PATH, 'hadoop-client')
    HADOOP_JAVA_HOME = os.path.join(HADOOP_CLIENT_PATH, 'java6')
    SPARK_CLIENT_PATH = os.path.join(TOOL_PATH, 'spark-client')
    JAVA_HOME = os.path.join(TOOL_PATH, 'hadoop-client/java6')
    MYSQL_CLIENT_PATH = 'mysql'
    MOLA_CLIENT_PATH = os.path.join(TOOL_PATH, 'mola')


    #HDFS input and output path
    HDFS_ROOT_PATH = "/app/vs/ns-video/"
    #user behavior log path
    HDFS_LOG_ROOT_PATH = dict()
    HDFS_LOG_ROOT_PATH['PC'] = os.path.join(HDFS_ROOT_PATH,
            'video-pc-data/vd-pc/behavior2/')
    HDFS_LOG_ROOT_PATH['Mobile'] = os.path.join(HDFS_ROOT_PATH,
            'video-mobile-data/vd-mobile/android-behavior-rec/')
    HDFS_LOG_PATH_DICT = dict()
    for platform in PLATFORM_LIST:
        HDFS_LOG_PATH_DICT[platform] = dict()
    HDFS_LOG_PATH_DICT['PC']['sim-work-click'] = os.path.join(
            HDFS_LOG_ROOT_PATH['PC'], 'sim-work-click/')
    # The log of PC play and view are saperated by works_type, which is not the
    # case in Mobile logs
    for log_type in PC_LOG_TYPE_LIST:
        HDFS_LOG_PATH_DICT['PC'][log_type] = dict()
        for works_type in WORKS_TYPE_ALIAS_DICT:
            HDFS_LOG_PATH_DICT['PC'][log_type][works_type] = os.path.join(
                    HDFS_LOG_ROOT_PATH['PC'],
                    '{0}/{1}/'.format(log_type, WORKS_TYPE_ALIAS_DICT[works_type]))
    for log_type in MOBILE_LOG_TYPE_LIST:
        HDFS_LOG_PATH_DICT['Mobile'][log_type] = os.path.join(
                HDFS_LOG_ROOT_PATH['Mobile'], log_type)

    #derivant output path
    HDFS_DERIVANT_PATH = os.path.join(HDFS_ROOT_PATH, 'guming/similar-work/')

    #final output path
    HDFS_FINAL_PATH = os.path.join(HDFS_ROOT_PATH, \
            'video-pc-result/vr-pc-play-daily-trackpush-spark/%s/' % \
            dt.date.today().strftime('%Y%m%d'))

    #local path
    LOCAL_DATA_PATH = os.path.join(PROJECT_PATH, 'data/')
    LOG_NAME = 'similar-work'

if __name__ == '__main__':
    print EnvConfig.CONF_PATH
