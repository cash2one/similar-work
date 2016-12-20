"""
# @file data_source_conf.py
# @Synopsis  contains configuration of data sources
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-04
"""
import os
from conf.env_config import EnvConfig
from table_file import TableFile
from cached_mysql_table import CachedMySQLTable

__all__ = ['long_videos_dict', 'related_works_dict']

def field_list_mapper(x):
    """
    # @Synopsis  map field list to requried format
    # @Args x
    # @Returns   list of dicts
    """
    if len(x) == 2:
        return {'field_name': x[0], 'data_type': x[1]}
    elif len(x) == 3:
        return {'field_name': x[0], 'column_name': x[1], 'data_type': x[2]}


def alterFieldList(field_list, field_name, new_field_setting):
    """
    # @Synopsis  alter field list, for convience of configuring similar data
    # sources, etc. four types of long videos
    #
    # @Args field_list
    # @Args field_name
    # @Args new_field_setting
    #
    # @Returns   None
    """
    for field_num, field_setting in enumerate(field_list):
        if field_setting[0] == field_name:
            field_list[field_num] = new_field_setting


related_works_dict = dict()
field_list = [
        ['work_id', 'works_id', unicode],
        ['similar_works', 'similar_works', unicode],
        ]
field_list = map(field_list_mapper, field_list)
for sim_type in ['category', 'director', 'actor']:
    inner_dict = dict()
    for works_type, works_type_num in EnvConfig.WORKS_TYPE_NUM_DICT.iteritems():
        condition_str = 'works_type={0}'.format(works_type_num)
        if sim_type == 'category':
            table_name = 'similar_works'
        else:
            table_name = 'same_{0}_works'.format(sim_type)
        file_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'source', table_name)
        inner_dict[works_type] = CachedMySQLTable(table_name=table_name, field_list=field_list,
                condition_str=condition_str, db_name='Attr', file_path=file_path)
    related_works_dict[sim_type] = inner_dict


long_videos_dict = dict()
for works_type in EnvConfig.WORKS_TYPE_ALIAS_DICT:
    field_list = [
            ['work_id', 'works_id', unicode],
            ['trunk', 'trunk', unicode],
            ['aliases', 'alias', unicode],
            ['directors', 'director', unicode],
            ['actors', 'actor', unicode],
            ['categories', 'type', unicode],
            ['tags', 'rc_tags', unicode],
            ['areas', 'area', unicode],
            ['release_year', 'al_date', int],
            ['available', 'all_sites!=""', bool],
            ['version', 'version', unicode],
            ['season', 'season', int],
            ['language', 'language', unicode],
            ['finish', 'finish', bool],
            ['author', '""', unicode],
            ['sites', 'all_sites', unicode],
            ]
    if works_type == 'movie':
        alterFieldList(field_list, 'release_year', ['release_year', 'net_show_time', int])
        alterFieldList(field_list, 'available', ['available', 'sites!=""', bool])
        alterFieldList(field_list, 'finish', ['finish', '1', bool])
        alterFieldList(field_list, 'sites', ['sites', 'sites', unicode])
    if works_type == 'comic':
        alterFieldList(field_list, 'author', ['author', 'author', unicode])
    if works_type == 'show':
        alterFieldList(field_list, 'directors', ['directors', 'host', unicode])
        alterFieldList(field_list, 'actors', ['actors', 'host', unicode])
    field_list = map(field_list_mapper, field_list)

    conditions = ['title!=""', 'big_poster!=""']
    if works_type == 'movie':
        conditions.append('source!=16')
    condition_str = ' and '.join(conditions)
    file_path = os.path.join(EnvConfig.LOCAL_DATA_PATH, works_type, 'source', 'video_final')
    long_videos_dict[works_type] = CachedMySQLTable(table_name = '{0}_final'.format(works_type),
            field_list=field_list, condition_str=condition_str, db_name='Final',
            file_path=file_path)

