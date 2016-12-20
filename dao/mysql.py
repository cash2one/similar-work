"""
# @file mysql_dal.py
# @Synopsis  mysql dal, connet to ns_video db and final db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""

import os
import logging
import MySQLdb

class MySQLConn(object):
    """
    # @Synopsis  mysql dal
    """
    def __init__(self, conf_dict, log_name):
        self.logger = logging.getLogger(log_name)
        self.conn = MySQLdb.connect(use_unicode=True, **conf_dict)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def insert(self, table_name, rows):
        """
        # @Synopsis  insert rows to database
        # @Args table_name
        # @Args rows
        # @Returns   number of successfully inserted rows
        """
        if len(rows) == 0:
            return 0
        column_name_list = rows[0].keys()
        values_list = map(lambda x: [x[k] for k in column_name_list], rows)
        sql = u'insert into {0}({1}) values({2})'.format(table_name, ','.join(column_name_list),
                ','.join(['%s'] * len(column_name_list)))
        n = self.cursor.executemany(sql, values_list)
        self.conn.commit()
        return n

    def delete(self, table_name, condition_dicts):
        """
        # @Synopsis  delete rows from table
        # @Args table_name
        # @Args rows
        # @Returns   lines deleted
        """
        if len(condition_dicts) == 0:
            return 0
        column_name_list = condition_dicts[0].keys()
        values_list = map(lambda x: [x[k] for k in column_name_list], condition_dicts)
        conditions = map(lambda x: '{0}=%s'.format(x), column_name_list)
        condition_str = ' and '.join(conditions)
        sql = u'DELETE FROM {0} WHERE {1}'.format(table_name, condition_str)
        n = self.cursor.executemany(sql, values_list)
        self.conn.commit()
        return n

    def update(self, table_name, condition_value_dicts):
        """
        # @Synopsis  update rows in table
        # @Args table_name
        # @Args row
        # @Args condition_str
        # @Returns   updated line cnt
        """
        if len(condition_value_dicts) == 0:
            return 0

        condition_column_name_list = condition_value_dicts[0][0].keys()
        set_column_name_list = condition_value_dicts[0][1].keys()

        def value_mapper(x):
            """
            # @Synopsis  order the dict values by the the column order of the first
            # row
            # @Args x
            # @Returns   
            """
            condition_values = [x[0][k] for k in condition_column_name_list]
            set_values = [x[1][k] for k in set_column_name_list]
            return set_values + condition_values

        value_list = map(value_mapper, condition_value_dicts)

        condition_strs = map(lambda x: u'{0}=%s'.format(x), condition_column_name_list)
        condition_str = ' and '.join(condition_strs)

        set_strs = map(lambda x: u'{0}=%s'.format(x), set_column_name_list)
        set_str = ','.join(set_strs)
        sql = u'UPDATE {0} SET {1} WHERE {2}'.format(table_name, set_str, condition_str)

        n = self.cursor.executemany(sql, value_list)
        self.conn.commit()
        return n

    def select(self, table_name, field_list, condition_str):
        """
        # @Synopsis  select from mysqldb
        #
        # @Args table_name
        # @Args field_list
        # @Args condition_str
        #
        # @Returns   rows
        """
        field_col_str_list = map(lambda x: '{column_name} as {field_name}'.format(**x), field_list)
        field_col_str = ','.join(field_col_str_list)
        sql_cmd = 'select {0} from {1} where {2}'.format(field_col_str, table_name, condition_str)
        self.cursor.execute(sql_cmd)
        ret = self.cursor.fetchall()
        return ret
