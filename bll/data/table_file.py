"""
# @file table_file.py
# @Synopsis  class of local disk table files, which contains lines consist of
# '\t' seperated column values
# @author Ming Gu(guming@itv.baidu.com))
# @version 1.0
# @date 2016-07-13
"""
class TableFile(object):
    """
    # @Synopsis  local files that contains lines of data, columns are 
    # seperated by \t, update method should be specified by subclasses
    """
    def __init__(self, field_list, file_path):
        """
        # @Synopsis  init
        #
        # @Args field_list
        # @Args file_path
        #
        # @Returns  nothing
        """
        self.field_list = field_list
        self.file_path = file_path

    def load(self):
        """
        # @Synopsis  load from local disk, the file format should be '\t' seperated
        # field values lines.
        #
        # @Returns   list of data, data unit is a dict of {key: value}
        """
        types = map(lambda x: x['data_type'], self.field_list)
        field_names = map(lambda x: x['field_name'], self.field_list)
        data_rows = []
        input_obj = open(self.file_path)
        for line in input_obj:
            line = line.decode('utf8').strip('\n')
            value_strs = line.split('\t')
            value_type_list = zip(value_strs, types)
            values = map(self.valueParser, value_type_list)
            key_values = zip(field_names, values)
            data_rows.append(dict(key_values))
        return data_rows

    def valueParser(self, value_type):
        """
        # @Synopsis  parse values in file, with the type specified in the class
        # object's field_list variable
        #
        # @Args value_type
        #
        # @Returns   value with the specified type if parsed correctly; None
        # otherwise
        """
        value = value_type[0]
        data_type = value_type[1]
        try:
            if data_type == bool:
                return bool(int(value))
            else:
                return data_type(value)
        except ValueError as e:
            return None



