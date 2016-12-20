"""
# @Synopsis  get_instance_by_service of baidu
"""
import commands

def getInstanceByService(db_bns):
    """
    # @Synopsis  host and port by BNS
    # @Args db_bns
    # @Returns   host, port
    """
    bash_cmd = 'get_instance_by_service -p {0}'.format(db_bns)
    status, output = commands.getstatusoutput(bash_cmd)
    if output == 'service not exist':
        raise ValueError('service not exist')
    elif status != 0:
        raise OSError('Failed to execute get_instance_by_service')

    host, port = output.split(' ')
    port = int(port)
    return host, port
