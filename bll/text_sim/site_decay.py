"""
# @file site_decay.py
# @Synopsis  video of some sites are of low quality according to PM, needed
# decay in weight
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2016-03-24
"""

from scipy.sparse import csr_matrix
class SiteDecay(object):
    """
    # @Synopsis  calculate decay matrix of video causing by the quality level of
    # sites
    """
    SITE_DECAY_LIST = ['pps.tv', '61.com', 'kumi.cn']

    @staticmethod
    def calDecayMatrix(sites_list):
        """
        # @Synopsis  calculate decay matrix
        #
        # @Args sites_list list of sites, which is the sites/all_sites field in
        # **_final table of final database
        #
        # @Returns   a diagonal matrix containing decay weight of each video in
        # corresponding element, weigh=1 if no decay is applied
        """
        dim = len(sites_list)
        decay_weights = map(SiteDecay.getDecayWeight, sites_list)
        row_ind = []
        col_ind = []
        data = []
        for row_id, weight in enumerate(decay_weights):
            row_ind.append(row_id)
            col_ind.append(row_id)
            data.append(weight)
        decay_matrix = csr_matrix((data, (row_ind, col_ind)), shape=(dim, dim))
        return decay_matrix

    @staticmethod
    def getDecayWeight(site_str):
        """
        # @Synopsis  decay weight calculation
        #
        # @Args site_str
        #
        # @Returns   decay weight, 1 for no decay
        """
        try:
            sites = site_str.split('$$')
            site_domains = map(lambda x: x.split(',')[0], sites)
            none_decay_sites = filter(lambda x: x not in SiteDecay.SITE_DECAY_LIST, site_domains)
            if len(none_decay_sites) == 0:
                return 0.5
            else:
                return 1
        except Exception as e:
            print e.message
            return 1
