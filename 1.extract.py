import os, sys
from utils import get_pmc_paths

"""
the data will get stored in the data directory
"""
pmc_comm_use = './data/pmc/oa_2022/comm_use_newversion'
pmc_noncomm_use = './data/pmc/oa_2022/noncomm_use_newversion'

query = 'absorption'
get_pmc_paths(pmc_comm_use, query_wrd=query, save_folder='pmc_comm')
