from utils import collect_sents

query = 'absorption'
for path_file_dir in ['pmc_comm', 'pmc_noncomm']:
    collect_sents(path_file_dir=path_file_dir, sents_file_name=path_file_dir, query_wrd=query)

