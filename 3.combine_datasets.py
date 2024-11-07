import sys
import os
import pickle
import pandas as pd

def collect_data(prop):
    
    with open("./pmc_comm.pkl", "rb") as f:
        comm = pickle.load(f)

    comm = pd.DataFrame(comm, columns=['text', 'file'])
    comm['source'] = 'comm'

    with open("./pmc_noncomm.pkl", "rb") as f:
        noncomm = pickle.load(f)

    noncomm = pd.DataFrame(noncomm, columns=['text', 'file'])
    noncomm['source'] = 'noncomm'

    s2_dir = os.listdir("./s2orc/")
    s2rc = pd.read_csv(f'./s2orc/s2rc_final.csv')

    s2rc = s2rc.drop_duplicates(subset=['text'])
    s2rc['source'] = 's2rc'
    df_all = pd.concat([comm, noncomm, s2rc])
    df_all = df_all.drop_duplicates(subset=['text'])
    df_all.to_csv(f'./{prop}_data.csv', index=False)
    

if __name__ == "__main__":
   prop = sys.argv[1]
   print("prop = ", prop)
   collect_data(prop)

