import pandas as pd
import os
from tqdm import tqdm
import numpy as np
import string
import re
import random

# string.punctuation

#defining the function to remove punctuation
def remove_punctuation(text):
    punctuationfree="".join([i for i in text if i not in string.punctuation])
    return punctuationfree
#storing the puntuation free text



def prepare_data_sets(tr_props, vl_props, ts_props):
    
    tr_dfs = [pd.read_csv(a) for a in [f"./data/matprops/{prop}/{prop}_ner_bert.csv" for prop in tr_props] ]
    tr_dfs = pd.concat(tr_dfs)

    vl_dfs = [pd.read_csv(a) for a in [f"./data/matprops/{prop}/{prop}_ner_bert.csv" for prop in vl_props] ]
    vl_dfs = pd.concat(vl_dfs)

    ts_dfs = [pd.read_csv(a) for a in [f"./data/matprops/{prop}/{prop}_ner_bert.csv" for prop in ts_props]]
    ts_dfs = pd.concat(ts_dfs)
    
    return tr_dfs, vl_dfs, ts_dfs

def remove_unicodes(text):
    new = re.sub(r"[^\x00-\x7F]+", "1__REMOVE__1", text)
    return new 


def more_cleaning(dft):
    us = [ '\uf0d2', '¼\ue001', '\ue002', '\u202bס\u202c', '\uf073', '\ue300', '\uf044', 
          '\u202bف\u202c', '\uf072', '\uf0fc', '\u2009', '\xa0', '\u202f', '\u2005',  '\u2003',
          ' ', '\t', '\t ']
    
    dft = dft[~dft.word.isin(us)]
    dft.reset_index(drop=True, inplace=True)
    
    return dft


def reduce_size(dft):
    
    grps = dft.groupby('text_id')
    keep=[]
    for k in tqdm(grps.groups.keys()):
        g = grps.get_group(k)
        if (g.shape[0] <70) & (g.shape[0] > 10):
            keep.append(k)
    dft = dft[dft.text_id.isin(keep)]
    
    return dft


if __name__ == "__main__":

    props = ['bp', 'mp', 'capacitance', 'permittivity',
              'conductivity', 'mass', 
              'charge','density','absorption', 'solubility']


    dfs = [pd.read_csv(a) for a in [f"./data/matprops/{prop}/{prop}_ner_bert.csv" for prop in props] ]
    all_props = ['capacitance', 'conductivity', 'mass', 'density','absorption', 'bp', 'charge', 'mp', 'solubility']

    for prop in all_props:
        
        test_prop = [prop]
        val_prop = random.sample(list(set(all_props).difference(test_prop)) , 2)
        train_prop = list(set(all_props).difference(val_prop+test_prop) )
        
        tr_dfs, vl_dfs, ts_dfs = prepare_data_sets(train_prop, val_prop, test_prop)

        tr_dfs = reduce_size(tr_dfs)
        vl_dfs = reduce_size(vl_dfs)
        ts_dfs = reduce_size(ts_dfs)

        tr_dfs = more_cleaning(tr_dfs)
        vl_dfs = more_cleaning(vl_dfs)
        ts_dfs = more_cleaning(ts_dfs)

        os.mkdir(f'./data_test_{prop}')
        
        tr_dfs.to_csv(f'./data_test_{prop}/train.csv', index=False)
        vl_dfs.to_csv(f'./data_test_{prop}/val.csv', index=False)
        ts_dfs.to_csv(f'./data_test_{prop}/test.csv', index=False)

        pd.DataFrame(np.column_stack( [train_prop, 
                                    val_prop + [0]*(len(train_prop)-len(val_prop)),
                                    test_prop + [0]*(len(train_prop)-len(test_prop))
                                    ]  ),
                    columns=['train', 'val', 'test']).to_csv(f'./data_test_{prop}/set_props.csv')
