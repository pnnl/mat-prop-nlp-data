from datetime import datetime
import numpy as np
import nltk
import os
import time
from pathlib import Path
import pickle
from tqdm import tqdm
import re
import json
from nltk.tokenize import sent_tokenize, word_tokenize
import pandas as pd



# FIND PATHS
def get_pmc_paths(fdir, query_wrd='boiling', save_folder='pmc_comm'):

    Path(save_folder).mkdir(parents=True, exist_ok=True)

    folders = os.listdir(fdir)

    pathlist=[]
    count_id=0
    st = time.time()
    for folder in tqdm(folders):

        # lists all the folders in the oa collection
        cdir = os.path.join(fdir,folder)
        try:
            files = os.listdir(cdir)
        except:
            continue

        # finds all the .txt files
        files = [fs for fs in files if fs.endswith(".txt")]

        for file in files:
            # reads the file content
            fpath = os.path.join(cdir, file )
            f = open(fpath, "r")
            try:
                f = f.readlines()
            except:
                continue

            # for each line in the file,
            for sraw in f:
                # tokenize into sentences using nltk
                if query_wrd in sraw.lower():
                    pathlist.append(fpath)
                    break


        if len(pathlist) > 100:
            pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_{count_id}.csv",index=False)
            pathlist=[]
            count_id+=1

    pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_last.csv",index=False)
    end = time.time()

    print("time: ", (end-st)/3600) 
    
    

def get_pmc_paths_split(fdir, query_wrd='boiling', save_folder='pmc_comm'):

    Path(save_folder).mkdir(parents=True, exist_ok=True)

    folders = os.listdir(fdir)

    pathlist=[]
    count_id=0
    st = time.time()
    for folder in tqdm(folders):

        # lists all the folders in the oa collection
        cdir = os.path.join(fdir,folder)
        try:
            files = os.listdir(cdir)
        except:
            continue

        files = [fs for fs in files if fs.endswith(".txt")]
        for file in files:
            fpath = os.path.join(cdir, file )
            f = open(fpath, "r")
            try:
                f = f.readlines()
            except:
                continue

            # for each line in the file,
            for sraw in f:
                # tokenize into sentences using nltk
                if query_wrd in sraw.lower().split(' '):
                    pathlist.append(fpath)
                    break


        if len(pathlist) > 100:
            pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_{count_id}.csv",index=False)
            pathlist=[]
            count_id+=1

    pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_last.csv",index=False)
    end = time.time()

    print("time: ", (end-st)/3600) 
    
    
# FIND SENTENCES
def get_sents(files, query_wrd='boiling point'):
    sents = []
    for file in files:
        # reads the file content
        f = open(file, "r")
        f = f.readlines()

        # for each line in the file,
        for sraw in f:
            # tokenize into sentences using nltk
            if query_wrd in sraw:
                sents.append([sraw, file])
    return sents


# FIND SENTENCES
def get_sents_toks(files, query_wrd='boiling point'):
    sents = []
    for file in files:
        # reads the file content
        f = open(file, "r")
        f = f.readlines()

        # for each line in the file,
        for sraw in f:
            words = nltk.word_tokenize(sraw.lower())
            # tokenize into sentences using nltk
            if query_wrd in words:
                sents.append([sraw, file])
    return sents



# COLLECT SENTENCES
def collect_sents(path_file_dir, sents_file_name, query_wrd):
    
    files = os.listdir(f"{path_file_dir}/")
    files = [f for f in files if f.endswith(".csv")]

    sents =[]
    for f in tqdm(files):
        df = pd.read_csv(f"{path_file_dir}/{f}")
        files = df.paths.values
        sents.extend(get_sents(files, query_wrd))


    with open(f"{sents_file_name}.pkl", "wb") as f:
        pickle.dump(sents, f)
        
        
def collect_sents_toks(path_file_dir, sents_file_name, query_wrd):
    
    files = os.listdir(f"{path_file_dir}/")
    files = [f for f in files if f.endswith(".csv")]

    sents =[]
    for f in tqdm(files):
        df = pd.read_csv(f"{path_file_dir}/{f}")
        files = df.paths.values
        sents.extend(get_sents_toks(files, query_wrd))


    with open(f"{sents_file_name}.pkl", "wb") as f:
        pickle.dump(sents, f)
        



def get_s2orc_paths(query_wrd='boiling', save_folder='s2orc'):
    
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    
    digit = r'((-?\d+\.?\d*) | (-?\d+\.?\d*)| (-?\d+\.?\d*)| (-?\d+\.?\d*) |\
    ((~|-|<)?\d+\.?\d*)| ((~|-)?\d+\.?\d*))\s*(V)'


    redox_files=[]
    count=0
    for batch_no in range(0,95):

        # batch_no = 0
        path = f'./s2orc/full/20200705v1/full/pdf_parses/pdf_parses_{batch_no}.jsonl'
        with open(path) as f_pdf:
            for line in tqdm(f_pdf):
                pdf_parse_dict = json.loads(line)

                pid = pdf_parse_dict["paper_id"]
                abst = pdf_parse_dict['abstract']
                body = pdf_parse_dict['body_text']

                text=''
                for a in abst:
                    text+=a['text']
                for b in body:
                    text+=b['text']

                if query_wrd in text.lower():

                    digit_found = re.findall(digit, text)

                    if len(digit_found)>0:
                        redox_files.append([text, batch_no, pid])

                        if len(redox_files)%10000==0:
    #                         print("A")

                            df = pd.DataFrame(redox_files, columns=['text','batch_no', 'pid'])
                            df.to_csv(f"{save_folder}/s2rc{count}.csv", index=False)
                            count+=1

    df = pd.DataFrame(redox_files, columns=['text', 'batch_no', 'pid'])
    df.to_csv(f"{save_folder}/s2rc_final.csv", index=False)
    
    
def get_s2orc_paths_split(query_wrd='boiling', save_folder='s2orc'):
    
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    
    digit = r'((-?\d+\.?\d*) | (-?\d+\.?\d*)| (-?\d+\.?\d*)| (-?\d+\.?\d*) |\
    ((~|-|<)?\d+\.?\d*)| ((~|-)?\d+\.?\d*))\s*(V)'


    redox_files=[]
    count=0
    for batch_no in range(0,95):

        # batch_no = 0
        path = f'./s2orc/full/20200705v1/full/pdf_parses/pdf_parses_{batch_no}.jsonl'
        with open(path) as f_pdf:
            for line in tqdm(f_pdf):
                pdf_parse_dict = json.loads(line)

                pid = pdf_parse_dict["paper_id"]
                abst = pdf_parse_dict['abstract']
                body = pdf_parse_dict['body_text']

                text=''
                for a in abst:
                    text+=a['text']
                for b in body:
                    text+=b['text']

                if query_wrd in text.lower().split(' '):

                    digit_found = re.findall(digit, text)

                    if len(digit_found)>0:
                        redox_files.append([text, batch_no, pid])

                        if len(redox_files)%10000==0:
    #                         print("A")

                            df = pd.DataFrame(redox_files, columns=['text','batch_no', 'pid'])
                            df.to_csv(f"{save_folder}/s2rc{count}.csv", index=False)
                            count+=1

    df = pd.DataFrame(redox_files, columns=['text', 'batch_no', 'pid'])
    df.to_csv(f"{save_folder}/s2rc_final.csv", index=False)
    
    


# FIND PATHS
def get_pmc_paths(fdir, query_wrd='boiling', save_folder='pmc_comm'):

    Path(save_folder).mkdir(parents=True, exist_ok=True)
    folders = os.listdir(fdir)

    pathlist=[]
    count_id=0
    st = time.time()
    for folder in tqdm(folders):

        # lists all the folders in the oa collection
        cdir = os.path.join(fdir,folder)
        try:
            files = os.listdir(cdir)
        except:
            continue

        # finds all the .txt files
        files = [fs for fs in files if fs.endswith(".txt")]
        # for each file in files,
        for file in files:
            # reads the file content
            fpath = os.path.join(cdir, file )
            f = open(fpath, "r")
            try:
                f = f.readlines()
            except:
                continue

            # for each line in the file,
            for sraw in f:
                # tokenize into sentences using nltk
                if query_wrd in sraw.lower():
                    pathlist.append(fpath)
                    break


        if len(pathlist) > 100:
            pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_{count_id}.csv",index=False)
            pathlist=[]
            count_id+=1

    pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_last.csv",index=False)
    end = time.time()

    print("time: ", (end-st)/3600) 
    
    

def get_pmc_paths_split(fdir, query_wrd='boiling', save_folder='pmc_comm'):

    Path(save_folder).mkdir(parents=True, exist_ok=True)
    folders = os.listdir(fdir)

    pathlist=[]
    count_id=0
    st = time.time()
    for folder in tqdm(folders):

        # lists all the folders in the oa collection
        cdir = os.path.join(fdir,folder)
        try:
            files = os.listdir(cdir)
        except:
            continue

        # finds all the .txt files
        files = [fs for fs in files if fs.endswith(".txt")]

        for file in files:
            fpath = os.path.join(cdir, file )
            f = open(fpath, "r")
            try:
                f = f.readlines()
            except:
                continue

            # for each line in the file,
            for sraw in f:
                # tokenize into sentences using nltk
                if query_wrd in sraw.lower().split(' '):
    #                 print(fpath, ic)
                    #fwrite.write(fpath+'\n')
                    pathlist.append(fpath)
                    break


        if len(pathlist) > 100:
            pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_{count_id}.csv",index=False)
            pathlist=[]
            count_id+=1

    pd.DataFrame(pathlist, columns=["paths"]).to_csv(f"{save_folder}/paths_last.csv",index=False)
    end = time.time()

    print("time: ", (end-st)/3600) 
    
    

    
# FIND SENTENCES
def get_sents(files, query_wrd='boiling point'):
    sents = []
    for file in files:
        # reads the file content
        f = open(file, "r")
        f = f.readlines()

        # for each line in the file,
        for sraw in f:
            # tokenize into sentences using nltk
            if query_wrd in sraw:
                sents.append([sraw, file])
    return sents


# FIND SENTENCES
def get_sents_toks(files, query_wrd='boiling point'):
    sents = []
    for file in files:
        # reads the file content
        f = open(file, "r")
        f = f.readlines()

        # for each line in the file,
        for sraw in f:
            words = nltk.word_tokenize(sraw.lower())
            # tokenize into sentences using nltk
            if query_wrd in words:
                sents.append([sraw, file])
    return sents



# COLLECT SENTENCES
def collect_sents(path_file_dir, sents_file_name, query_wrd):
    
    files = os.listdir(f"{path_file_dir}/")
    files = [f for f in files if f.endswith(".csv")]

    sents =[]
    for f in tqdm(files):
        df = pd.read_csv(f"{path_file_dir}/{f}")
        files = df.paths.values
        sents.extend(get_sents(files, query_wrd))


    with open(f"{sents_file_name}.pkl", "wb") as f:
        pickle.dump(sents, f)
        
        
def collect_sents_toks(path_file_dir, sents_file_name, query_wrd):
    
    files = os.listdir(f"{path_file_dir}/")
    files = [f for f in files if f.endswith(".csv")]

    sents =[]
    for f in tqdm(files):
        df = pd.read_csv(f"{path_file_dir}/{f}")
        files = df.paths.values
        sents.extend(get_sents_toks(files, query_wrd))


    with open(f"{sents_file_name}.pkl", "wb") as f:
        pickle.dump(sents, f)
        


def get_s2orc_paths(query_wrd='boiling', save_folder='s2orc'):
    
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    
    digit = r'((-?\d+\.?\d*) | (-?\d+\.?\d*)| (-?\d+\.?\d*)| (-?\d+\.?\d*) |\
    ((~|-|<)?\d+\.?\d*)| ((~|-)?\d+\.?\d*))\s*(V)'


    redox_files=[]
    count=0
    for batch_no in range(0,95):

        # batch_no = 0
        path = f'./s2orc/full/20200705v1/full/pdf_parses/pdf_parses_{batch_no}.jsonl'
        with open(path) as f_pdf:
            for line in tqdm(f_pdf):
                pdf_parse_dict = json.loads(line)

                pid = pdf_parse_dict["paper_id"]
                abst = pdf_parse_dict['abstract']
                body = pdf_parse_dict['body_text']

                text=''
                for a in abst:
                    text+=a['text']
                for b in body:
                    text+=b['text']

                if query_wrd in text.lower():

                    digit_found = re.findall(digit, text)

                    if len(digit_found)>0:
                        redox_files.append([text, batch_no, pid])

                        if len(redox_files)%10000==0:
                            df = pd.DataFrame(redox_files, columns=['text','batch_no', 'pid'])
                            df.to_csv(f"{save_folder}/s2rc{count}.csv", index=False)
                            count+=1

    df = pd.DataFrame(redox_files, columns=['text', 'batch_no', 'pid'])
    df.to_csv(f"{save_folder}/s2rc_final.csv", index=False)
    
    
 
def get_s2orc_paths_split(query_wrd='boiling', save_folder='s2orc'):
    
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    
    digit = r'((-?\d+\.?\d*) | (-?\d+\.?\d*)| (-?\d+\.?\d*)| (-?\d+\.?\d*) |\
    ((~|-|<)?\d+\.?\d*)| ((~|-)?\d+\.?\d*))\s*(V)'


    redox_files=[]
    count=0
    for batch_no in range(0,95):

        path = f'./s2orc/full/20200705v1/full/pdf_parses/pdf_parses_{batch_no}.jsonl'
        with open(path) as f_pdf:
            for line in tqdm(f_pdf):
                pdf_parse_dict = json.loads(line)

                pid = pdf_parse_dict["paper_id"]
                abst = pdf_parse_dict['abstract']
                body = pdf_parse_dict['body_text']

                text=''
                for a in abst:
                    text+=a['text']
                for b in body:
                    text+=b['text']

                if query_wrd in text.lower().split(' '):

                    digit_found = re.findall(digit, text)

                    if len(digit_found)>0:
                        redox_files.append([text, batch_no, pid])

                        if len(redox_files)%10000==0:
                            df = pd.DataFrame(redox_files, columns=['text','batch_no', 'pid'])
                            df.to_csv(f"{save_folder}/s2rc{count}.csv", index=False)
                            count+=1

    df = pd.DataFrame(redox_files, columns=['text', 'batch_no', 'pid'])
    df.to_csv(f"{save_folder}/s2rc_final.csv", index=False)
    



def get_bert_data(df, prop_name):
    

    bert_data = []
    for irow in tqdm(df.index):

        cde = Sentence(df.loc[irow, 'text'])
        toks = cde.tokens
        pos = cde.pos_tags

        res=[]
        for tok in toks:
            text, st, en = tok.text, tok.start, tok.end
            res.append([tok.text, tok.start, tok.end])

        res = pd.DataFrame(res, columns=['word', 'st', 'en'])

        res['tag'] = np.nan

        text_id = df.loc[irow, 'text_id']
        chem_st = df.loc[irow, 'chem_start']
        chem_en = df.loc[irow, 'chem_end']
        value_st = df.loc[irow, 'value_start']
        value_en = df.loc[irow, 'value_end']
        unit_st = df.loc[irow, 'unit_start']
        unit_en = df.loc[irow, 'unit_end']
        chem = df.loc[irow, 'chem']
        value = df.loc[irow, 'value']
        unit = df.loc[irow, 'unit']

        
        try:
            n_chem_toks = (res[res.en == chem_en].index  - res[res.st == chem_st].index).item()+1
            n_value_toks = (res[res.en == value_en].index  - res[res.st == value_st].index).item()+1
            n_unit_toks = (res[res.en == unit_en].index  - res[res.st == unit_st].index).item()+1

        except:
            continue
        
        for i in res.index:
            text, st, en = res.loc[i,'word'], res.loc[i, 'st'], res.loc[i, 'en']

            if st == chem_st:
                res.loc[i, 'tag'] = f'B-{prop_name}-CHEM'
                for j in range(1, n_chem_toks):
                    res.loc[i+j, 'tag'] = f'I-{prop_name}-CHEM'

            if st == value_st:
                res.loc[i, 'tag'] = f'B-{prop_name}-VALUE'
                for j in range(1, n_value_toks):
                    res.loc[i+j, 'tag'] = f'I-{prop_name}-VALUE'

            if st == unit_st:
                res.loc[i, 'tag'] = f'B-{prop_name}-UNIT'
                for j in range(1, n_unit_toks):
                    res.loc[i+j, 'tag'] = f'I-{prop_name}-UNIT'




        res['tag'] = res['tag'].fillna('O')
        res['POS'] = pos
        res['text_id'] = text_id
        bert_data.append(res)

    bert_data = pd.concat(bert_data)

    return bert_data

    
