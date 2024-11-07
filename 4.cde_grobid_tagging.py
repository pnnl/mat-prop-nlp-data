import pickle
import nltk
from tqdm import tqdm
from chemdataextractor.doc import Sentence
import pandas as pd
from grobid_utils import processMap
import os
from collections import Counter

# java -jar build/libs/grobid-quantities-0.7.1-SNAPSHOT-onejar.jar server resources/config/config.yml

from grobid_quantities.quantities import QuantitiesClient
client = QuantitiesClient(apiBase='http://localhost:8060/service/')


# 1
def get_prop_sents(sent_list, prop_name):
    mp_sents=[]
    for i in tqdm(sent_list):

        sent_text = nltk.sent_tokenize(i)

        for st in sent_text:
            if prop_name in st:
                mp_sents.append(st)

    return mp_sents

#2
def sents_with_chem(sent_list, num_chems):
    
    cem_sents = []
    
    for i in tqdm(sent_list):
        if len(Sentence(i).cems) == num_chems:
            cem_sents.append(i)
    
    return cem_sents


def grobid_extract(cem_sents):
    
    extracted = []
    for text in tqdm(cem_sents):

        try:
            data = client.process_text(text)
        except:
            print("failed for ", text)
            continue

        loaded = data[1]
        if loaded !=None:
            if 'measurements' not in loaded:
                continue


            if len(loaded['measurements'])==1:


                m = loaded['measurements'][0]

                
                if 'quantityBase' in loaded['measurements'][0]:

                    if 'rawUnit' in m['quantityBase']:
                        value = m['quantityBase']['rawValue']
                        unit = m['quantityBase']['rawUnit']['name']

                        unit_start = m['quantityBase']['rawUnit']['offsetStart']
                        unit_end = m['quantityBase']['rawUnit']['offsetEnd']

                        value_start = m['quantityBase']['offsetStart']
                        value_end = m['quantityBase']['offsetEnd']

                        extracted.append([text, value,value_start, value_end, unit, unit_start, unit_end ])


                elif 'quantity' in loaded['measurements'][0]:

                    if 'rawUnit' in m['quantity']:

                        value = m['quantity']['rawValue']

                        # data[1]['measurements'][0]['quantity']['rawUnit']

                        unit = m['quantity']['rawUnit']['name']

                        unit_start = m['quantity']['rawUnit']['offsetStart']
                        unit_end = m['quantity']['rawUnit']['offsetEnd']

                        value_start = m['quantity']['offsetStart']
                        value_end = m['quantity']['offsetEnd']


                        extracted.append([text, value,value_start, value_end, unit, unit_start, unit_end ])


    return extracted


def grobid_extract_wo_unit(cem_sents):
    
    extracted = []
    for text in tqdm(cem_sents):

        data = client.process_text(text)

        loaded = data[1]
        if loaded !=None:
            if 'measurements' not in loaded:
                continue


            if len(loaded['measurements'])==1:


                m = loaded['measurements'][0]


                if 'quantityBase' in loaded['measurements'][0]:

                        value = m['quantityBase']['rawValue']

                        value_start = m['quantityBase']['offsetStart']
                        value_end = m['quantityBase']['offsetEnd']

                        extracted.append([text, value,value_start, value_end])


                elif 'quantity' in loaded['measurements'][0]:

                        value = m['quantity']['rawValue']

                        value_start = m['quantity']['offsetStart']
                        value_end = m['quantity']['offsetEnd']


                        extracted.append([text, value, value_start, value_end])


    return extracted


def run_extraction(data_file='./bp_data.csv', prop='boiling point'):
    
    mp = pd.read_csv(data_file)
    
    mp = mp.drop_duplicates(subset=['text'])
    mp_sents = get_prop_sents(mp.text.values, prop)
    cem_sents = sents_with_chem(mp_sents, 1)
    extracted = grobid_extract(cem_sents)
    extracted = pd.DataFrame(extracted, columns=['text', 'value','value_start',
                                                 'value_end', 'unit', 'unit_start', 'unit_end'])
    return extracted


def run_extraction_wounit(data_file='./bp_data.csv', prop='boiling point'):
    
    mp = pd.read_csv(data_file)
    
    mp = mp.drop_duplicates(subset=['text'])
    mp_sents = get_prop_sents(mp.text.values, prop)
    cem_sents = sents_with_chem(mp_sents, 1)
    extracted = grobid_extract_wo_unit(cem_sents)
    extracted = pd.DataFrame(extracted, columns=['text', 'value','value_start', 'value_end'])
    return extracted


def get_chem_data(extracted):
    for i in extracted.index:
        cemd = Sentence(extracted.loc[i, 'text'])
        extracted.loc[i, 'chem'] = cemd.cems[0].text
        extracted.loc[i, 'chem_start'] = cemd.cems[0].start
        extracted.loc[i, 'chem_end'] = cemd.cems[0].end
        
    extracted.chem_start = extracted.chem_start.astype(int)         
    extracted.chem_end = extracted.chem_end.astype(int) 
    return extracted

def unit_select_and_save(extracted, keep_units, prop_name, save_path):
    
    extracted = extracted[extracted.unit.isin(keep_units)]
    extracted.reset_index(drop=True, inplace=True)

    extracted = get_chem_data(extracted)
    # pd.set_option('display.max_colwidth', None)
    # extracted = extracted[extracted.unit != '°C/min']
    extracted.to_csv(f"{save_path}/{prop_name}_ner_word.csv", index=False)


import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--prop', type=str, default='absorption', help='')
    parser.add_argument('--data_path', type=str, default='/data/', help='where prop_data.csv is stored')
    parser.add_argument('--auto_filter', type=bool, default=True, help='automatically select keep-units?')
    args = parser.parse_args()

    a = os.listdir("./")
    print([i for i in a if i.endswith('_data.csv')])

    extracted = run_extraction(f'{args.data_path}/{args.prop}_data.csv', args.prop)
    extracted.to_csv(f"{args.data_path}/{args.prop}_before_clean.csv", index=False)

    # Have to manually inspect and find which units to keep
    # Keep this in keep_units

    if args.auto_filter:
        
        c = Counter(extracted.unit.values(10))
        keep_units = [i[0] for i in c]
        # keep the most common units
        # keep_units = []
        unit_select_and_save(extracted, keep_units, args.prop, args.data_path)
