import pandas as pd
from chemdataextractor.doc import Sentence
import numpy as np
from tqdm import tqdm
from utils import run_extraction
from utils import get_chem_data
tqdm.pandas()

create_solubility = False

if create_solubility:
    
    # solubility ner
    sol = pd.read_csv("bert_main.csv")
    sol['word'] = sol['Word']
    sol['tag'] = sol['Tag']
    # sol['text_id'] = sol['Sentence #']

    i = 0
    for n, g in sol.groupby('Sentence #'):
        sol.loc[g.index, 'text_id'] = f'sol_{i}'
        i+=1

    sol["FaultTypeChanged"] = sol["Tag"].shift(1, fill_value=sol["Tag"].head(1)) != sol["Tag"]
    transitioned = sol.index[sol['FaultTypeChanged'] == True].tolist()

    sol['NewTag'] = 'O'

    for i in range(0, len(transitioned)-1):
        start_loc = transitioned[i]
        end_loc = transitioned[i+1]

        diff = end_loc - start_loc
        tmp = sol.loc[start_loc:end_loc-1]


        if diff > 1:
            if 'O' not in tmp.Tag.values:
                sol.loc[start_loc, 'NewTag'] = 'B-sol-'+sol.loc[start_loc, 'Tag']
                for j in range(start_loc+1, end_loc):
                    sol.loc[j, 'NewTag'] = 'I-sol-'+sol.loc[j, 'Tag']


        elif diff == 1:
            if 'O' not in tmp.Tag.values:
                sol.loc[start_loc, 'NewTag'] = 'B-sol-'+sol.loc[start_loc, 'Tag']

    sol['tag'] = sol['NewTag']
    sol.to_csv("sol_ner_bert.csv", index=False)


def clean_text(text):
    return ' '.join(Sentence(text).raw_tokens)

props = ['bp', 'mp', 'capacitance', 'permittivity',
              'conductivity', 'mass', 
              'charge','density','absorption']


prop_to_actual = {'bp' : 'boiling point',
                  'mp' : 'melting point',
                  'capacitance' : 'capacitance',
                  'permittivity' : 'permittivity',
                  'conductivity' : 'conductivity',
                  'mass' : 'mass', 
                  'charge' : 'charge',
                  'density' : 'density',
                  'absorption' : 'absorption'}


if __name__ == "__main__":


    for prop in  props:
    
        df = pd.read_csv(f"./data/matprops/{prop}/{prop}_ner_word.csv")
        df['new_text'] = df.text.progress_map(clean_text)
        df.to_csv(f"./data/matprops/{prop}/{prop}_ner_word_cleantext.csv", index=False)
    


    for prop in props:
        
        dft = run_extraction(data_file=f"./data/matprops/{prop}/{prop}_ner_word_cleantext.csv",\
                            prop=prop_to_actual[prop],
                            text_columns='new_text')

        chems = get_chem_data(dft)
        chems['text_id'] = [f'{prop}_'+str(i) for i in chems.index]
        dfb = get_bert_data(chems, prop)
        dfb.to_csv(f"./data/matprops/{prop}/{prop}_ner_bert.csv", index=False)
