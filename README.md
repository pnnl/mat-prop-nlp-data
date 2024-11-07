# Material Property Data Extraction

This code provides the data downloading and processing scripts to support data creation for the paper "Extracting Material Property Measurements from Scientific Literature with Limited Annotations".

# Download data
First, the text data has to be downloaded from pubchem.
https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/txt/

# Preprocessing data
Use these scripts to extract materials property data and create property datasets.

1. extract.py
2. getprops.py
3. combine_datasets.py

-----
# Chemical entity tagging
Use these scripts to create named-entity-tagged datasets.

4. cde_grobid_tagging.py
5. ner_data.py
6. create_data.py


# DISCLAIMER
This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or
represents that its use would not infringe privately owned rights.
 
Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
 
                 PACIFIC NORTHWEST NATIONAL LABORATORY
                              operated by
                                BATTELLE
                                for the
                   UNITED STATES DEPARTMENT OF ENERGY
                    under Contract DE-AC05-76RL01830
