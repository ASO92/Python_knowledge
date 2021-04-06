

import re
import os
import bs4 as bs
import pandas as pd
import requests
from datetime import date
from datetime import datetime

wiki_metadata = [{'market':'IBEX35','url':'https://es.wikipedia.org/wiki/IBEX_35','pos_table':{'ticker':0, 'company':1,'sector':4,'entry_date':3,'ISIN':5}},
                {'market':'DAX30','url':'https://de.wikipedia.org/wiki/DAX','pos_table':{'ticker':1, 'company':0,'sector':2,'entry_date':5}},
                {'market':'CAC40','url':'https://es.wikipedia.org/wiki/CAC_40','pos_table':{'ticker':2,'company':0, 'sector':1}},
                {'market':'SP500','url':'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies','pos_table':{'ticker':0, 'company':1,'sector':3,'sub_industry':4,'entry_date':6,'CIK':7}}
                ]


def get_wikitable_from_url(wiki_metadata) -> dict:
    today = date.today()
    resp = requests.get(wiki_metadata['url'])
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    wikitable_data = []
    for row in table.findAll('tr')[1:]:
        wikitable_dict = {}
        for key in wiki_metadata['pos_table']:
            data = row.findAll('td')[wiki_metadata['pos_table'][key]].text
            wikitable_dict[key] = data.strip()
        wikitable_dict['market'] = wiki_metadata['market']
        wikitable_dict['active_type'] = 'stock'
        wikitable_dict['active_flag'] = True
        wikitable_dict['timestamp'] = today.strftime("%d-%m-%Y")
        wikitable_dict['data_update'] = None
        wikitable_data.append(wikitable_dict)
    wikitable_data.append({'ticker': wiki_metadata['market'] , 
                           'market': wiki_metadata['market'], 
                           'timestamp':today.strftime("%d-%m-%Y"),
                           'data_update': '-', 
                           'active_type':'ETF'})
    return wikitable_data

def get_raw_ticker_list():    
    raw_ticker_list = []
    print('Start raw ticker data extraction from Wikipedia ')
    for i,metadata in enumerate(wiki_metadata):
        print('Getting data from market:{}, site:{}'.format(metadata['market'], metadata['url']))
        wikitable_data = get_wikitable_from_url(metadata)
        raw_ticker_list += wikitable_data
    print('Extraction completed')
    return raw_ticker_list




