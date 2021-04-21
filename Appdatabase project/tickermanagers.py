from abc import ABC, abstractmethod
# # [warning]Using or importing the ABCs from 'collections' instead of from 'collections.abc' is deprecated since Python 3.3
import re
import os
import bs4 as bs
import pandas as pd
import requests
from datetime import date
from datetime import datetime
import os.path as path

import housekeeper
from parsers import WikiToYahooFinance_Parser
from parsers import WikiToTickerKey_Parser

class TickerManager(ABC):

    def __init__(self, tickermanager_id, tickers_list):
        #Variables comunes a todos los tipos de tickermanager.
        self.__tickermanager_id = tickermanager_id
        self.__tickers_list = tickers_list # Lista de diccionarios. Ej:
        # [{ "ticker":"BBVA(raw)", "market":"IBEX35", "hora":"YYYY/MM/DD/HH/MM/SS", "source":"Wikipedia", "tickerYahoofinance":"BBVA.MC", "tickerAlphaVantage":"BBVA.BM(inventado)"}]
        self.__update_date = []
        self.__tickers_config = []
        self.__config_filename = "tickers_config.json"
        self.__dir_list = ['Data', 'Tickers', 'Dummy1']
        self.__upper_stages = 0
        self.__data_housekeeper = housekeeper.instance_class()
    
    @abstractmethod
    def get_raw_tickers(self):
        pass

    @abstractmethod
    def get_parsed_tickers(self):
        pass
    
    def get_config_filename(self):
        return self.__config_filename
    
    def set_config_filename(self, config_filename):
        self.__config_filename = config_filename
        
    def get_dir_list(self):
        return self.__dir_list
    
    def set_dir_list(self, dir_list):
        self.__dir_list = dir_list
    
    def get_upper_stages(self):
        return self.__upper_stages
    
    def set_upper_stages(self, upper_stages):
        self.__upper_stages = dir_list
    
    def get_housekeeper(self):
        return self.__data_housekeeper

    def set_tickers_list(self, tickers_list):
        # Sobreescribe la lista tickers_list
        print("Set tickers_list")
        self.__tickers_list = tickers_list
    
    def get_tickers_list(self):
        # Devuelve la lista tickers_list
        print('Getting tickers_list')
        return self.__tickers_list
    
    def set_selected_tickers(self, selected_tickers_list):
        self.__selected_tickers_list = selected_tickers_list
        
    def get_selected_tickers(self):
        return self.__selected_tickers_list
        
    def set_tickers_config(self, tickers_config):
        # Sobreescribe la lista tickers_config
        print("Set tickers_config")
        self.__tickers_config = tickers_config
    
    def get_tickers_config(self):
        # Devuelve la lista tickers_config
        print('Getting tickers_config')
        return self.__tickers_config
    
    def get_num_tickers(self):
        # Devuelve el número de diccionarios(uno por cada ticker) en la tickers_list
        print("Number of tickers: {}".format(len(self.__tickers_list)))
        return len(self.__tickers_list)

    def set_tickermanager_id(self, tickermanager_id):
        # Actualiza el par-value del diccionario: source. Ej. "source":"Wikipedia"
        print("Set tickermanager_id: {}".format(tickermanager_id))
        self.__tickermanager_id = tickermanager_id
    
    def get_tickermanager_id(self):
        print("TickerManager id is: {}".format(self.__tickermanager_id))
        return self.__tickermanager_id
    
    def set_update_date(self, update_date):
        # Actualiza el par-value del diccionario: "hora":"YYYY/MM/DD/HH/MM/SS"
        print("Set date of update: {}".format(update_date))
        self.__update_date = update_date #FIX. Actualiza diccionario, no nueva variable

    def get_update_date(self):
        # Devuelve el par-value del diccionario: "hora":"YYYY/MM/DD/HH/MM/SS"
        print("Last date of update id is: {}".format(self.__update_date))
        return self.__update_date
    
    def save_tickers_list(self):
        # Guarda la ticker list en JSON en la ruta especificada.
        self.__data_housekeeper.list_dict_to_json(self.get_dir_list(), 
                                           self.get_upper_stages(), 
                                           self.get_config_filename(), 
                                           self.get_tickers_list())
        print("Saved tickers list")
    
    def save_tickers_config(self):
        now = datetime.now()
        ticker_list_metadata_ = [{"timestamp":now.strftime("%d/%m/%Y %H:%M:%S")}]
        tickers_config_ = {'metadata':ticker_list_metadata_, 'data':self.get_tickers_list()}
        self.set_tickers_config(tickers_config_)
        self.__data_housekeeper.list_dict_to_json(self.get_dir_list(), 
                                           self.get_upper_stages(), 
                                           self.get_config_filename(), 
                                           self.get_tickers_config())
        


class TickerManager_Wiki(TickerManager):

    def __init__(self):
        super().__init__(tickermanager_id = "Wiki", tickers_list=[])
        #Variables que sólo sean características del tipo WIKI. Market, url y posiciones en la tabla wiki
        #FIX 
        self.__wiki_metadata = [{'market':'IBEX35','url':'https://es.wikipedia.org/wiki/IBEX_35','pos_table':{'ticker':0, 'company':1,'sector':4,'entry_date':3,'ISIN':5}},
                {'market':'DAX30','url':'https://de.wikipedia.org/wiki/DAX','pos_table':{'ticker':1, 'company':0,'sector':2,'entry_date':5}},
                {'market':'CAC40','url':'https://es.wikipedia.org/wiki/CAC_40','pos_table':{'ticker':2,'company':0, 'sector':1}},
                {'market':'SP500','url':'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies','pos_table':{'ticker':0, 'company':1,'sector':3,'sub_industry':4,'entry_date':6,'CIK':7}}
                ]
        self.__paths = []
        self.__markets = []
        # Instanciar aquí el parser. Para este tickermanger, tener una lista de parsers.
        self.__my_parser_W2YF = WikiToYahooFinance_Parser()
        self.__my_parser_W2TK = WikiToTickerKey_Parser()
        # El código puede ser genérico, 
    
    #Métodos que sólo son característicos del tipo Wiki
    def get_wikitable_from_url(self,wiki_metadata) -> dict:
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
    
    def set_markets(self):
        self.__markets = [metadata['market'] for metadata in self.__wiki_metadata]
    
    def get_markets(self):
        return self.__markets
    
    def get_raw_tickers(self, markets):
        # Descargar los tickers de los markets seleccionados y guardar en la variables raw_ticker_list
        raw_ticker_list = []
        print('Start raw ticker data extraction from Wikipedia ')
        for metadata in (metadata for metadata in self.__wiki_metadata if metadata['market'] in markets):
            print('Getting data from market:{}, site:{}'.format(metadata['market'], metadata['url']))
            #print(i), print(metadata)
            wikitable_data = self.get_wikitable_from_url(metadata)
            raw_ticker_list += wikitable_data
        print('Extraction completed')
        super().set_tickers_list(raw_ticker_list)


    #def get_parsed_tickers(self):
    #    # Actualiza los par-value del diccionario: "tickerYahoofinance":"value", "tickerAlphaVantage":"value"
    #    print('Activation: get_parsed_tickers')
    
    def get_parsed_tickers(self): # Función con la que hay que engancahr
        # Actualiza los par-value del diccionario: "tickerYahoofinance":"value", "tickerAlphaVantage":"value"
        print('Activation: get_parsed_tickers')
        raw_ticker_list = self.get_tickers_list()
        feeder_ticker_list = []
        for raw_ticker in raw_ticker_list:
            feeder_ticker = self.__my_parser_W2TK.feeder_ticker(raw_ticker)
            feeder_ticker = self.__my_parser_W2YF.feeder_ticker(feeder_ticker)
            feeder_ticker_list.append(feeder_ticker)
            
        print('All tickers fed')
        self.set_tickers_list(feeder_ticker_list)
        #return feeder_ticker_list
        
    def compare_tickers_config(self, tickers_new, tickers_config):
        # To set the appropiate flag active(not_active in the tickers if belongs to the market
        tickerKey_list_config = [d['tickerKey'] for d in tickers_config]
        tickerKey_list_new = [d['tickerKey'] for d in tickers_new]
        timestamp_new_values =  tickers_new[0]['timestamp']

        # Loop in tickers_config. Update coincident tickers
        for ticker_config in tickers_config:
            ticker_config['active_flag'] = False # By default false 
            if ticker_config.get('active_type') == 'stock': # Tickers of stock type. 
                if ticker_config['tickerKey'] in tickerKey_list_new: #In case appear in new vales. Update Timestamp and active flag
                    ticker_config['timestamp'] = timestamp_new_values # deepcopy
                    ticker_config['active_flag'] = True # deepcopy
            if ticker_config.get('active_type') == 'ETF':
                ticker_config['active_flag'] = True # Siempre true, no pueden desaparecer. deepcopy

        # Loop in new_tickers. Update new tickers tickers of stock type. Add full ticker.
        for ticker in tickers_new:
            if ticker['tickerKey'] not in tickerKey_list_config:
                tickers_config.append(ticker)
        return tickers_config
    
    def update_market_tickers_config(self, markets_):
        #Step 1 - Download new set of tickers associated to the markets, parse it and save it to tickers_list variable .
        self.get_raw_tickers(markets_)
        self.get_parsed_tickers() #Parse and save it

        #Step 2 - Load previous tickers_config. If not present, then create from scratch
        #Get path to tickers_config file, to check existence.
        file_path_tickers_config =  '.' + self.get_housekeeper().create_file_path_in_nested_dir(self.get_dir_list(), 
                                                                                          self.get_config_filename())
        if not path.isfile(file_path_tickers_config):
            print('No file tickers_config.json detected, creating..')
            self.save_tickers_config()
            print('Created tickers_config.json')
            return
        
        tickers_config = self.get_housekeeper().load_json_to_list(self.get_dir_list(), 
                                                            self.get_config_filename())
        tickers_config = tickers_config['data']

        #Step 3 - Compare new tickers with tickers_config
        new_tickers = self.get_tickers_list() #Tickers obtained in Step2 and saved in tickers_list
        tickers_config = self.compare_tickers_config(new_tickers, tickers_config)
        self.set_tickers_config(tickers_config)
        #Step 4 - Save updated tickers config
        self.save_tickers_config()
        
    def update_tickers_config(self):
        self.set_markets() # Get all the markets from the 
        self.update_market_tickers_config(self.get_markets())