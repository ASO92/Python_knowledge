from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
import os

import housekeeper

import yfinance as yf
import pandas as pd

class DataManager(ABC):
    
    def __init__(self):
        
        self.__myHousekeeper = housekeeper.instance_class()
        self.__config_filename = "tickers_config.json"
        self.__dir_list = ['Data', 'Tickers', 'Dummy1']
        self.__upper_stages = 0
        self.__tickers_config_list = []
        self.__tickers_list = []
        self.__active_tickers_list = []
        self.__selected_tickers_list = []
        self.__timestamp = ''
        self.__markets = []
        self.__last_date_flag = False
    

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
        
    def get_last_date_flag(self):
        return self.__last_date_flag
    
    def set_last_date_flag(self, last_date_flag):
        self.__last_date_flag = last_date_flag
        
    def get_tickers_config(self):
        return self.__tickers_config_list
    
    def set_tickers_config(self, tickers_config_list):
        self.__tickers_config_list = tickers_config_list
    
    def get_tickers(self):
        return self.__tickers_list
    
    def set_tickers(self, tickers_list):
        self.__tickers_list = tickers_list
        
    def get_active_tickers(self):
        return self.__active_tickers_list
    
    def set_active_tickers(self, active_tickers_list):
        self.__active_tickers_list = active_tickers_list
        
    def get_selected_tickers(self):
        return self.__selected_tickers_list
    
    def set_selected_tickers(self, selected_tickers_list):
        self.__selected_tickers_list = selected_tickers_list
    
    def get_timestamp(self):
        return self.__timestamp
    
    def set_timestamp(self, timestamp):
        self.__timestamp = timestamp
    
    def get_markets(self):
        return self.__markets
    
    def set_markets(self, markets):
        self.__markets = markets
    
    def load_tickers_config(self):
        data = self.__myHousekeeper.load_json_to_list(self.__dir_list, self.__config_filename)
        self.set_tickers_config(data)
        
    def save_tickers_config(self):
        #No invocar a esta función sin previamente haber cargado tickers_config. O se sobreescribe tickers_config
        tickers_config = self.get_tickers_config()
        self.__myHousekeeper.list_dict_to_json(self.get_dir_list(), 
                                               self.get_upper_stages(), 
                                               self.get_config_filename(), 
                                               self.get_tickers_config())
    
    def initialize_metadata(self):
        self.load_tickers_config()
        data = self.get_tickers_config()
        self.set_timestamp(data['metadata'][0]['timestamp'])
        self.set_tickers(data['data'])
        
    def initialize_config_tickers(self):
        # Get markets, get active_tickers
        markets = []
        active_tickers_ = []
        self.initialize_metadata()
        data = self.get_tickers()
        for d in data:
            markets.append(d['market'])
            if d['active_type']=='stock' and d['active_flag']:
                active_tickers_.append(d)
            elif d['active_type']=='ETF':
                active_tickers_.append(d)
        self.set_active_tickers(active_tickers_)
        self.set_markets(list(set(markets)))
    
    def api_selected_tickers(self):
        #Se recarga el tickers_config para info actualizada de los tickers.
        self.initialize_config_tickers()
        # Se despliegan los tickers activos en la UI para que el usuario elija qué tickers quiere actualizar el data.
        ticker_list = self.get_tickers()
        self.set_selected_tickers(ticker_list[0:3])
        
        #return self.get_active_tickers() #TODO
    
    def update_timeseries_download_date(self, selected_tickers_to_update):
        config_ticker_list = self.get_tickers_config()
        today = date.today()
        # LAs fechas se guardan en formato %m-%d-%Y
        [t.update({'data_update':today.strftime("%m-%d-%Y")}) for t in config_ticker_list['data'] if t in selected_tickers_to_update]
        self.set_tickers_config(config_ticker_list)
        self.save_tickers_config()
         
    def load_ticker_data(self, file_name):
        return self.__myHousekeeper.csv_to_df(self.__dir_list,
                                              file_name)
    
    def save_ticker_data(self, file_name, data):
        self.__myHousekeeper.df_to_csv(self.__dir_list,
                                       self.__upper_stages, file_name, data)
        

class DataManager_YahooFinance(DataManager):
    
    def __init__(self):
        super().__init__()
        
    
    def download_ticker_data_from_scratch(self, ticker, ticker_key):
        print('Downloading from scratch historic data of: ' + ticker)
        data_csv = yf.download(ticker)
        data_csv.insert(loc=0, column='Date', value=pd.to_datetime(data_csv.index, errors='coerce'))
        data_csv['Date'] = [time.date() for time in data_csv['Date']]
        data_csv.reset_index(drop=True, inplace=True)
        self.save_ticker_data(ticker_key,data_csv )
        return data_csv
    
    def download_ticker_data_from_last_date(self, ticker, ticker_key, start_date):
        print('Updating historic data of: ' + ticker)
        # 1. Descargar datos desde la ultima fecha
        data_csv = yf.download(ticker, start = start_date)
        data_csv.insert(loc=0, column='Date', value=pd.to_datetime(data_csv.index, errors='coerce'))
        data_csv['Date'] = [time.date() for time in data_csv['Date']]
        print('Downloaded(sessions)', len(data_csv))
        # 2. Cargar el csv
        data_csv_local = DM_YF.load_ticker_data(ticker_key)
        # 3. Apendear los datos que faltan, resetear el index y esta será la nueva varaible data_csv
        data_csv = pd.concat([data_csv_local, data_csv], ignore_index = True)
        data_csv.reset_index(drop=True, inplace=True)
        data_csv.drop(data_csv.columns[0], axis = 1, inplace = True)
        # 4. Guardar los datos sobreescribiendo el archivo anterior
        self.save_ticker_data(ticker_key, data_csv)
        #return data_csv
    
    def last_date_download(self, ticker_dict):
        # Local variables
        last_date_str_ = ticker_dict['data_update']
        ticker_key_ = ticker_dict['tickerKey']
        ticker = ticker_dict['feeds']['ticker']
        # 3 casos: A) last_date is None -> from scratch, B) last >= today -> no hay descarga C) start < today (else) -> download_ticker_data_from_last_date
        if last_date_str_ is None: # Aquí va un download_from_scratch
            print(ticker + " is not found in database, adding ----")
            #data_csv = yf.download(ticker) # Aquí va un download_from_scratch
            self.download_ticker_data_from_scratch(ticker, ticker_key_)
            return
        now = datetime.now()
        last_date = datetime.strptime(last_date_str_, '%m-%d-%Y')
        delta = now - last_date
        start_date = last_date + timedelta(days=+1)
        if delta.days <= 0: # Aquí no hay download
            print('Data of ', ticker_key_ ,'is already updated')
            return
        else: # Función download_ticker_data_from_last_date
            self.download_ticker_data_from_last_date(ticker, ticker_key_, start_date)
            delta = now - start_date
            print('Downloaded(days): ', delta.days)
            #return data_csv
    
    
    def timeseries_download_manager(self, ticker_dict):
        if self.get_last_date_flag(): # From last date
            print('Download ', ticker_dict['tickerKey'],' from last updated_date')
            self.last_date_download(ticker_dict)
        else: # From scratch
            print('Download', ticker_dict['tickerKey'],' from scratch')
            self.download_ticker_data_from_scratch(ticker_dict['feeds']['ticker'],ticker_dict['tickerKey'])
        
    
    def download_selected_tickers(self):
        # Se almacenan los tickers que van a se actualizados y se guarda la fecha de actualización en el ticker_config. 
        # 1.- Almacenar selected_Tickers from user selection and a default option.
        #selected_tickers_list = self.api_active_tickers()
        self.api_selected_tickers()
        #2.- Establecer el tipo de descarga: last_date(True) / from scratch(False, default) 
        self.set_last_date_flag(False)
        #3.- Descargar los selected_tickers. Enganchar con timeseries_download_manager
        [self.timeseries_download_manager(t) for t in self.get_selected_tickers()]
        # 4.- Actualizar data_update en tickers_config de los tickers descargados
        self.update_timeseries_download_date(self.get_selected_tickers())
    
    
    def download_market_data(self, markets, _last_date_flag = False): #TODO: especificar el subconjunto en selected tickers. Para que se actualice la fecha data_update
        print('Download market ticker')
        #1.- Almacenar en selected_ticker los tickers correspondientes a un market
        #Se recarga el tickers_config para info actualizada de los tickers.
        self.initialize_config_tickers()
        # Se despliegan los tickers activos en la UI para que el usuario elija qué tickers quiere actualizar el data.
        active_ticker_list = self.get_active_tickers()
        ticker_list = [t for t in active_ticker_list if t['market'] in markets]
        self.set_selected_tickers(ticker_list)
        #2.- Establecer el tipo de descarga: last_date(True) / from scratch(False, default) 
        self.set_last_date_flag(_last_date_flag)
        #3.- Descargar los selected_tickers. Enganchar con timeseries_download_manager
        #tickers = self.get_active_tickers()
        #[DM_YF.download_ticker_data_from_scratch(t['feeds']['ticker'], t['tickerKey']) for t in tickers if t['market'] in markets]
        [self.timeseries_download_manager(t) for t in self.get_selected_tickers()]
        # 4.- Actualizar data_update en tickers_config de los tickers descargados
        self.update_timeseries_download_date(self.get_selected_tickers())
        
    def download_all_markets(self):
        print('Download ALL MARKETS')
        self.download_market_data(self.get_markets())
    