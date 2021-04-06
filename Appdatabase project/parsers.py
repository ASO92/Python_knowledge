class BaseParser():
    
    def __init__(self, source, target):
        self.__source = source
        self.__target = target
        self.__exceptions ={('Wiki','YahooFinance'):{'IBEX35': '^IBEX','CAC40': '^FCHI','DAX30': '^GDAXI', 'SP500':'^GSPC'},
                            ('Wiki', 'Ticker_Key'):{'IBEX35': '^IBEX.TT','CAC40': '^FCHI.TT','DAX30': '^GDAXI.TT', 'SP500':'^GSPC.TT'},
                            ('Wiki','AlphaVantage'):{}}
        
    def get_exceptions(self):
        return self.__exceptions
    
    def set_source(self, source):
        print("Set ticker source in parser")
        self.__source = source
    
    def get_source(self):
        return self.__source
    
    def set_target(self, target):
        print("Set ticker target in parser")
        self.__target = target
    
    def get_target(self):
        return self.__target
       
    def parse(self, name, prefix, suffix, excedent_string):
        key_tuple = (self.__source,self.__target) # Tuple key to select Source-Target exceptions
        exception_dict = self.__exceptions.get(key_tuple, {}) #FIX inform not properly loaded. Mirar longitud del dict.

        if name in exception_dict:
            return exception_dict.get(name, name) 
        
        name = name.replace(excedent_string,'')
        return prefix + name + suffix
    
    def parse_markets(self, raw_ticker, parser_keys): # Feeds tickers for Yahoo finance downloader based on market. Uses parse from BaseParser
        try:
            return self.parse(raw_ticker['ticker'], 
                     parser_keys[('market', raw_ticker['market'])]['prefix'],
                     parser_keys[('market', raw_ticker['market'])]['suffix'],
                     parser_keys[('market', raw_ticker['market'])]['excedent_string'])
        except:
            return raw_ticker['ticker']
     
    
class WikiToYahooFinance_Parser(BaseParser):
    
    def __init__(self):
        super().__init__(source='Wiki', target='YahooFinance')
        
        self.__parser_keys_feed = {('market','IBEX35'):{'prefix':'','suffix':'.MC','excedent_string':''},
                              ('market','CAC40'):{'prefix':'','suffix':'.PA','excedent_string':'Euronext: '},
                              ('market','DAX30'):{'prefix':'','suffix':'.DE', 'excedent_string':''},
                              ('market','SP500'):{'prefix':'','suffix':'', 'excedent_string':''}}
        
        
    def feeder_ticker(self,feed_ticker): # Appends feeds to original dictionary, under 'feeds' label. Uses parse_market.
        feed_ticker['feeds'] = {'name': self.get_target(), 
                                'ticker': self.parse_markets(feed_ticker, self.__parser_keys_feed)
                               } #Feeds
        return feed_ticker
    
    
class WikiToTickerKey_Parser(BaseParser):
    
    def __init__(self):
        super().__init__(source='Wiki', target='YahooFinance')
        
        self.__parser_keys_tickerKey = {('market','IBEX35'):{'prefix':'','suffix':'.MC.TT','excedent_string':''},
                                        ('market','CAC40') :{'prefix':'','suffix':'.PA.TT','excedent_string':'Euronext: '},
                                        ('market','DAX30') :{'prefix':'','suffix':'.DE.TT', 'excedent_string':''},
                                        ('market','SP500') :{'prefix':'','suffix':'', 'excedent_string':''}}
        
    def feeder_ticker(self,feed_ticker): # Appends feeds to original dictionary, under 'feeds' label. Uses parse_market.
        feed_ticker['tickerKey'] = self.parse_markets(feed_ticker, self.__parser_keys_tickerKey)
        return feed_ticker
    
    
def get_parsed_tickers(raw_ticker_list): # Función con la que hay que engancahr
        # Actualiza los par-value del diccionario: "tickerYahoofinance":"value", "tickerAlphaVantage":"value"
        print('Activation: get_parsed_tickers')
        my_parser_W2YF = WikiToYahooFinance_Parser()
        my_parser_W2TK = WikiToTickerKey_Parser()
        feeder_ticker_list = []
        for raw_ticker in raw_ticker_list:
            feeder_ticker = my_parser_W2TK.feeder_ticker(raw_ticker)
            feeder_ticker = my_parser_W2YF.feeder_ticker(feeder_ticker)
            feeder_ticker_list.append(feeder_ticker)
        print('All tickers fed')
        return feeder_ticker_list 
    