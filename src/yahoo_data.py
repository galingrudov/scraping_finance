import requests
import  pandas as pd
import datetime as dt
import time

class yahoo_data:
    def __init__(self):
        self.base_history_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&period1=0&period2=9999999999&interval={1}'
        self.base_finance_url = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?modules='
        self.modules = ['assetProfile', 'incomeStatementHistory', 'incomeStatementHistoryQuarterly'
                        , 'balanceSheetHistory', 'balanceSheetHistoryQuarterly', 'cashflowStatementHistory'
                        , 'cashflowStatementHistoryQuarterly', 'defaultKeyStatistics', 'financialData'
                        , 'calendarEvents', 'secFilings', 'recommendationTrend', 'upgradeDowngradeHistory'
                        , 'institutionOwnership', 'fundOwnership', 'majorDirectHolders','majorHoldersBreakdown'
                        , 'insiderTransactions', 'insiderHolders', 'netSharePurchaseActivity', 'earnings', 'earningsHistory'
                        , 'earningsTrend', 'industryTrend', 'indexTrend', 'sectorTrend' ]
        self.modules_concatenation_string = '%2C'
        self.failed_url_reqests = []
        self.non_equity = []
        
    def __transform_timestamp_to_datetime(self, timestamp):
        dt_o = dt.datetime.fromtimestamp(timestamp)
        return dt_o.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def __extract_url_history_data(self, symbol, period_begin=0, period_end= 9999999999,interval = '1d', range=None):
        url = self.base_history_url.format(symbol, interval)
        if range!=None:
            url=url+'&range={}'.format(range)
        responce = requests.get(url)
        if responce.status_code != 200:
            self.failed_url_reqests.append(symbol)
            print (responce)
            return {}
        time.sleep(1)
        json_page = responce.json()
        main_data_dic = json_page['chart']['result'][0]
        return main_data_dic
    
    def __extract_history_data (self, symbol, main_data_dic):
        if 'meta' not in main_data_dic.keys():
            print ('missing "meta" tag, symbol {}'.format(symbol))
            return pd.DataFrame()
        times = [self.__transform_timestamp_to_datetime(tm_stp) for tm_stp in main_data_dic['timestamp']]
        adj_close = main_data_dic['indicators']['adjclose'][0]['adjclose']
        df = pd.DataFrame({
            'Data': times
            , symbol: adj_close})
        return (df)
    
    def __extract_meta_data(self, symbol, main_data_dic):
        if 'meta' not in main_data_dic.keys():
            print ('missing "meta" tag, symbol {}'.format(symbol))
            return pd.DataFrame()
        if 'instrumentType' not in main_data_dic['meta'].keys():
            print( 'missing "instrumentType" tag, symbol {}'.format(symbol))
            return pd.DataFrame ()
        dic_data = {'symbol':symbol}
        dic_data['symbol_type'] = main_data_dic['meta']['instrumentType']
        dic_data['currency'] = main_data_dic['meta']['currency']
        return dic_data
    # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
    # Valid Ranges:["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max"]
    def extract_history_data (self, symbols, period_begin=0, period_end= 9999999999,interval = '1d', range=None):
        return_df = pd.DataFrame({'Data':[]})
        rows_list = []
        for symbol in symbols:
            data_history = self.__extract_url_history_data(symbol, period_begin, period_end, interval , range)
            df = self.__extract_history_data(symbol, data_history)
            return_df = return_df.merge(df, 'outer')
            
            meta_row = self.__extract_meta_data(symbol,data_history)
            rows_list.append(meta_row)
        meta_df = pd.DataFrame(rows_list)
        return return_df , meta_df


