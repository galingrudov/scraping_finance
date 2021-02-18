# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 22:18:27 2021

@author: galin
"""

import string
import requests
from bs4 import BeautifulSoup
import pandas as pd
import  time


def get_indexes (stocks_excange):
    def get_stock_indexes(stock_exchange_name):
        base_url="http://eoddata.com/stocklist"
        letters_upper_case = list(string.ascii_uppercase)
        df_index_description = pd.DataFrame()
        progress_bar = []
        urls = (base_url +  "/{}/{}.htm".format(stock_exchange_name, letter) 
                for letter in  letters_upper_case )
        html_contents = (requests.get(url).text for url in urls)
        for html_content in  html_contents:
            
            progress_bar.append('*')
            soup = BeautifulSoup(html_content, "lxml")
            tables = soup.find_all(lambda tag: tag.name=='table')
            read_table = pd.read_html(str(tables[5]))
            df_index_description = pd.concat([df_index_description
                                              , read_table[0][['Code','Name']]
                                              ])
            print(progress_bar)
            time.sleep(1)
        return df_index_description['Code']
    df_index_description = pd.DataFrame()
    for stocks in stocks_excange:
        df_index_description = pd.concat([df_index_description
                                        , get_stock_indexes(stocks)])
    return df_index_description