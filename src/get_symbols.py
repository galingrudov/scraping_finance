#!/usr/bin/env python3
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
from tqdm.auto import tqdm

# stock exchange :
# AMEX - American Stock Exchange,
# LSE - London Stock Exchange, 
# NASDAQ - NASDAQ Stock Exchange,
# NYSE - New York Stock Exchange,
# SGX - Singapore Stock Exchange
# TSX - Toronto Stock Exchange
def get_indexes (stocks_exchange):
    def get_stock_indexes(stock_exchange_name):
        base_url="http://eoddata.com/stocklist"
        letters_upper_case = list(string.ascii_uppercase)
        digits = [x for x in range(10)]
        if stock_exchange_name in ['LSE', 'SGX']:
            symbols_tabs = digits + letters_upper_case 
        else:
            symbols_tabs = letters_upper_case
         
        df_index_description = pd.DataFrame({'Code':[],
                                             'Name':[]})
        urls = [base_url +  "/{}/{}.htm".format(stock_exchange_name, letter) 
                for letter in  symbols_tabs ]
        html_contents = (requests.get(url).text for url in urls)
        with tqdm(total=len(urls)) as pbar:
            for html_content in  html_contents:
                soup = BeautifulSoup(html_content, "lxml")
                tables = soup.find_all(lambda tag: tag.name=='table')
                read_table = pd.read_html(str(tables[5]))
                temp = read_table[0][['Code','Name']]                
                if(set(temp['Code']).isdisjoint(set(df_index_description['Code']))):                                                         
                    df_index_description = pd.concat([df_index_description
                                                      , read_table[0][['Code','Name']]
                                                      ])
                time.sleep(1)
                pbar.update(1)
        return df_index_description['Code']
    
    df_index_description = pd.DataFrame()
    for stocks in stocks_exchange:
        df_index_description = pd.concat([df_index_description
                                        , get_stock_indexes(stocks)])
    return df_index_description
