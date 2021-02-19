
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 22:15:52 2020

@author: galin
"""

import pandas as pd
import urllib.request
import datetime as dt
import os
import  time
from tqdm.auto import tqdm



def scraping_zacks_ranks(Symbols):
    
    def zacks_rank(Symbol):
        url = 'https://quote-feed.zacks.com/index?t='+Symbol
        downloaded_data = urllib.request.urlopen(url)
        data = downloaded_data.read()
        data_str = data.decode()
        Z_Rank = ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell']
        for Rank in Z_Rank:
            if data_str.find(Rank) != -1:
                return Rank
        
    strong_buy = []
    buy = []
    hold = []
    sell = []
    strong_sell = []
    no_zacks_rank = []
    for symbol in tqdm(Symbols):
        time.sleep(1)
        rank = zacks_rank(symbol)
        if(rank == 'Strong Buy'):
            #print('Strong Buy: {}'.format(symbol))
            strong_buy.append(symbol)
        elif rank == 'Buy':
            #print('Buy: {}'.format(symbol))
            buy.append(symbol)
        elif rank == 'Hold':
            #print('Hold: {}'.format(symbol))
            hold.append(symbol)
        elif rank == 'Sell':
            #print('Sell: {}'.format(symbol))
            sell.append(symbol)
        elif rank == 'Strong Sell':
            #print('Strong Sell: {}'.format(symbol))
            strong_sell.append(symbol)
        else:
            #print ("symbol no Zacks rank {}".format(symbol))
            no_zacks_rank.append(symbol)
    
    df = pd.DataFrame({
        'Strong_Buy':pd.Series(strong_buy, dtype = str),
        'Buy': pd.Series(buy, dtype = str),
        'Hold': pd.Series(hold, dtype = str),
        'Sell': pd.Series(sell, dtype = str),
        'Strong_Sell': pd.Series(strong_sell,dtype = str)        
        })
    if len(no_zacks_rank) > 0:
        print('These symbols do not have zacks rank: \n {}'.format(no_zacks_rank))
    return df

