#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 16:24:13 2021

@author: galin
"""

import sys
sys.path.append('../src/')
import scraping_zacks
import get_symbols

print ('Get Symbols')
symbols = get_symbols.get_indexes(['NYSE'])
print('Get zacks ranks')
df = scraping_zacks.scraping_zacks_ranks(symbols)
df.to_csv('ranks.csv')
print (df)
