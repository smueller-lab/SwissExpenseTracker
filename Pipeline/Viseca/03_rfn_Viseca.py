# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: venv
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from Pipeline.config import *
from Pipeline.cfg_cleaning import config
from Pipeline.libs import (
    load_cache,
    save_cache,
    parse_Pipeline_args,
    get_sfn_process
)

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()
fn = Filename()
cfg = config()

# %%
pth_Cache = oj(dr.Rfn_Viseca_labelAI, fn.Cache_Viseca)
vk_Cache = load_cache(pth_Cache, q_Redo)
sfn_process = get_sfn_process(dr.Rfn_Viseca_labelAI, vk_Cache, q_Redo)

# %%
for fn in sfn_process:
    pth = oj(dr.Rfn_Viseca_labelAI, fn)
    pdf = pd.read_pickle(pth)

    # split categories into seperate columns
    pdf[['category_main', 'category_second']] = pdf['Category_OpenAI'].str.split(',', expand=True)
    pdf = pdf.drop(columns=['Category_OpenAI'])
    # remove white spaces
    pdf[['category_main', 'category_second', 'MerchantName']] = pdf[['category_main', 'category_second', 'MerchantName']].apply(lambda x: x.str.strip())

    # Rename Merchant so you have same names for the same merchant
    pdf.loc[pdf['MerchantName'].str.contains(config().nm_TennisPlatform, case=False, na=False), 'MerchantName'] = config().nm_TennisPlatform
    pdf.loc[pdf['MerchantName'].str.contains(config().nm_PublicTransportSwiss, case=False, na=False), 'MerchantName'] = config().nm_PublicTransportSwiss

    # convert Rail to Train
    pdf.loc[pdf['category_second'] == 'Rail', ['category_main', 'category_second']] = ['Transport', 'Train']

    # replace Transportation with Transport
    pdf['category_main'] = pdf['category_main'].replace('Transportation', 'Transport')

    # swap categories
    mk_Travel = pdf['category_second'] == 'Travel'
    category_main = pdf.loc[mk_Travel, 'category_main']
    pdf.loc[mk_Travel, 'category_main'] = 'Travel'
    pdf.loc[mk_Travel, 'category_second'] = category_main
    
    # fill values for Card fees
    mk_CardFees = pdf['Details'] == 'Rechnungsgebühr'
    pdf.loc[mk_CardFees, ['MerchantName', 'MerchantPlace', 'MerchantCountry', 'category_main', 'category_second']] = ['Viseca', cfg.nm_LocationViseca, 'CHE', 'Finance', 'Card Fees']

    # fill values for credit card payment
    mk_payment = (pdf['Details'] == 'Ihre Zahlung - Danke') & (pdf['transaction_type'] == 'income')
    pdf.loc[mk_payment, ['MerchantName', 'MerchantPlace', 'MerchantCountry', 'category_main', 'category_second']] = ['Viseca', cfg.nm_LocationViseca, 'CHE', 'Finance', 'Payment Services']

    # correct categories of wrong labeling
    pdf.loc[pdf['MerchantName'].str.contains(cfg.nm_CustomSportsWear), config().snm_Category] = ['Retail', 'Sport']
    pdf.loc[pdf['MerchantName'].str.contains('Restaurant'), config().snm_Category] = ['Food', 'Restaurants']
    pdf.loc[pdf['MerchantName'].str.contains(cfg.nm_GolfLinks), config().snm_Category] = ['Sport', 'Golf']
    pdf.loc[pdf['MerchantName'].str.contains(cfg.nm_RacketSportsApp), config().snm_Category] = ['Sport', 'Tennis']
    pdf.loc[pdf['MerchantName'].str.contains(cfg.nm_eLearning), config().snm_Category] = ['Education', 'e-Learning']
    pdf.loc[pdf['MerchantName'].str.contains(cfg.nm_GroceryShop1), config().snm_Category] = ['Food', 'Groceries']
    pdf.loc[pdf['MerchantName'].str.contains('Hostel'), config().snm_Category] = ['Travel', 'Accomodation']

    # write to labelAI_clean
    pth_Refined = oj(dr.Rfn_Viseca_labelAI_cleaned, fn)
    pdf.to_pickle(pth_Refined)

    # update cache
    vk_Cache['sfn_processed'].append(fn)

save_cache(pth_Cache, vk_Cache)
