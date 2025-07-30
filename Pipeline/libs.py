import pandas as pd
import json
import os
import numpy as np
from typing import Dict, Any
from Pipeline.cfg_cleaning import config
from Pipeline.OpenAI import OpenAI_Bot
import argparse


# ----- Pipeline presteps -----

# functions for caching
def load_cache(pth_cache: str, q_Redo: bool):
    if q_Redo:
        vk_Cache = {}
        vk_Cache.setdefault('sfn_processed', [])
        return vk_Cache
    if os.path.exists(pth_cache):
        with open(pth_cache, 'r') as f:
            return json.load(f)
    else:
        return {"sfn_processed": []}
        

def save_cache(pth_cache: str, vk_Cache: Dict):
    with open(pth_cache, 'w') as f:
        json.dump(vk_Cache, f, indent=2)


def load_TID_cache(pth_Cache: str):
    if os.path.exists(pth_Cache):
        with open(pth_Cache, 'r') as f:
            return json.load(f)
    return {'sfn_processed': [], 's_TID': []}


def save_TID_cache(vk_Cache: Dict[str, Any], pth_Cache: str):
    with open(pth_Cache, 'w') as f:
        json.dump(vk_Cache, f, indent=2)


def parse_Pipeline_args():
    """
    Parse the command-line argument '--q_Redo' as a boolean

    Returns:
        bool: True if --q_Redo=True, False otherwise.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--q_Redo', type=lambda x: x.lower() == 'true', default=False)
    args = parser.parse_args()
    return args.q_Redo


def get_sfn_process(dr: str, vk_Cache: dict, q_Redo: bool):
    sfn = [f for f in os.listdir(dr) if f.endswith('.pkl')]
    sfn_process = sfn if q_Redo else [f for f in sfn if f not in vk_Cache['sfn_processed']]
    return sfn_process


# ----- functions for data cleaning -----

def BookingText_Split(text: str):
    if 'TWINT' in text:
        return text.split(':', 1)[-1]
    elif 'ZKB Visa Debit' in text:
        return text.split(',', 1)[-1]
    elif 'Debit eBanking' in text:
        return text.split(':', 1)[-1]
    elif 'ZKB Mastro card' in text:
        return text.split(',', 1)[-1]
    else:
        return text.split(':', 1)[-1]
    

def map_Migros_subject(nm_subject: str, vk_Migros: dict):
    if not 'Migros' in nm_subject:
        return nm_subject
    
    for substring, Subject in vk_Migros.items():
        if substring in nm_subject:
            return Subject
        
    return 'Migros'


def fill_Category(pdf: pd.DataFrame, vk_Cat: dict):
    for merchant in vk_Cat:
        pdf.loc[pdf['Merchant'].str.contains(merchant, case=False, na=False), 'Category_OpenAI'] = vk_Cat[merchant]
    
    return pdf


# ----- functions to process data -----

def extract_date(fn):
    return pd.to_datetime(fn.split('_')[0], format="%Y%m%d")


def process_viseca(pdf: pd.DataFrame):
    # filter to avoid still pending transactions
    pdf = pdf[pdf['StateType'] == 'BOOKED'].copy()

    # drop not needed columns
    pdf = pdf.drop(columns=['CardId', 'OriginalAmount', 'Currency', 'OriginalCurrency', 'StateType', 'Type', 'Exchange Rate'])

    # extract time from Date_Action and transform Datelike columns
    pdf['Date'] = pd.to_datetime(pdf['Date'])
    pdf['ValutaDate'] = pd.to_datetime(pdf['ValutaDate'])
    pdf['Date_Action'] = pdf['Date'].dt.date
    pdf['Time_Action'] = pdf['Date'].dt.time
    pdf['Date_Valuta'] = pdf['ValutaDate'].dt.date
    pdf = pdf.drop(columns=['Date', 'ValutaDate'])

    # rename columns
    pdf = pdf.rename(columns={
        'TransactionId': 'TransactionID',
        'Amount': 'amount_CHF'
    })

    # add transaction_type: (expense, income)
    pdf['transaction_type'] = np.where(pdf['amount_CHF'] > 0, 'expense', 'income')
    pdf['amount_CHF'] = pdf['amount_CHF'].abs()

    # custom value replacement
    pdf['MerchantName'] = pdf['MerchantName'].replace(config().vk_Merchant_replace, regex=True)
    pdf['Details'] = pdf['Details'].replace(config().vk_Merchant_replace, regex=True)

    return pdf


def label_transactions(pdf: pd.DataFrame, Bot: OpenAI_Bot):
    s_Merchant = pdf['Details'].unique()
    vk_Category = Bot.get_vkCategory(s_Merchant)

    # map categories to dataframe
    pdf_label = pdf.copy()
    pdf_label.loc[:, 'Category_OpenAI'] = pdf_label['Details'].map(vk_Category)

    return pdf


def get_unprocessed_DebitTransactions(pdf_Master: pd.DataFrame, pdf_Debit: pd.DataFrame) -> pd.DataFrame:
    """
    Getting pdf_Debit which contains only transactions which haven't processed yet.
    Based on last full day and transactions from last day in case if missing transactions from the last day which aren't yet in Masters file.
    
    Args:
        pdf_Master (pd.DataFrame): Master file from Use
        pdf_Debit (pd.DataFrame): New Debit data

    Returns:
        pd.DataFrame: filtered dataframe containing only unprocessed transactions
    """
    z_DateUniqueSorted = pdf_Master['Date'].sort_values(ascending=False).unique()
    Date_lastDayFull = z_DateUniqueSorted[1]
    Date_lastDay = z_DateUniqueSorted[0]

    s_ZKBReference_drop = pdf_Master.loc[pdf_Master['Date'] == Date_lastDay, 'ZKB reference'].unique()
    s_ZKBReference_drop = [x for x in s_ZKBReference_drop if not pd.isna(x)]

    pdf_Debit = pdf_Debit[pdf_Debit['Date'] > Date_lastDayFull].reset_index(drop=True)
    pdf_Debit = pdf_Debit[~pdf_Debit['ZKB reference'].isin(s_ZKBReference_drop)].reset_index(drop=True)

    return pdf_Debit