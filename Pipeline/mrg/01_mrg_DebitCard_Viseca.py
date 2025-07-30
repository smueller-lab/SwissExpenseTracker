# %%
import pandas as pd
from datetime import datetime
from Pipeline.config import *

# --- Init ---
dr = Drive()
fn = Filename()

# %%
pth_Master_Viseca = oj(dr.Use_Viseca, fn.Master_Viseca)
pth_Master_Debit = oj(dr.Use_Debit, fn.Master_Debit)

pdf_Viseca = pd.read_parquet(pth_Master_Viseca)
pdf_Debit = pd.read_parquet(pth_Master_Debit)

# %%
# preprocessing
pdf_Debit = pdf_Debit.rename(columns={'nm_subject': 'Merchant'})

# get year-month
pdf_Debit['year_month'] = pdf_Debit['Date'].dt.to_period('M')
pdf_Viseca['year_month'] = pdf_Viseca['Date_billing'].dt.to_period('M')

# get df with CreditCardPayment from DebitCard
pdf_Debit['idx'] = pdf_Debit.index
pdf_DebitBilling = pdf_Debit.loc[pdf_Debit['Merchant'] == 'Viseca Payment Services SA', ['Date', 'idx', 'year_month']]

# remove billing and merge DebitBilling into Viseca to get the idx, filter dataframe to only contain data which exists in both dataframes
pdf_Viseca = pdf_Viseca[pdf_Viseca['Details'] != 'Ihre Zahlung - Danke'].reset_index(drop=True)
pdf_Viseca = pdf_Viseca.merge(pdf_DebitBilling, on='year_month', how='left')
pdf_Viseca = pdf_Viseca[pdf_Viseca['Date'].notna()].reset_index(drop=True)

# get billing idx and remove them from Debit
sidx_Billing = pdf_Viseca['idx'].unique()
pdf_Debit = pdf_Debit[~pdf_Debit.index.isin(sidx_Billing)]

# concat Viseca and Debit and reindex
pdf_Data = pd.concat([pdf_Debit, pdf_Viseca], ignore_index=True)
pdf_Data = pdf_Data.sort_values(by='idx', kind='stable').reset_index(drop=True)

# drop not needed columsn before next step
pdf_Data = pdf_Data.drop(columns=['year_month', 'idx', 'Date_billing', 'Booking text', 'Payment purpose', 'Details'])

# %%
# re-calculate Balance with new inserted Viseca transactions

# invert order of entire dataframe
pdf_Data = pdf_Data.iloc[::-1].reset_index(drop=True)

# get signed amount with +/-
pdf_Data['signed_amount'] = pdf_Data['amount_CHF']
pdf_Data.loc[pdf_Data['transaction_type'] == 'income', 'signed_amount'] *= -1

# get balance start and re-calculate Balance
balance_Start = pdf_Data.iloc[0]['Balance CHF'] + pdf_Data.iloc[0]['amount_CHF']
pdf_Data['Balance_CHF'] = balance_Start - pdf_Data['signed_amount'].cumsum()

# reinvert dataframe
pdf_Data = pdf_Data.iloc[::-1].reset_index(drop=True)
# TODO: Write test if top row Balance CHF and 'Balance_CHF' are actually the same

# combine columns
pdf_Data['TransactionID'] = pdf_Data['TransactionID'].combine_first(pdf_Data['ZKB reference'])
pdf_Data['Merchant'] = pdf_Data['Merchant'].combine_first(pdf_Data['MerchantName'])
pdf_Data['MerchantPlace'] = pdf_Data['MerchantPlace'].combine_first(pdf_Data['city'])

# drop other columns
pdf_Data = pdf_Data.drop(columns=['ZKB reference', 'city', 'MerchantName', 'signed_amount', 'Balance CHF'])

# reorder columns
pdf_Data = pdf_Data[['TransactionID', 'Date', 'Balance_CHF', 'transaction_type', 'Merchant', 'amount_CHF', 'category_main', 'category_second', 'MerchantPlace', 'MerchantCountry', 'Date_Action', 'Time_Action', 'Date_Valuta']].copy()

# %%
# write final dataframe to use
date_fn = datetime.today().strftime('%Y%m%d')

pth_use_Master = oj(dr.Use_Bank_ZKB, 'Master_Bank_ZKB.parquet')
pth_use_Master_v = oj(dr.Use_Bank_ZKB, f'Master_Bank_ZKB_{date_fn}.parquet')

pdf_Data.to_parquet(pth_use_Master, engine='pyarrow', index=False)
pdf_Data.to_parquet(pth_use_Master_v, engine='pyarrow', index=False)