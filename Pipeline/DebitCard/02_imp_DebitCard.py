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
import re
from Pipeline.OpenAI import OpenAI_Bot
from Pipeline.config import *
from Pipeline.cfg_cleaning import config
from Pipeline.libs import (
    BookingText_Split, 
    load_cache, save_cache,
    get_sfn_process,
    get_unprocessed_DebitTransactions,
    parse_Pipeline_args
)

# --- Load CLI arg for q_Redo ---
q_Redo = parse_Pipeline_args()

# --- Init ---
dr = Drive()
fn = Filename()
cfg = config()

# %%
pth_Cache = oj(dr.Raw_Debit, fn.Cache_label)
vk_Cache = load_cache(pth_Cache, q_Redo)
sfn_process = get_sfn_process(dr.Raw_Debit, vk_Cache, q_Redo)

# read DebitCard Master to only process data which are not in Master file yet
pth_Master = oj(dr.Use_Debit, fn.Master_Debit)
pdf_Master = pd.read_parquet(pth_Master)

# %%
Bot = OpenAI_Bot()

for fn in sfn_process:
    pth = oj(dr.Raw_Debit, fn)
    pdf = pd.read_pickle(pth)

    # Date forward fill for missing Dates
    pdf['Date'] = pdf['Date'].ffill()

    # filter df to contain only unprocessed data
    pdf = get_unprocessed_DebitTransactions(pdf_Master, pdf)

    # When there are multiple transactions via eBanking it tracks the total amount with reference number and the individual transactions without reference but with transaction origin
    # we copy the reference number and add an increment for the ones without and then delete the transaction with the total amount
    pdf['referenceFill'] = pdf['ZKB reference'].ffill()
    pdf['cnt_NA'] = pdf.groupby(pdf['referenceFill'].ne(pdf['referenceFill'].shift()).cumsum()).cumcount()

    # Get transactions subjects and generate AI labels
    pdf['Subject'] = pdf['Booking text'].apply(BookingText_Split)
    pdf = pdf.drop(columns=['Reference number', 'Value date'])

    # drop row with total eBanking as it will be replaced by the individual eBanking transactions
    s_dup = pdf.duplicated('referenceFill', keep=False)
    pattern_eBanking = '|'.join(map(re.escape, cfg.snm_eBanking))
    sidx_drop = pdf[s_dup & (pdf['Subject'].str.contains(pattern_eBanking, case=False, na=False, regex=True))].index
    pdf = pdf.drop(index=sidx_drop)

    pdf['referenceFill'] = pdf['referenceFill'] + pdf['cnt_NA'].where(pdf['ZKB reference'].isna(), 0).astype(str).replace('0', '')
    pdf['ZKB reference'] = pdf['referenceFill'].copy()
    pdf = pdf.drop(columns=['cnt_NA', 'referenceFill'])

    # There are 2 kind of reference NA's.
    # 1. Transactions made from eBanking: Will take care of that later
    # 2. Temporary transactions which haven't been fully approved yet. 
    # It's also not giving a Balance and incorrect information about Balance CHF. Filter df to only include data until first temporary transaction.
    pdf_referenceNA = pdf[pdf['ZKB reference'].isna()]
    s_Date = pdf_referenceNA['Date'].dropna().tolist()
    if len(s_Date) > 0:
        Date_tempTA = min(s_Date)
        pdf = pdf[pdf['Date'] < Date_tempTA].reset_index(drop=True)

    # delete the transactions with the total amount
    pdf = pdf[~pdf['Subject'].str.contains(pattern_eBanking, case=False, na=False, regex=True)]

    # Paste transactions values from eBanking into Debit CHF when:
    pdf.loc[(pdf['Credit CHF'].isna()) & (pdf['Debit CHF'].isna()) & (pdf['Curr'] == 'CHF'), 'Debit CHF'] = pdf['Amount details']

    # Now we can safely drop the columns Curr and Amount details
    pdf = pdf.drop(columns=['Curr', 'Amount details'])

    # create category for transaction type
    pdf.loc[pdf['Debit CHF'].notna(), 'transaction_type'] = 'expense'
    pdf.loc[pdf['Credit CHF'].notna(), 'transaction_type'] = 'income'

    # combine both amount columns into one and drop the old ones
    pdf['amount_CHF'] = abs(pdf['Debit CHF'].fillna(0) - pdf['Credit CHF'].fillna(0))
    pdf = pdf.drop(columns=['Debit CHF', 'Credit CHF'])

    # fill missing Balance based on last known Balance
    while pdf['Balance CHF'].isna().sum() > 0:
        pdf['prev_Balance'] = pdf['Balance CHF'].shift(-1)
        pdf.loc[(pdf['Balance CHF'].isna()) & (pdf['transaction_type'] == 'expense'), 'Balance CHF'] = pdf['prev_Balance'] - pdf['amount_CHF']
    if 'prev_Balance' in pdf.columns:
        pdf = pdf.drop(columns=['prev_Balance'])

    pdf['Subject'] = pdf['Subject'].str.replace(' GP ', ' Golfpark ', regex=True)
    pdf = pdf.reset_index(drop=True)
    
    # get transaction labels
    s_df = []
    for i in range(0, len(pdf), 100):
        pdf_subset = pdf.iloc[i:i+100].copy()
        pdf_subset = pdf_subset.reset_index(drop=True)
        pdf_subset['uid'] = pdf_subset.index.astype(str)

        vk_Subject = pdf_subset[['uid', 'Subject']].to_dict('records')

        df_final = pd.DataFrame(columns=['uid', 'nm_subject', 'category_main', 'category_second'])
        n_try = 0
        max_try = 10

        # OpenAI sometimes doesn't return labels for some transactions
        # loop until it returned values for all transactions
        while n_try < max_try:
            n_try += 1
            df = Bot.get_df_ZKBTransactionDetails(vk_Subject)
            df_clean = df[~(df['nm_subject'].isna() | (df['nm_subject'] == 'NA'))]
            df_clean.loc[:, 'uid'] = df_clean['uid'].astype(str).str.split('|').str[0]

            if len(df_clean) == len(pdf_subset):
                break
        
        pdf_subset = pdf_subset.merge(df_clean, on='uid', how='left').drop(columns=['uid'])
        s_df.append(pdf_subset)


    # combine individual dataframes 
    pdf_result = pd.concat(s_df)
    pdf_result = pdf_result.reset_index(drop=True)

    # write dataframe to rfn
    fn_output = fn.replace('.pkl', '_labelAI.pkl')
    pth_output = oj(dr.Rfn_Debit_labelAI, fn_output)
    pdf_result.to_pickle(pth_output)

    # update cache
    vk_Cache['sfn_processed'].append(fn)

# save cache
save_cache(pth_Cache, vk_Cache)
