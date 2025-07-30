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
from rapidfuzz import fuzz
from Pipeline.config import *
from Pipeline.cfg_cleaning import config
from Pipeline.libs import (
    load_cache,
    save_cache,
    map_Migros_subject,
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
pth_Cache = oj(dr.Rfn_Debit_labelAI, fn.Cache_Debit)
vk_Cache = load_cache(pth_Cache, q_Redo)
sfn_process = get_sfn_process(dr.Rfn_Debit_labelAI, vk_Cache, q_Redo)

# %%
for fn in sfn_process:
    pth = oj(dr.Rfn_Debit_labelAI, fn)
    pdf = pd.read_pickle(pth)

    # ----- First step is to combine all similar subject names
    for Merchant in cfg.s_Merchant_sim:
        pdf['nm_subject'] = pdf['nm_subject'].apply(lambda x: Merchant if Merchant in x else x)

    # get right nm_subject for all Migros stores
    mk_Migros = pdf['nm_subject'].str.contains('Migros', na=False)
    pdf.loc[mk_Migros, 'nm_subject'] = pdf.loc[mk_Migros, 'Subject'].apply(lambda x: map_Migros_subject(x, cfg.vk_Migros))

    # combine custom
    pdf['nm_subject'] = pdf['nm_subject'].apply(lambda x: cfg.nm_PublicTransportSwiss if cfg.nm_PublicTransportSwiss in x and cfg.nm_Kiosk not in x else x)
    pdf['nm_subject'] = pdf['nm_subject'].apply(lambda x: cfg.nm_KioskLate if fuzz.partial_ratio(x.lower(), cfg.nm_KioskLate.lower()) > 80 or cfg.nm_Valora in x else x)

    # drop not needed columns
    pdf = pdf.drop(columns=['Subject'])

    # OpenAI labeling post-processing
    # Some labels might be not correct, inconsistent or not what I had in mind.
    # in this step we will fix this

    # clean names for Friend
    pdf.loc[pdf['category_main'] == 'Friend', 'nm_subject'] = pdf.loc[pdf['category_main'] == 'Friend', 'nm_subject'].str.title()
    pdf['nm_subject'] = pdf['nm_subject'].str.replace(r'(\+41|0041)\d{7,}', '', regex=True)
    pdf['city'] = pdf['city'].str.replace(r'(\+41|0041)\d{7,}', '', regex=True)

    # replace Housing and tax names
    pdf.loc[pdf['Payment purpose'].str.contains('Miete', na=False), cfg.snm_Category] = ['Hausing', 'Rent']
    pdf.loc[pdf['nm_subject'].str.contains(cfg.nm_Friend_rent), cfg.snm_Category] = ['Housing', 'Rent']
    pdf.loc[pdf['Payment purpose'].str.contains('Kaution', na=False), cfg.snm_Category] = ['Housing', 'Deposit']
    pdf.loc[pdf['nm_subject'] == cfg.nm_Landlord1, cfg.snm_Category] = ['Housing', 'Rent']
    pdf.loc[pdf['Payment purpose'].str.contains('Steuer', na=False), cfg.snm_Category] = ['Government', 'Tax Services']

    pdf.loc[pdf['Booking text'].str.contains(cfg.nm_GasStation_de), 'nm_subject'] = cfg.nm_GasStation_en

    # replace categories for work
    pdf.loc[(pdf['nm_subject'] == cfg.nm_Work1) & (pdf['transaction_type'] == 'income'), cfg.snm_Category] = cfg.snm_Salary
    pdf.loc[(pdf['nm_subject'] == cfg.nm_Work2) & (pdf['transaction_type'] == 'income'), cfg.snm_Category] = cfg.snm_Salary

    pdf['Payment purpose'] = pdf['Payment purpose'].str.replace(r'\bSalar\b', 'Salaer', regex=True)
    pdf.loc[(pdf['transaction_type'] == 'income') & (pdf['Payment purpose'].str.contains('Salaer')), cfg.snm_Category] = cfg.snm_Salary
    pdf.loc[(pdf['transaction_type'] == 'income') & (pdf['nm_subject'].str.contains(cfg.nm_Work3)), cfg.snm_Category] = cfg.snm_Salary
    pdf.loc[(pdf['nm_subject'] == cfg.nm_Work1) & (pdf['transaction_type'] == 'expense'), cfg.snm_Category] = ['Restaurant', 'Food']

    # replace categories for Migros
    pdf.loc[pdf['nm_subject'] == cfg.nm_GolfHome, cfg.snm_Category] = ['Sport', 'Golf']
    pdf.loc[pdf['nm_subject'] == cfg.nm_GolfHome_Rest, cfg.snm_Category] = ['Restaurant', 'Food']
    pdf.loc[pdf['nm_subject'] == cfg.nm_ShopSport, cfg.snm_Category] = ['Retail', 'Sport']
    pdf.loc[pdf['nm_subject'] == cfg.nm_MarketRest, cfg.snm_Category] = ['Restaurant', 'Food']
    pdf.loc[pdf['nm_subject'] == cfg.nm_ShopGarden, cfg.snm_Category] = ['Retail', 'Garden Center']
    pdf.loc[pdf['nm_subject'] == cfg.nm_Supermarket, cfg.snm_Category] = ['Groceries', 'Supermarket']

    # replace more categories
    pdf.loc[pdf['nm_subject'] == cfg.nm_ShopSmall, cfg.snm_Category] = ['Groceries', 'Supermarket']
    pdf.loc[pdf['nm_subject'] == cfg.nm_localCafe, cfg.snm_Category] = ['Restaurant', 'Cafe']
    pdf.loc[pdf['nm_subject'] == cfg.nm_PostOffice, cfg.snm_Category] = ['Postal Services', 'Post Office']
    pdf.loc[pdf['nm_subject'] == cfg.nm_HairSalon, cfg.snm_Category] = ['Healthcare', 'Hair Salon']
    pdf.loc[pdf['nm_subject'] == cfg.nm_BakerySpecial, cfg.snm_Category] = ['Restaurant', 'Bakery']
    pdf.loc[pdf['nm_subject'] == cfg.nm_KioskLate, cfg.snm_Category] = ['Groceries', 'Supermarket']
    pdf.loc[pdf['nm_subject'] == cfg.nm_Bakery, cfg.snm_Category] = ['Restaurant', 'Bakery']
    pdf['nm_subject'] = pdf['nm_subject'].replace({cfg.nm_OpenBar1: cfg.nm_OpenBar2})
    
    # replace other categories
    pdf.loc[pdf['category_second'] == 'Swimming pool', 'category_main'] = 'Sport'
    pdf.loc[pdf['category_second'] == 'Cable Car', 'category_main'] = 'Transport'
    pdf.loc[pdf['category_main'] == 'Parking', 'category_second'] = 'Car'
    pdf.loc[pdf['nm_subject'] == cfg.nm_OpenBar3, cfg.snm_Category] = ['Restaurant', 'Pub']

    pth_Output = oj(dr.Rfn_Debit_labelAI_cleaned, fn)
    pdf.to_pickle(pth_Output)

    # update cache
    if not q_Redo:
        vk_Cache['sfn_processed'].append(fn)
    else:
        vk_Cache['sfn_processed'] = sfn_process

# save cache
save_cache(pth_Cache, vk_Cache)
