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
# ---

# %%
import pandas as pd
from Pipeline.config import *
from Pipeline.cfg_cleaning import config
cfg = config()

# %%
# --- Init ---
dr = Drive()
fn = Filename()

# %%
pdf = pd.read_parquet(oj(dr.Use_Bank_ZKB, fn.Master_ZKB))

# %%
# group values for Cafe
s_Cat_Cafe = ['Café', 'Cafe', 'Bar & Cafe', 'Coffee Shop']
pdf.loc[pdf['category_second'].isin(s_Cat_Cafe), 'category_second'] = 'Cafe'

# group values for Bakery
s_Cat_Bakery = ['Bakery', 'Pastry Shop']
pdf.loc[pdf['category_second'].isin(s_Cat_Bakery), 'category_second'] = 'Bakery'
pdf.loc[pdf['category_second'] == 'Bakery', 'category_main'] = 'Restaurant' 

# group values for Bar
s_Cat_Bar = ['Pub', 'Bar']
pdf.loc[pdf['category_second'].isin(s_Cat_Bar), 'category_second'] = 'Bar'

# group all other values for Restaurant under Restaurant under the 2nd category
pdf.loc[(pdf['category_main'] == 'Restaurant') & (~pdf['category_second'].isin(s_Cat_Cafe + s_Cat_Bakery + s_Cat_Bar)), 'category_second'] = 'Restaurant'

# set work place as Cafeteria because it's a collection of meals during one month
pdf.loc[(pdf['category_second'] == 'Restaurant') & (pdf['Merchant'] == cfg.nm_Work1), cfg.snm_Category] = ['Restaurant', 'Cafeteria']

# clean Merchant
pdf.loc[pdf['Merchant'].str.contains(cfg.nm_GroceryShop2), 'Merchant'] = cfg.nm_GroceryShop2

# clean Spa
pdf.loc[(pdf['Merchant'] == cfg.nm_CompanySpaMarathon) & (pdf['Date'] == cfg.Date_SpaMarathon), ['Merchant', 'category_main', 'category_second']] = [cfg.nm_SpaMarathon, 'Entertainment', 'Spa']

# correct values
pdf.loc[pdf['Merchant'] == cfg.nm_Mall1, cfg.snm_Category] = ['Groceries', 'Supermarket']
pdf.loc[pdf['Merchant'] == cfg.nm_LiftCompany, cfg.snm_Category] = ['Sport', 'Ticketing']
pdf.loc[pdf['Merchant'] == cfg.nm_FerryHome, cfg.snm_Category] = ['Transport', 'Ferry']

# group similar categories together
pdf.loc[pdf['category_main'].str.contains('Public Service', case=False, na=False), 'category_main'] = 'Public Service'
pdf.loc[pdf['Merchant'] == cfg.nm_HotelKisok, 'category_main'] = 'Groceries'
pdf.loc[pdf['category_second'] == 'Rental', 'category_main'] = 'Travel'
pdf['category_main'] = pdf['category_main'].replace({
    'Personal Care': 'Healthcare',
    'Hausing': 'Housing',
    'Health': 'Healthcare',
    'Financial': 'Finance',
    'Telecom': 'Telecommunication'
})

# group Accomodation (Airbnb, Hotel, Hostel)
pdf['category_second'] = pdf['category_second'].replace({'Accomodation': 'Accommodation', 'Airline': 'Flight'})
pdf.loc[pdf['Merchant'].str.contains(cfg.nm_HotelWebsite, case=False, na=False), 'Merchant'] = cfg.nm_HotelWebsite
pdf.loc[pdf['Merchant'] == cfg.nm_HotelWebsite, 'category_second'] = 'Accommodation'
pdf.loc[pdf['category_second'].str.contains('Accommodation', case=False, na=False), 'category_second'] = 'Accommodation'
pdf.loc[pdf['category_second'].str.contains('Hotel', case=False, na=False), 'category_second'] = 'Accommodation'

# override Transport in other countries as Travel
s_MerchantCountry = [i for i in pdf['MerchantCountry'].unique() if i not in [None, 'CHE', 'DEU']]
pdf.loc[
    (pdf['MerchantCountry'].isin(['USA', 'ITA', 'NLD', 'ESP', 'IRL', 'PRT', 'IDN'])) &
    (pdf['category_main'] == 'Transport'),
    'category_main'] = 'Travel'

# clean 3rd pillar
pdf.loc[
    (pdf['Merchant'].str.contains(cfg.nm_FullName)) & 
    (pdf['Date'].isin(pd.to_datetime(cfg.s_Date_Investing))),
    ['Merchant', 'category_main', 'category_second']
] = [cfg.nm_ThirdPillar, 'Investing', 'Third pillar']

# clean transport
pdf.loc[pdf['Merchant'] == cfg.nm_MobileProvider, 'category_main'] = 'Telecommunication'
pdf.loc[pdf['Merchant'] == cfg.nm_FineProvider, 'category_second'] = 'Traffic control'

mk_gasStation = pdf['category_main'] == 'Gas station'
pdf.loc[mk_gasStation & (pdf['amount_CHF'] < 10), 'category_second'] = 'Groceries'
pdf.loc[mk_gasStation & (pdf['amount_CHF'] >= 10), 'category_second'] = 'Car'

pdf['Merchant'] = pdf['Merchant'].replace({cfg.nm_Healthcare2: cfg.nm_Healthcare})

# differentiate between different Insurances at the same Insurance provider
pdf['Merchant'] = pdf['Merchant'].replace({cfg.nm_Insurance2: cfg.nm_Insurance})
pdf.loc[pdf['Merchant'] == cfg.nm_Insurance, 'category_main'] = 'Insurance'

mk_Insurance = pdf['Merchant'] == cfg.nm_Insurance
pdf.loc[mk_Insurance & (pdf['amount_CHF'] < cfg.Insurance_Threshold), 'category_second'] = 'Liability'
pdf.loc[mk_Insurance & (pdf['amount_CHF'] >= cfg.Insurance_Threshold), 'category_second'] = 'Car'

pdf.loc[pdf['Merchant'] == cfg.nm_thriftshop, cfg.snm_Category] = ['Retail', 'Thrift Shop']
pdf.loc[pdf['Merchant'] == cfg.nm_Dealership, 'categrory_second'] = 'Car Dealership'

pdf['category_second'] = pdf['category_second'].replace({
    'Car Dealer': 'Car Dealership',
    'Car-sharing': 'Car Sharing'
})
pdf.loc[pdf['Merchant'] == cfg.nm_Dealership2, 'category_main'] = 'Transport'

# clean Sport
pattern_retail  = '|'.join(['Retail', 'Goods', 'Equipment'])
pdf.loc[(pdf['category_main'] == 'Sport') & (pdf['category_second'].str.contains(pattern_retail, case=False, na=False)), 'category_second'] = 'Retail'
pdf.loc[pdf['category_second'].str.contains('Golf', na=False), 'category_second'] = 'Golf'
pdf.loc[pdf['category_second'].str.contains('Tennis', na=False), 'category_second'] = 'Tennis'

mk_GolfShop = pdf['Merchant'].str.contains(cfg.pattern_GolfShop, case=False, na=False)
pdf.loc[mk_GolfShop, 'Merchant'] = cfg.nm_GolfShop

pdf.loc[pdf['Merchant'].isin([cfg.nm_GolfShop, cfg.nm_GolfShop2, cfg.nm_GolfShop3]), 'category_second'] = 'Retail'
pdf.loc[pdf['Merchant'] == cfg.nm_FootballClub, cfg.snm_Category] = ['Entertainment', 'Sports Ticketing']
pdf.loc[pdf['Merchant'].str.contains(cfg.nm_SportShop, case=False, na=False), 'Merchant'] = cfg.nm_SportShop
pdf.loc[pdf['Merchant'] == cfg.nm_SportShop, cfg.snm_Category] = ['Sport', 'Retail']

pdf.loc[pdf['Merchant'].str.contains(cfg.nm_SportShop2, case=False, na=False), 'Merchant'] = cfg.nm_SportShop2
pdf.loc[pdf['Merchant'] == cfg.nm_SportShop2, cfg.snm_Category] = ['Sport', 'Retail']

mk_Rest = pdf['Merchant'].str.contains(cfg.nm_GolfHome_Rest, case=False, na=False)
mk_Golf = pdf['Merchant'].str.contains(cfg.nm_GolfHome, case=False, na=False)

pdf.loc[mk_Rest, 'Merchant'] = cfg.nm_GolfHome_Rest
pdf.loc[mk_Golf & ~mk_Rest, 'Merchant'] = cfg.nm_GolfHome

pdf['category_second'] = pdf['category_second'].replace({
    'University Sports': 'University Sport'
})

pdf.loc[pdf['Merchant'] == cfg.nm_BikeShop, cfg.snm_Category] = ['Sport', 'Retail']

# %%
pdf.to_parquet(oj(dr.Use_Bank_ZKB_RFN, fn.Master_ZKB))
