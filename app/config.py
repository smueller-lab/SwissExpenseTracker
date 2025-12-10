import os
from dataclasses import dataclass
from dotenv import load_dotenv
from dash.dash_table import FormatTemplate
oj = os.path.join
load_dotenv()

@dataclass
class config:
    vk_dTick_Grocery = {'Monthly': 100, 'Yearly': 500, 'Visit': 20}
    vk_dTick_Food = {'Monthly': 100, 'Yearly': 1000, 'Visit': 20}

    d_Tick_balance: int = 5000
    d_Tick_pct: int = 20


@dataclass
class FDP:
    dr_Box = os.getenv('dr_Box')
    dr_app = os.getenv('dr_app')
    
    dr_Use_Debit = oj(dr_Box, 'use', 'DebitCard')
    dr_Use_Viseca = oj(dr_Box, 'use', 'Viseca')
    dr_Use_Bank_ZKB = oj(dr_Box, 'use', 'Bank_ZKB')
    dr_Use_app = oj(dr_Box, 'use', 'app')
    dr_app_data = oj(dr_app, 'data')

    pth_Master_Debit = oj(dr_Use_Debit, 'Master_DebitCard.parquet')
    pth_Master_Viseca = oj(dr_Use_Viseca, 'Master_Viseca.parquet')
    pth_Master_BankZKB = oj(dr_Use_Bank_ZKB, 'Master_Bank_ZKB.parquet')

    # ----- Use data for app -----
    pth_app_GroceryMonth = oj(dr_Use_app, 'GroceryMonth.parquet')
    pth_app_RestMonth = oj(dr_Use_app, 'RestaurantMonth.parquet')

    # ----- Tables -----
    pth_table_Balance = oj(dr_app_data, 'table_Balance.pkl')
    pth_table_Groceries = oj(dr_app_data, 'table_Groceries.pkl')
    pth_table_Food = oj(dr_app_data, 'table_Food.pkl')
    pth_table_Stats = oj(dr_app_data, 'table_Stats.pkl')
    pth_table_TopExpenses = oj(dr_app_data, 'table_TopExpenses.pkl')

    # ----- Figures -----
    pth_fig_GroceryBar = oj(dr_app_data, 'fig_GroceryBar.json')
    pth_fig_GroceryBarPct = oj(dr_app_data, 'fig_GroceryBarPct.json')
    pth_fig_GroceryLR = oj(dr_app_data, 'fig_GroceryLR.json')


@dataclass
class VIS:
    vk_Variable_show = {
        'amount_CHF': 'Amount [CHF]',
        'Balance_CHF': 'Balance [CHF]',
        'category_main': 'Main category',
        'category_second': 'Second category',
        'MerchantPlace': 'City'
    }

    vk_format_col = {
        'Amount [CHF]': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')},
        'Balance [CHF]': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')}
    }

    vk_GroceryStore_col = {
        'Aldi': "#45C7F6",
        'Lidl': "#FAF263",
        'Migros': "#D052E9",
        'Coop': "#E38A04",
        'Denner': "#FA6363",
        'migrolino': "#17C528",
        'Avec': "#7563FA"
    }

    vk_Food_col = {
        'Groceries': "#45C7F6",
        'Supermarket': "#45C7F6",
        'Restaurant': "#E38A04",
        'Cafe': "#17C528",
        'Bakery': "#7563FA",
        'Bar': "#FA6363",
        'Cafeteria': "#FAF263"
    }

    s_Merchant_Grocery = ['Coop', 'Migros', 'Lidl', 'Aldi', 'Denner', 'migrolino', 'Avec']
    s_Category_Food = ['Supermarket', 'Restaurant', 'Cafe', 'Bakery', 'Bar']