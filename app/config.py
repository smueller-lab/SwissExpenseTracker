import os
from dataclasses import dataclass
from dotenv import load_dotenv
from dash.dash_table import FormatTemplate
from Pipeline.cfg_cleaning import config
cfg = config()
oj = os.path.join
load_dotenv()

@dataclass
class config:
    #dTick
    vk_dTick_Grocery = {'Monthly': 100, 'Yearly': 1000, 'Visit': 20}
    vk_dTick_Food = {'Monthly': 200, 'Yearly': 1000, 'Visit': 20}
    dTick_Balance: int = 5000
    dTick_Pct: int = 20
    dTick_Vacation: int = 1000
    dTick_Transport: int = 1000

    #npixel
    vk_npixel_Food = {'Monthly': 50, 'Yearly': 40, 'Visit': 50}
    vk_npixel_Grocery = {'Monthly': 50, 'Yearly': 80, 'Visit': 50}
    npixel_Balance: int = 80
    npixel_Pct: int = 80
    npixel_Vacation: int = 80
    npixel_Transport: int = 80

    # default ry_Axis
    ry_Axis_Pct = [0, 100]


@dataclass
class FDP:
    dr_Box = os.getenv('dr_Box')
    dr_app = os.getenv('dr_app')
    
    dr_Use_Debit = oj(dr_Box, 'use', 'DebitCard')
    dr_Use_Viseca = oj(dr_Box, 'use', 'Viseca')
    dr_Use_Bank_ZKB = oj(dr_Box, 'use', 'Bank_ZKB_RFN')
    dr_Use_app = oj(dr_Box, 'use', 'app')
    dr_app_data = oj(dr_app, 'data', 'tables')

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
    pth_table_CatMain = oj(dr_app_data, 'table_CatMain.pkl')
    pth_table_TopCat = oj(dr_app_data, 'table_TopCategory.pkl')
    pth_table_NetBalanceMonth = oj(dr_app_data, 'table_NetBalanceMonth.pkl')
    pth_table_Vacation = oj(dr_app_data, 'table_Vacation.pkl')
    pth_table_Transport = oj(dr_app_data, 'table_Transport.pkl')
    pth_table_TransportHeatmap = oj(dr_app_data, 'table_TransportHeatmap.pkl')


@dataclass
class VIS:
    vk_Variable_show = {
        'amount_CHF': 'Amount [CHF]',
        'category_main': 'Main category',
        'category_second': 'Second category',
        'MerchantPlace': 'Location',
        'expense': 'Expense',
        'income': 'Income'
    }

    vk_format_col = {
        'Amount [CHF]': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')},
        'Expense': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')},
        'Income': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')},
        'NetBalance': {'type': 'numeric', 'format': FormatTemplate.money(2).symbol('')},
    }

    vk_GroceryStore_col = {
        cfg.nm_GroceryShop3: "#45C7F6",
        cfg.nm_GroceryShop4: "#FAF263",
        cfg.nm_Supermarket: "#D052E9",
        cfg.nm_GroceryShop1: "#E38A04",
        cfg.nm_GroceryShop2: "#FA6363",
        cfg.nm_ShopSmall: "#17C528",
        cfg.nm_KioskLate: "#7563FA"
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

    s_Merchant_Grocery = [cfg.nm_GroceryShop1, cfg.nm_Supermarket, cfg.nm_GroceryShop4, cfg.nm_GroceryShop3, cfg.nm_GroceryShop2, cfg.nm_ShopSmall, cfg.nm_KioskLate]
    s_Category_Food = ['Supermarket', 'Restaurant', 'Cafe', 'Bakery', 'Bar']
    s_Col_Text = ['Date', 'Month', 'Merchant', 'Main category', 'Second category', 'Location']