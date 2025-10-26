import os
from dataclasses import dataclass
from dotenv import load_dotenv
from dash.dash_table import FormatTemplate
oj = os.path.join
load_dotenv()

@dataclass
class config:
    d_Tick_balance: int = 5000


@dataclass
class FDP:
    dr_Box = os.getenv('dr_Box')
    dr_app = os.getenv('dr_app')
    
    dr_Use_Debit = oj(dr_Box, 'use', 'DebitCard')
    dr_Use_Viseca = oj(dr_Box, 'use', 'Viseca')
    dr_Use_Bank_ZKB = oj(dr_Box, 'use', 'Bank_ZKB')
    dr_app_data = oj(dr_app, 'data')

    pth_Master_Debit = oj(dr_Use_Debit, 'Master_DebitCard.parquet')
    pth_Master_Viseca = oj(dr_Use_Viseca, 'Master_Viseca.parquet')
    pth_Master_BankZKB = oj(dr_Use_Bank_ZKB, 'Master_Bank_ZKB.parquet')

    pth_table_Stats = oj(dr_app_data, 'table_Stats.pkl')
    pth_table_TopExpenses = oj(dr_app_data, 'table_TopExpenses.pkl')
    pth_fig_BalancePerDay = oj(dr_app_data, 'fig_BalancePerDay.json')


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