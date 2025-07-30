import os
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Dict
oj = os.path.join
load_dotenv()


@dataclass
class Drive:
    Box: str = os.getenv('dr_Box')

    Lnd_Debit: str = oj(Box, 'lnd', 'DebitCard')
    Lnd_Viseca: str = oj(Box, 'lnd', 'Viseca')

    Raw_Debit: str = oj(Box, 'raw', 'DebitCard')
    Raw_Viseca: str = oj(Box, 'raw', 'Viseca')

    Rfn_Debit: str = oj(Box, 'rfn', 'DebitCard')
    Rfn_Debit_Master: str = oj(Rfn_Debit, 'Master')
    Rfn_Debit_labelAI: str = oj(Rfn_Debit, 'labelAI')
    Rfn_Debit_labelAI_cleaned: str = oj(Rfn_Debit, 'labelAI_cleaned')

    Rfn_Viseca: str = oj(Box, 'rfn', 'Viseca')
    Rfn_Viseca_labelAI: str = oj(Rfn_Viseca, 'labelAI')
    Rfn_Viseca_labelAI_cleaned: str = oj(Rfn_Viseca, 'labelAI_cleaned')

    Use_Debit: str = oj(Box, 'use', 'DebitCard')
    Use_Viseca: str = oj(Box, 'use', 'Viseca')
    Use_Bank_ZKB: str = oj(Box, 'use', 'Bank_ZKB')


@dataclass
class Filename:
    Cache_label = 'DebitCard_label_cache.json'
    Cache_Debit = 'DebitCard_cache.json'
    Cache_Viseca = 'Viseca_cache.json'
    
    Master_Debit = 'Master_DebitCard.parquet'
    Master_Viseca = 'Master_Viseca.parquet'
