# %%
import pandas as pd
from app.config import FDP
fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

# %%
pdf = pdf[(pdf['transaction_type'] == 'expense') & (pdf['category_main'] == 'Travel')].reset_index(drop=True)
pdf['Year'] = pdf['Date'].dt.year

pdf_Vacation = pdf.groupby(['Year', 'category_second'], as_index=False)['amount_CHF'].sum().rename(columns={'amount_CHF': 'Total'})

# %%
pdf_Vacation.to_pickle(fdp.pth_table_Vacation)