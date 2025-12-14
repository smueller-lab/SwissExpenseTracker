# %%
import pandas as pd
from app.config import FDP
from app.vis.figure import Fig
fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)
pdf_Food = pd.read_pickle(fdp.pth_table_Food)
pdf_Grocery = pd.read_pickle(fdp.pth_table_Groceries)
pdf_Balance = pd.read_pickle(fdp.pth_table_Balance)
pdf_CatMain = pd.read_pickle(fdp.pth_table_CatMain)

# %%
F = Fig()
pdf_CatMain = pdf_CatMain[pdf_CatMain['Year'] == 'All']

fig = F.fig_DonutCategoryMain(pdf_CatMain)

fig.show()