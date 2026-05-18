# %%
import pandas as pd
from app.config import FDP
from app.vis.figure import Fig

fdp = FDP()

# %%
pdf = pd.read_parquet(fdp.pth_Master_BankZKB)

pdf_CategoryCorrelation = pd.read_pickle(fdp.pth_table_CategoryCorrelation)

# %%
F = Fig()

fig = F.fig_CategoryCorrelation(
    pdf=pdf_CategoryCorrelation,
    col_category="category_second",
    Period="Month",
    Year=2025,
)

fig.show()
