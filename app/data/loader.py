import pandas as pd
from app.config import FDP, VIS


class DataLoader:
    def __init__(self):
        self.fdp = FDP()
        self.vis = VIS()
        self._load_all_data()


    def _load_all_data(self):
        self.pdf_Master = pd.read_parquet(self.fdp.pth_Master_BankZKB)
        self.pdf_Balance = pd.read_pickle(self.fdp.pth_table_Balance)
        self.pdf_Grocery = pd.read_pickle(self.fdp.pth_table_Groceries)
        self.pdf_Food = pd.read_pickle(self.fdp.pth_table_Food)
        self.pdf_CatMain = pd.read_pickle(self.fdp.pth_table_CatMain)
        self.z_StatsTable = pd.read_pickle(self.fdp.pth_table_Stats)
        self.pdf_TopCat = pd.read_pickle(self.fdp.pth_table_TopCat)

        self.pdf_TopExpenses = pd.read_pickle(self.fdp.pth_table_TopExpenses)
        self.pdf_TopExpenses['Date'] = self.pdf_TopExpenses['Date'].dt.strftime('%d-%m-%Y')
        self.scol_TopExpenses = [{'name': col, 'id': col, **self.vis.vk_format_col.get(col, {'type': 'text'})} for col in self.pdf_TopExpenses.columns]

        self.pdf_NetBalanceMonth = pd.read_pickle(self.fdp.pth_table_NetBalanceMonth)
        self.scol_NetBalanceMonth = [{"name": col, "id": col, **self.vis.vk_format_col.get(col, {'type': 'text'})} for col in ['Month', 'expense', 'income', 'NetBalance']]