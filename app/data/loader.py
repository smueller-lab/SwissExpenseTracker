import pandas as pd
from app.config import FDP, VIS


class DataLoader:
    def __init__(self):
        self.fdp = FDP()
        self.vis = VIS()
        self._load_all_data()

    
    def get_scol_DashTable(self, pdf: pd.DataFrame):
        return [{'name': col, 'id': col, **self.vis.vk_format_col.get(col, {'type': 'text'})} for col in pdf.columns]
    

    def apply_Variable_show(self, pdf: pd.DataFrame):
        pdf = pdf.rename(columns=self.vis.vk_Variable_show)
        return pdf


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
        self.pdf_TopExpenses = self.apply_Variable_show(self.pdf_TopExpenses)

        self.pdf_NetBalanceMonth = pd.read_pickle(self.fdp.pth_table_NetBalanceMonth)[['Month', 'expense', 'income', 'NetBalance']]
        self.pdf_NetBalanceMonth = self.apply_Variable_show(self.pdf_NetBalanceMonth)


    def get_TopExpenses_Category_Month(self, Category: str, Month: str):
        pdf = self.pdf_Master.copy()
        pdf['Month'] = pd.to_datetime(pdf['Date']).dt.to_period('M')
        pdf['Date'] = pdf['Date'].dt.strftime('%d-%m-%Y')

        # filter pdf for given Category and Month
        pdf = pdf[
            (pdf['category_main'] == Category) &
            (pdf['transaction_type'] == 'expense') &
            (pdf['Month'] == Month)   
        ].reset_index(drop=True)

        # sort pdf
        pdf = pdf.sort_values(by='amount_CHF', ascending=False).head(7)

        # select columns
        s_col = ['Date', 'amount_CHF', 'Merchant', 'category_second', 'MerchantPlace']
        pdf = pdf[s_col].copy()

        pdf = self.apply_Variable_show(pdf)

        return pdf