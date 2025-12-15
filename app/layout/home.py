from datetime import datetime
from dash import html
from app.components.cards import make_figure_card, make_card_selectBox, make_table_card, make_number_card, make_TopCategory_card
from app.vis.figure import Fig
F = Fig()


def layout(data):
    z_TopCategory = data.pdf_TopCat.sort_values(by='amount_MonthLast', ascending=False).iloc[0]

    return html.Div([
        html.Div([
            make_number_card('Current Balance', data.z_StatsTable['Balance_current']),
            make_number_card('Average net Balance (3 months)', data.z_StatsTable['Balance_net_3months']),
            make_number_card('Average net Balance (12 months)', data.z_StatsTable['Balance_net_12months']),
            make_number_card(f'Net Balance ({datetime.today().year})', data.z_StatsTable['Balance_net_currentYear']),
            make_TopCategory_card(
                title='Top Category',
                MonthLast=z_TopCategory['MonthLast'],
                Category=z_TopCategory['category_main'],
                amount_MonthLast=z_TopCategory['amount_MonthLast'],
                amount_MonthPrev=z_TopCategory['amount_MonthPrev'],
                amount_12m_avg=z_TopCategory['amount_AVG_12m'],
                diff_prev_pct=z_TopCategory['diff_prev_pct'],
                diff_12m_pct=z_TopCategory['diff_12m_pct']
            ),
        ], className="cards-container"),

        html.Div([
            make_card_selectBox('Expense distribution', data.pdf_CatMain),
            make_figure_card("Balance Progression", F.fig_BalancePerDay(data.pdf_Balance)),
            make_table_card(
                title='Top 20 Expenses',
                s_col=data.scol_TopExpenses,
                data=data.pdf_TopExpenses.to_dict('records'),
                table_id="table-top-expenses"
            ),
        ], className='cards-grid')
    ])