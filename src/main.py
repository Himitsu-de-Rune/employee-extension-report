import pandas as pd
import numpy as np
import re

fin_df = pd.read_csv('data/input/financial_data.csv')
pro_df = pd.read_csv('data/input/prolongations.csv')

months_ru = {
    'январь': '01', 'февраль': '02', 'март': '03', 'апрель': '04',
    'май': '05', 'июнь': '06', 'июль': '07', 'август': '08',
    'сентябрь': '09', 'октябрь': '10', 'ноябрь': '11', 'декабрь': '12'
}

def ru_month_to_ym(name):
    match = re.match(r'([А-Яа-я]+)\s+(\d{4})', name.lower())
    if not match:
        return None
    month_str, year = match.groups()
    month_num = months_ru.get(month_str)
    if month_num is None:
        return None
    return f"{year}-{month_num}"

month_cols_ru = []
for col in fin_df.columns:
    ym = ru_month_to_ym(col)
    if ym:
        month_cols_ru.append(col)

def clean_amount(value):
    if pd.isna(value):
        return 0.0
    
    if value in ['стоп', 'в ноль', 'end']:
        return 0.0
    
    cleaned = value.replace(',', '.').replace('\xa0', '')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

fin_long = fin_df.melt(
    id_vars=['id', 'Причина дубля', 'Account'],
    value_vars=month_cols_ru,
    var_name='month_raw',
    value_name='amount'
)

fin_long['month'] = fin_long['month_raw'].apply(lambda x: pd.Period(ru_month_to_ym(x), freq='M').strftime('%Y-%m'))
fin_long['amount'] = fin_long['amount'].apply(clean_amount)

shipments = fin_long.groupby(['id', 'month'], as_index=False)['amount'].sum()

pro_df['month'] = pro_df['month'].apply(lambda x: pd.Period(ru_month_to_ym(x), freq='M').strftime('%Y-%m'))
pro_df = pro_df.rename(columns={'month': 'last_month', 'AM': 'manager'})
pro_df = pro_df.sort_values(by='last_month')
pro_unique = pro_df[['id', 'last_month', 'manager']].drop_duplicates(subset='id', keep='last')

shipments = shipments.merge(pro_unique, on='id', how='left')
shipments = shipments.dropna(subset=['manager', 'last_month'])


min_req_month = '2022-11'
max_req_month = '2023-12'

months_2023 = pd.period_range('2023-01', '2023-12', freq='M').strftime('%Y-%m').tolist()
all_months = pd.period_range(min_req_month, max_req_month, freq='M').strftime('%Y-%m').tolist()

def prev_month(m):
    return (pd.Period(m, freq='M') - 1).strftime('%Y-%m')

all_ids = shipments['id'].unique()
idx = pd.MultiIndex.from_product([all_ids, all_months], names=['id', 'month'])

shipments_full = pd.DataFrame(index=idx).reset_index()
shipments_full = shipments_full.merge(shipments, on=['id', 'month'], how='left')
shipments_full['amount'] = shipments_full['amount'].fillna(0)


rows = []

for M in months_2023:
    M1 = prev_month(M)
    M2 = prev_month(M1)

    finished_M1 = shipments_full[shipments_full['last_month'] == M1]
    if not finished_M1.empty:
        grp_M1 = finished_M1[finished_M1['month'].isin([M1, M])].groupby(['manager', 'month'])['amount'].sum().unstack(fill_value=0)
        for col in [M1, M]:
            if col not in grp_M1.columns:
                grp_M1[col] = 0
        grp_M1 = grp_M1.rename(columns={M1: 'K1_base', M: 'K1_prolong'})
        grp_M1['K1'] = np.where(grp_M1['K1_base'] > 0,
                                grp_M1['K1_prolong'] / grp_M1['K1_base'],
                                np.nan)
        grp_M1 = grp_M1.reset_index()[['manager', 'K1_base', 'K1_prolong', 'K1']]
        grp_M1['month'] = M
    else:
        grp_M1 = pd.DataFrame(columns=['manager', 'K1_base', 'K1_prolong', 'K1', 'month'])

    finished_M2 = shipments_full[shipments_full['last_month'] == M2]
    if not finished_M2.empty:
        ship_M1 = finished_M2[finished_M2['month'] == M1][['id', 'amount']].rename(columns={'amount': 'amount_M1'})
        finished_M2 = finished_M2.merge(ship_M1, on='id', how='left')
        finished_M2['amount_M1'] = finished_M2['amount_M1'].fillna(0)
        not_prolonged = finished_M2[finished_M2['amount_M1'] == 0]

        if not not_prolonged.empty:
            grp_M2 = not_prolonged[not_prolonged['month'].isin([M2, M])].groupby(['manager', 'month'])['amount'].sum().unstack(fill_value=0)
            for col in [M2, M]:
                if col not in grp_M2.columns:
                    grp_M2[col] = 0
            grp_M2 = grp_M2.rename(columns={M2: 'K2_base', M: 'K2_prolong'})
            grp_M2['K2'] = np.where(grp_M2['K2_base'] > 0,
                                    grp_M2['K2_prolong'] / grp_M2['K2_base'],
                                    np.nan)
            grp_M2 = grp_M2.reset_index()[['manager', 'K2_base', 'K2_prolong', 'K2']]
            grp_M2['month'] = M
        else:
            grp_M2 = pd.DataFrame(columns=['manager', 'K2_base', 'K2_prolong', 'K2', 'month'])
    else:
        grp_M2 = pd.DataFrame(columns=['manager', 'K2_base', 'K2_prolong', 'K2', 'month'])

    month_res = pd.merge(grp_M1, grp_M2, on=['manager', 'month'], how='outer')
    rows.append(month_res)

detail = pd.concat(rows, ignore_index=True)
detail = detail.sort_values(['month', 'manager']).reset_index(drop=True)



dept_data = detail.groupby('month').agg(
    K1_base_sum=('K1_base', 'sum'),
    K1_prolong_sum=('K1_prolong', 'sum'),
    K2_base_sum=('K2_base', 'sum'),
    K2_prolong_sum=('K2_prolong', 'sum')
).reset_index()

dept_data['K1_coef'] = np.where(dept_data['K1_base_sum'] > 0,
                                dept_data['K1_prolong_sum'] / dept_data['K1_base_sum'],
                                np.nan)
dept_data['K2_coef'] = np.where(dept_data['K2_base_sum'] > 0,
                                dept_data['K2_prolong_sum'] / dept_data['K2_base_sum'],
                                np.nan)

dept_report = dept_data[['month',
                         'K1_base_sum', 'K1_prolong_sum', 'K1_coef',
                         'K2_base_sum', 'K2_prolong_sum', 'K2_coef']].copy()
dept_report.columns = [
    'Месяц',
    'К1_к_пролонгации', 'К1_пролонгировано', 'К1_Коэффициент',
    'К2_к_пролонгации', 'К2_пролонгировано', 'К2_Коэффициент'
]
dept_report = dept_report.round(2)
dept_report.to_csv('data/output/department_year_report.csv', index=False, encoding='utf-8-sig')


manager_year = detail.groupby('manager').agg(
    K1_base_total=('K1_base', 'sum'),
    K1_prolong_total=('K1_prolong', 'sum'),
    K2_base_total=('K2_base', 'sum'),
    K2_prolong_total=('K2_prolong', 'sum')
).reset_index()

manager_year['K1_coef'] = np.where(manager_year['K1_base_total'] > 0,
                                   manager_year['K1_prolong_total'] / manager_year['K1_base_total'],
                                   np.nan)
manager_year['K2_coef'] = np.where(manager_year['K2_base_total'] > 0,
                                   manager_year['K2_prolong_total'] / manager_year['K2_base_total'],
                                   np.nan)

manager_report = manager_year[['manager',
                               'K1_base_total', 'K1_prolong_total', 'K1_coef',
                               'K2_base_total', 'K2_prolong_total', 'K2_coef']].copy()
manager_report.columns = [
    'Менеджер',
    'К1_к_пролонгации', 'К1_пролонгировано', 'К1_Коэффициент',
    'К2_к_пролонгации', 'К2_пролонгировано', 'К2_Коэффициент'
]
manager_report = manager_report.round(2)
manager_report.to_csv('data/output/manager_year_report.csv', index=False, encoding='utf-8-sig')


pivot_K1 = detail.pivot(index='manager', columns='month', values='K1')
pivot_K2 = detail.pivot(index='manager', columns='month', values='K2')

pivot_combined = pd.concat([pivot_K1, pivot_K2], axis=1, keys=['K1', 'K2'])
pivot_combined.index.name = 'Менеджер'
pivot_combined = pivot_combined.round(2)
pivot_combined.to_csv('data/output/manager_monthly_coeffs.csv', encoding='utf-8-sig')