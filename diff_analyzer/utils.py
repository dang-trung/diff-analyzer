import pandas as pd
import numpy as np


def num_similarity(x: float, y: float) -> float:
    if y == 0:
        if x == 0:
            return 1
        else:
            return 0
    else:
        if x == 0:
            return 0
        elif x != 0:
            return 1 - abs((x - y) / ((x + y) / 2))


def str_similarity(x: str, y: str) -> float:
    if x == y:
        return 1
    else:
        return 0


def severity(
    c: str, pct_similar: float, join_cols: list, df: pd.DataFrame
) -> None:
    if 1.01 >= pct_similar > 0.99:
        severity = None
    elif pct_similar > 0.9:
        severity = ' *'
    elif pct_similar > 0.5:
        severity = ' **'
    else:
        severity = ' ***'

    if severity:
        sample = df[df[f'{c}_O'] != df[f'{c}_M']][
            join_cols + [f'{c}_O', f'{c}_M']].sample(1).to_dict()
        print(f'DUPLICATES: {pct_similar * 100:.1f}%{severity} - e.g. {sample}\n')


def adjust_exchange_label(df: pd.DataFrame) -> None:
    if 'EXCHANGE_CODE' in df.columns:
        df = df.replace(
            {'EXCHANGE_CODE': {
                'HOSTC': 'HOSE',
                'HASTC': 'HNX',
                'OTH': 'OTC'
            }}
        )

    return df


def stats(df: pd.DataFrame, col: str) -> float:
    c = df[col]
    num_obs = len(c)
    if c.dtype == 'float64':
        col_stats = {
            'MISSING': c.isna().sum() / num_obs,
            'INFINITE': np.isinf(c).sum() / num_obs,
            'ZERO': (c == 0).sum() / num_obs,
        }
    else:
        col_stats = {
            'MISSING': c.isna().sum() / num_obs,
        }
    
    col_stats = ' - '.join([f'{k}: {v * 100:.1f}%' for k, v in col_stats.items()])
    
    print(f'[{col}]\n{col_stats}')
