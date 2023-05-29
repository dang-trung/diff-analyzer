import tomllib
from pathlib import Path

import pandas as pd
from pandas.api.types import is_datetime64_dtype
from bsc_utils.database import Database, query

from .utils import (
    num_similarity, str_similarity, severity, adjust_exchange_label, stats
)


def get_query(table: str) -> dict:
    with open(Path('tables') / f'{table}.toml', 'rb') as f:
        query_by_db = tomllib.load(f)

    return query_by_db


def get_data(query_by_db: dict) -> dict:
    oracle = query(Database.ORACLE, query_by_db.get('ORACLE'))
    mssql = query(Database.MSSQL, query_by_db.get('MSSQL'))

    return {
        'ORACLE': pd.DataFrame(oracle),
        'MSSQL': adjust_exchange_label(pd.DataFrame(mssql))
    }


def check(table: str) -> None:
    q = get_query(table)
    ignored_cols, join_cols = q.get('IGNORE_COLS'), q.get('JOIN_COLS')
    data = get_data(q)

    merge = pd.merge(
        data.get('ORACLE'),
        data.get('MSSQL'),
        on=join_cols,
        suffixes=['_O', '_M'],
        how='inner'
    )

    print(f"{'-' * 10}{table}{'-' * 10}\n")
    check_cols = [
        c for c in data.get('ORACLE').columns if c not in ignored_cols
    ]
    c_dtypes = col_dtypes(check_cols, data.get('ORACLE'))

    for c in check_cols:
        stats(data.get('ORACLE'), c)
        if c in c_dtypes.get('numerical'):
            pct_similar = merge.apply(
                lambda r: num_similarity(r[f'{c}_O'], r[f'{c}_M']), axis=1
            ).mean()

        elif c in c_dtypes.get('text'):
            pct_similar = merge.apply(
                lambda r: str_similarity(r[f'{c}_O'], r[f'{c}_M']), axis=1
            ).mean()

        severity(c, pct_similar, join_cols, merge)


def col_dtypes(cols: list, df: pd.DataFrame) -> dict:
    dtypes = {}
    dtypes['numerical'] = [c for c in cols if df[c].dtype == 'float64']
    dtypes['text'] = [
        c for c in cols if (df[c].dtype == 'O' or is_datetime64_dtype(df[c]))
    ]

    return dtypes