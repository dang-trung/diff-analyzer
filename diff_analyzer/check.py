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
    full_oracle = query(Database.ORACLE, query_by_db.get('FULL_ORACLE'))

    return {
        'ORACLE': pd.DataFrame(oracle),
        'MSSQL': adjust_exchange_label(pd.DataFrame(mssql)),
        'FULL': pd.DataFrame(full_oracle)
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

    print(f"{'-' * 10}{table}{'-' * 10}")
    print(f"\nNum obs: {len(data.get('ORACLE'))}")
    check_cols = [
        c for c in data.get('ORACLE').columns if c not in ignored_cols
    ]
    c_dtypes = col_dtypes(check_cols, data.get('ORACLE'))

    merge_out = pd.merge(
        data.get('ORACLE')[join_cols],
        data.get('MSSQL')[join_cols],
        on=join_cols,
        suffixes=['_O', '_M'],
        how='outer',
        indicator=True
    )
    sample_missing = {}
    in_mssql_not_oracle = len(merge_out[merge_out['_merge'] == 'right_only']
                             ) / len(merge_out)
    if in_mssql_not_oracle > 0:
        sp = merge_out[merge_out['_merge'] == 'right_only'
                      ][join_cols].sample(1).iloc[0].to_dict()
        if sp:
            sample_missing['in_mssql_not_oracle'] = f' - e.g. {sp}'

    in_oracle_not_mssql = len(merge_out[merge_out['_merge'] == 'left_only']
                             ) / len(merge_out)
    if in_oracle_not_mssql > 0:
        sp = merge_out[merge_out['_merge'] == 'left_only'
                      ][join_cols].sample(1).iloc[0].to_dict()
        if sp:
            sample_missing['in_oracle_not_mssql'] = f' - e.g. {sp}'

    in_both = len(merge_out[merge_out['_merge'] == 'both']) / len(merge_out)

    print(
        f'\nMSSQL has, Oracle lacks: {in_mssql_not_oracle * 100:.1f}%'
        f'{sample_missing.get("in_mssql_not_oracle", "")}'
    )
    print(
        f'Oracle has, MSSQL lacks: {in_oracle_not_mssql * 100:.1f}%'
        f'{sample_missing.get("in_oracle_not_mssql", "")}'
    )
    print(f'In both: {in_both * 100:.1f}%')

    for c in data.get('FULL').columns:
        stats(data.get('FULL'), c, join_cols)
        if c in check_cols:
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