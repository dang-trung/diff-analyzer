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


def severity(c: str, pct_similar: float) -> None:
    if 1.01 >= pct_similar > 0.99:
        severity = None
    elif pct_similar > 0.9:
        severity = ' *'
    elif pct_similar > 0.5:
        severity = ' **'
    else:
        severity = ' ***'
        
    if severity:
        print(f'{c}: {pct_similar * 100:.1f}%{severity}')
        