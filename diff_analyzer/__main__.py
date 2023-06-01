from pathlib import Path
import sys

from .check import check

if __name__ == '__main__':
    output_file = open(
        Path('results') / f'{sys.argv[1]}.txt', 'w', encoding='utf-8'
    )
    sys.stdout = output_file
    check(sys.argv[1])
    output_file.close()
