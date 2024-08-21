import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from parsing.table import table


def main():
    t = table()
    t.update("Ekaterinburg", "GisMeteo")


if __name__ == '__main__':
    main()