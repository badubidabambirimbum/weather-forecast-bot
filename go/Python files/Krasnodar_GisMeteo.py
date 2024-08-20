import os
import sys
current_dir = os.path.dirname(os.path.abspath(sys.executable))
sys.path.append(os.path.abspath(os.path.join(current_dir, "..", "..", "Parsing")))
from table import table


def main():
    t = table()
    t.update("Krasnodar", "GisMeteo")


if __name__ == '__main__':
    main()