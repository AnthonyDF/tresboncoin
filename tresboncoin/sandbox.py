import re
import os
import pandas as pd

if __name__ == '__main__':
    print(pd.read_csv(os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv").columns)
