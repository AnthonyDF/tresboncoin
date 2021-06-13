import pandas as pd
from utils import km_per_year
import os


train_set = os.path.dirname(os.path.abspath(__file__)) + "/data/master_with_fuzzy_and_cleaning.csv"


def get_data():
    '''returns training set DataFrames'''
    df_train = pd.read_csv(train_set)
    return df_train


def clean_data(df):
    ''' return clean dataframe '''
    df = df[~df["brand_db"].isnull()]
    df = df[~df["model_db"].isnull()]
    df = df[~df["type_db"].isnull()]
    df = df[~df["category_db"].isnull()]
    df = df[(df["bike_year"]>=1970) & (df["bike_year"]<=2022)]
    df = df[(df["mileage"]>=1000) & (df["mileage"]<=80000)]
    df = df[(df["price"]>=1000) & (df["price"]<30000)]
    df.drop(['url', 'uniq_id', 'model_db', "type_db", 'brand', "model", "brand_db"], axis=1, inplace=True)
    df['km/year'] = df.apply(lambda x: km_per_year(x['mileage'], x['bike_year']), axis=1)
    return df


if __name__ == '__main__':
    df_train = get_data()
    df_train = clean_data(df_train)
    print("Train dataframe loaded. Shape:" + str(df_train.shape))
    print("Train dataframe columns:" + str(list(df_train.columns)))
