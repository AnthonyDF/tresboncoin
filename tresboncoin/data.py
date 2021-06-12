import pandas as pd
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
    return df


if __name__ == '__main__':
    df_train = get_data()
    df_train = clean_data(df_train)
    print("Train dataframe loaded. Shape:" + str(df_train.shape))
