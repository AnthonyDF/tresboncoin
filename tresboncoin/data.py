import pandas as pd
import os


train_set = os.path.dirname(os.path.abspath(__file__)) + "/data/master_with_fuzzy_and_cleaning.csv"


def get_data():
    '''returns training set DataFrames'''
    df_train = pd.read_csv(train_set)
    return df_train


def clean_data(df):
    ''' return clean dataframe '''
    df = df.drop(["Name",
                  "PassengerId",
                  "Ticket",
                  "Embarked",
                  "Parch",
                  "SibSp",
                  "Cabin"], axis=1)
    return df


if __name__ == '__main__':
    df_train = get_data()
    print("Train dataframe loaded. Shape:" + str(df_train.shape))
