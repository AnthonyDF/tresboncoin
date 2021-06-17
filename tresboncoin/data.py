from tresboncoin.utils import km_per_year
from datetime import datetime
from tresboncoin.fuzzy_match import fuzzy_match
import pandas as pd
import os

raw_data = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv"
history_data = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv"
moto_database = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"


def get_raw_data():
    '''
    raw data from conctenation after scraping
    '''
    return pd.read_csv(raw_data)


def get_data():
    '''returns training set DataFrames'''
    return pd.read_csv(history_data)


def get_motorcycle_db():
    '''returns motorcycle database'''
    return pd.read_csv(moto_database)


def get_new_data(master_data, history_data):
    '''
    function to return new rows in a dataframe compared to the history
    '''
    history_data['checker'] = 1
    new_data = master_data.merge(history_data[['uniq_id','checker']],how='left', left_on='uniq_id', right_on='uniq_id')
    new_data = new_data[new_data.checker.isnull()]
    new_data.drop(columns=['checker'], inplace=True)
    return new_data


def clean_raw_data(df):
    ''' return clean dataframe '''
    df = df[~df["brand"].isnull()]
    df = df[~df["model"].isnull()]
    df = df[(df["bike_year"] >= 1900) & (df["bike_year"] <= datetime.now().year)]
    df = df[(df["mileage"] >= 100) & (df["mileage"] <= 150000)]
    df = df[(df["price"] >= 100) & (df["price"] < 40000)]
    df = df[df["engine_size"] >= 49]

    # Clean same annonce with mutiple prices (keep lowest price)
    df.sort_values('price', ascending=False, inplace=True)
    df.drop_duplicates(subset=['uniq_id'], keep='first', inplace=True)
    return df


def append(new_data_matched, history_data):
    new_history = history_data.append(new_data_matched)
    new_history.to_csv(os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv", index=False)
    return new_history


def clean_data(df):
    ''' return clean dataframe '''
    df = df[~df["brand_db"].isnull()]
    df = df[~df["model_db"].isnull()]
    df = df[~df["category_db"].isnull()]
    df = df[~df["engine_size"].isnull()]
    df.drop_duplicates(subset=['model_db', 'brand_db', 'price', 'engine_size', 'mileage', 'bike_year'], inplace=True)

    # remove categories with low count of bikes
    category_count_threshold = 100
    groupby_category = df.groupby('category_db').agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    drop_category = groupby_category[groupby_category .Count < category_count_threshold].index.to_list()
    drop_category.append('unspecified category')

    # remove brands with low count of bikes
    brand_count_threshold = 10
    groupby_brand = df.groupby('brand_db').agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    drop_brand = groupby_brand[groupby_brand.Count < brand_count_threshold].index.to_list()
    df = df[df.brand_db.isin(drop_brand) == False]

    # remove models with low count of bikes
    #model_count_threshold = 1
    #groupby_model = df.groupby(['model_db']).agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    #drop_model = groupby_model[groupby_model.Count < model_count_threshold].index.to_list()
    #df = df[df.model_db.isin(drop_model) == False]

    # feature engineering
    df['km/year'] = df.apply(lambda x: km_per_year(x['mileage'], x['bike_year']), axis=1)

    return df[['brand_db', 'bike_year', 'mileage', 'engine_size', 'km/year', "price"]]


if __name__ == '__main__':
    new_data = get_new_data(clean_raw_data(get_raw_data()), get_data())
    print("New data to be matched. Shape:" + str(new_data.shape))
    if not new_data.empty:
        print('Fuzzy match in progress, wait...')
        new_data_matched = fuzzy_match(new_data, get_motorcycle_db())
        print("Fuzzy match completed. Shape:" + str(new_data_matched.shape))
        history = append(new_data_matched, get_data())
        print("New dataframe avaialble. Shape:" + str(history.shape))
    else:
        print('No new data to match')
    df_train = get_data()
    df_train = clean_data(df_train)
    print("Train dataframe loaded. Shape:" + str(df_train.shape))
    print("Train dataframe columns:" + str(list(df_train.columns)))
