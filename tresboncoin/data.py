# from tresboncoin.utils import km_per_year
import string
from rapidfuzz import process
import numpy as np
from datetime import datetime
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

    # Clean same annonce with mutiple prices (keep lowest price)
    df.sort_values('price', ascending=False, inplace=True)
    df.drop_duplicates(subset=['uniq_id'], keep='first', inplace=True)
    return df


def remove_punctuations(text):
    '''
    remove punctuation in a string
    '''
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text


def fuzzy_match(new_data, moto_database):

    # CLEAN BRAND AND MODEL
    new_data.dropna(subset=['model','brand'],inplace=True)
    # lower and remove spaces
    new_data.brand = new_data.brand.str.lower().str.replace(' ','')
    new_data.model = new_data.model.str.lower().str.replace(' ','')
    # cremove punctuation
    new_data.brand = new_data.brand.apply(remove_punctuations)
    new_data.model = new_data.model.apply(remove_punctuations)

    # MATCH BRAND NAME
    def match_brand(choices, to_match):
        return process.extractOne(to_match, choices)

    new_data['fuzzy_brand_result'] = new_data.apply(lambda x: match_brand(
        [str(x) for x in moto_database.brand_db.unique().tolist()],
        x['brand']),
        axis=1)

    # unpack results
    def unpack_tuple_name(result):
        try:
            return result[0]
        except:
            return np.nan

    def unpack_tuple_score(result):
        try:
            return result[1]
        except:
            return np.nan

    new_data['fuzzy_score'] = new_data['fuzzy_brand_result'].apply(unpack_tuple_score)
    new_data['fuzzy_brand'] = new_data['fuzzy_brand_result'].apply(unpack_tuple_name)
    new_data.drop(columns=['fuzzy_brand_result'], inplace=True)
    new_data.dropna(subset=['fuzzy_brand'], inplace=True)

    # MATCH MODEL
    # list of models, submodel...
    def choices(brand, type_name):
        choices = moto_database[moto_database.brand_db==brand][type_name].unique().tolist()
        return [str(x) for  x in choices]

    def match_model(choices, to_match):
        return process.extractOne(to_match, choices)

    new_data['fuzzy_result_model'] = new_data.apply(lambda x:
                                                  match_model(
                                                      choices(x['fuzzy_brand'], 'model_db'),
                                                      x['model']),
                                                  axis=1)

    new_data['fuzzy_result_submodel'] = new_data.apply(lambda x:
                                                     match_model(
                                                         choices(x['fuzzy_brand'], 'model_submodel_db'),
                                                         x['model']),
                                                     axis=1)

    new_data['fuzzy_result_submodel_inv']= new_data.apply(lambda x:
                                                     match_model(
                                                         choices(x['fuzzy_brand'],'model_submodel_inv_db'),
                                                         x['model']),
                                                     axis=1)

    new_data['fuzzy_result_model_inv']= new_data.apply(lambda x:
                                                     match_model(
                                                         choices(x['fuzzy_brand'],'model_inv_db'),
                                                         x['model']),
                                                     axis=1)

    new_data['fuzzy_result_model_size']= new_data.apply(lambda x:
                                                     match_model(
                                                         choices(x['fuzzy_brand'],'model_size_db'),
                                                         x['model']),
                                                     axis=1)

    new_data['fuzzy_result_model_size_inv']= new_data.apply(lambda x:
                                                     match_model(
                                                         choices(x['fuzzy_brand'],'model_size_inv_db'),
                                                         x['model']),
                                                     axis=1)

    new_data['fuzzy_model'] = new_data['fuzzy_result_model'].apply(unpack_tuple_name)
    new_data['fuzzy_model_score'] = new_data['fuzzy_result_model'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_model', inplace=True)

    new_data['fuzzy_model_inv'] = new_data['fuzzy_result_model_inv'].apply(unpack_tuple_name)
    new_data['fuzzy_model_inv_score'] = new_data['fuzzy_result_model_inv'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_model_inv', inplace=True)

    new_data['fuzzy_submodel'] = new_data['fuzzy_result_submodel'].apply(unpack_tuple_name)
    new_data['fuzzy_submodel_score'] = new_data['fuzzy_result_submodel'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_submodel', inplace=True)

    new_data['fuzzy_submodel_inv'] = new_data['fuzzy_result_submodel_inv'].apply(unpack_tuple_name)
    new_data['fuzzy_submodel_inv_score'] = new_data['fuzzy_result_submodel_inv'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_submodel_inv', inplace=True)

    new_data['fuzzy_model_size'] = new_data['fuzzy_result_model_size'].apply(unpack_tuple_name)
    new_data['fuzzy_model_size_score'] = new_data['fuzzy_result_model_size'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_model_size', inplace=True)

    new_data['fuzzy_model_size_inv'] = new_data['fuzzy_result_model_size_inv'].apply(unpack_tuple_name)
    new_data['fuzzy_model_size_inv_score'] = new_data['fuzzy_result_model_size_inv'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_model_size_inv', inplace=True)

    def is_best(fuzzy_model_score,
                fuzzy_model_inv_score,
                fuzzy_submodel_score,
                fuzzy_submodel_inv_score,
                fuzzy_model_size_score,
                fuzzy_model_size_inv_score):

        '''
        function to define the best fuzzy matching score
        '''
        scores = [float(fuzzy_model_score),
                  float(fuzzy_model_inv_score),
                  float(fuzzy_submodel_score),
                  float(fuzzy_submodel_inv_score),
                  float(fuzzy_model_size_score),
                  float(fuzzy_model_size_inv_score)]

        max_score = max(scores)
        max_score_postion = scores.index(max_score)

        return max_score_postion

    new_data['fuzzy_model_score'] = new_data['fuzzy_model_score'].fillna(0)
    new_data['fuzzy_model_inv_score'] = new_data['fuzzy_model_inv_score'].fillna(0)
    new_data['fuzzy_submodel_score'] = new_data['fuzzy_submodel_score'].fillna(0)
    new_data['fuzzy_submodel_inv_score'] = new_data['fuzzy_submodel_inv_score'].fillna(0)
    new_data['fuzzy_model_size_score'] = new_data['fuzzy_model_size_score'].fillna(0)
    new_data['fuzzy_model_size_inv_score'] = new_data['fuzzy_model_size_inv_score'].fillna(0)

    new_data['is_best'] = new_data.apply(
        lambda x: is_best(
            x['fuzzy_model_score'],
            x['fuzzy_model_inv_score'],
            x['fuzzy_submodel_score'],
            x['fuzzy_submodel_inv_score'],
            x['fuzzy_model_size_score'],
            x['fuzzy_model_size_inv_score'],),
        axis=1)

    new_data.dropna(subset=['is_best'], inplace=True)

    data_model = new_data.copy()[new_data.is_best==0]
    data_model_inv = new_data.copy()[new_data.is_best==1]
    data_submodel = new_data.copy()[new_data.is_best==2]
    data_submodel_inv = new_data.copy()[new_data.is_best==3]
    data_model_size = new_data.copy()[new_data.is_best==4]
    data_model_size_inv = new_data.copy()[new_data.is_best==5]

    moto_database_model = moto_database.copy()
    moto_database_model.drop_duplicates(subset=['brand_db','model_db'],inplace=True)
    moto_database_model_inv = moto_database.copy()
    moto_database_model_inv.drop_duplicates(subset=['brand_db','model_inv_db'],inplace=True)
    moto_database_submodel = moto_database.copy()
    moto_database_submodel.drop_duplicates(subset=['brand_db','model_submodel_db'],inplace=True)
    moto_database_submodel_inv = moto_database.copy()
    moto_database_submodel_inv.drop_duplicates(subset=['brand_db','model_submodel_inv_db'],inplace=True)
    moto_database_model_size = moto_database.copy()
    moto_database_model_size.drop_duplicates(subset=['brand_db','model_size_db'],inplace=True)
    moto_database_model_size_inv = moto_database.copy()
    moto_database_model_size_inv.drop_duplicates(subset=['brand_db','model_size_inv_db'],inplace=True)

    data_model = data_model.merge(
        moto_database_model,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_model'],
        right_on=['brand_db', 'model_db'])

    data_model_inv = data_model_inv.merge(
        moto_database_model_inv,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_model_inv'],
        right_on=['brand_db', 'model_inv_db'])

    data_submodel = data_submodel.merge(
        moto_database_submodel,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_submodel'],
        right_on=['brand_db', 'model_submodel_db'])

    data_submodel_inv = data_submodel_inv.merge(
        moto_database_submodel_inv,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_submodel_inv'],
        right_on=['brand_db', 'model_submodel_inv_db'])

    data_model_size = data_model_size.merge(
        moto_database_model_size,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_model_size'],
        right_on=['brand_db', 'model_size_db'])

    data_model_size_inv = data_model_size_inv.merge(
        moto_database_model_size_inv,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_model_size_inv'],
        right_on=['brand_db', 'model_size_inv_db'])

    new_data = data_model.append(data_model_inv)
    new_data = new_data.append(data_submodel)
    new_data = new_data.append(data_submodel_inv)
    new_data = new_data.append(data_model_size)
    new_data = new_data.append(data_model_size_inv)
    # new_data.reset_index(drop=True)

    new_data.drop(
    columns=[
        'fuzzy_brand', 'fuzzy_score',
        'fuzzy_model', 'fuzzy_model_score', 'fuzzy_model_inv','fuzzy_model_inv_score',
        'fuzzy_model_size', 'fuzzy_model_size_score', 'fuzzy_model_size_inv','fuzzy_model_size_inv_score',
        'fuzzy_submodel', 'fuzzy_submodel_score', 'fuzzy_submodel_inv', 'fuzzy_submodel_inv_score',
        'is_best','model_submodel_inv_db', 'model_inv_db', 'model_size_db', 'model_size_inv_db'],
    inplace=True)

    return new_data


def append(new_data_matched, history_data):
    new_history = history_data.append(new_data_matched)
    new_history.to_csv(os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv", index=False)
    return new_history


def clean_data(df):

    ''' return clean dataframe '''
    df = df[~df["brand_db"].isnull()]
    df = df[~df["model_db"].isnull()]
    df = df[~df["category_db"].isnull()]
    #df.drop(['url', 'uniq_id', 'model_db', 'brand', "model", "brand_db"], axis= 1, inplace=True)
    #df['km/year'] = df.apply(lambda x: km_per_year(x['mileage'], x['bike_year']), axis=1)

    return df


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
