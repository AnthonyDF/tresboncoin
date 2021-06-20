import pandas as pd
import string
from rapidfuzz import process
import numpy as np
import os

PATH_TO_BIKES = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"


def remove_punctuations(text):
    '''
    remove punctuation in a string
    '''
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text


def fuzzy_match(new_data, moto_database):

    # CLEAN BRAND AND MODEL
    new_data.dropna(subset=['model', 'brand'], inplace=True)
    # lower and remove spaces
    new_data.brand = new_data.brand.str.lower().str.replace(' ', '')
    new_data.model = new_data.model.str.lower().str.replace(' ', '')
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


def fuzzy_match_one(X_pred):
    """
    fuction to fuzzy match motorbike name
    """
    # remove punctuation
    def remove_punctuations(text):
        for punctuation in string.punctuation:
            text = text.replace(punctuation, '')
        return text

    # remove punctuation
    if X_pred.brand[0]!=None:
        X_pred.brand = X_pred.brand.apply(remove_punctuations)

    if X_pred.model[0]!=None:
        X_pred.model = X_pred.model.apply(remove_punctuations)

    def create_title_is_missing(brand, model, title):
        if title == None:
            return brand + " " + model
        return title

    X_pred.title = X_pred.apply(lambda x: create_title_is_missing(x.brand, x.model, x.title), axis=1)

    # import motorcycle databse
    motorcycle_database = pd.read_csv(PATH_TO_BIKES)

    motorcycle_database.drop(columns=['model_inv_db', 'model_submodel_inv_db',
      'engine_type_db', 'torque_db','compression_db', 'cooling_system_db', 'dry_weight_db',
      'power/weight_ratio_db', 'model_size_db', 'model_size_inv_db'], inplace=True)

    def concat(brand, submodel):
        return str(brand) + " " + str(submodel)

    motorcycle_database['brand_submodel_db'] = motorcycle_database.apply(lambda x: concat(x.brand_db, x.model_submodel_db), axis=1)

    def choices(year):
        choices = motorcycle_database[motorcycle_database.year_db == year].brand_submodel_db.unique().tolist()
        return [str(x) for x in choices]

    def match_model(choices, to_match):
        return process.extractOne(to_match, choices)

    X_pred["fuzzy_result"] = X_pred.apply(lambda x: match_model(choices(x.bike_year), x.title), axis=1)

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

    X_pred['fuzzy_score'] = X_pred['fuzzy_result'].apply(unpack_tuple_score)
    X_pred['fuzzy_brand'] = X_pred['fuzzy_result'].apply(unpack_tuple_name)
    X_pred.drop(columns=['fuzzy_result'], inplace=True)
    X_pred.drop(columns=['brand','model','title'], inplace=True)

    X_pred[['brand', 'model']] = X_pred.fuzzy_brand.apply(lambda x: pd.Series(str(x).split(" ")))

    X_pred.drop(columns=['fuzzy_brand'], inplace=True)

    X_pred = X_pred.merge(motorcycle_database, how='left', left_on=['brand', 'model', 'bike_year'], right_on=['brand_db', 'model_submodel_db', 'year_db'])
    X_pred.drop(columns=['model_submodel_db', 'year_db', 'brand_submodel_db', 'fuzzy_score'], inplace=True)

    return X_pred


if __name__ == '__main__':
    X_pred = pd.DataFrame(
        {'uniq_id': ['ERT34983'],
         'brand': [None],
         'model': [None],
         'title': ['Doucati Monster'],
         'price': [4500],
         'mileage': [5002],
         'bike_year': [2010],
         'engine_size': [None]})

    print(fuzzy_match_one(X_pred))
