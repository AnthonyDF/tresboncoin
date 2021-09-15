import pandas as pd
import string
from rapidfuzz import process
import numpy as np
import os

moto_database = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"


def remove_punctuations(text):
    """
    remove punctuation in a string
    """
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text


def fuzzy_match(new_data, moto_database):
    """
    fuzzy_match brand and model for history and new data
    """

    # CLEAN BRAND AND MODEL
    new_data.dropna(subset=['model', 'brand'], inplace=True)
    # lower and remove spaces
    new_data.brand = new_data.brand.str.lower()
    new_data.model = new_data.model.str.lower()
    # remove punctuation
    new_data.brand = new_data.brand.apply(remove_punctuations)
    new_data.model = new_data.model.apply(remove_punctuations)

    # Clean Year
    new_data.bike_year = new_data.bike_year.astype(int)

    # MATCH BRAND NAME
    def match_brand(choices, to_match):
        return process.extractOne(to_match, choices, score_cutoff=80)

    new_data['fuzzy_brand_result'] = new_data.apply(
        lambda x: match_brand(
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

    new_data['fuzzy_brand'] = new_data['fuzzy_brand_result'].apply(unpack_tuple_name)
    new_data['fuzzy_brand_score'] = new_data['fuzzy_brand_result'].apply(unpack_tuple_score)

    new_data.drop(columns=['fuzzy_brand_result'], inplace=True)
    new_data.dropna(subset=['fuzzy_brand'], inplace=True)

    # MATCH MODEL
    # list of models, submodel...
    def choices(brand, year, engine_size, type_name):
        choices = moto_database[
            (moto_database.brand_db == str(brand))
            & (moto_database.year_db == int(year))
            & ((moto_database.engine_size_db <= engine_size + 75)
               & (moto_database.engine_size_db >= engine_size - 75))][type_name].unique().tolist()
        return [str(x) for x in choices]

    def match_model(choices, to_match):
        return process.extractOne(str(to_match), choices, score_cutoff=86)

    # fuzzy match model
    new_data['fuzzy_result_model'] = new_data.apply(lambda x:
                                                    match_model(
                                                        choices(x['fuzzy_brand'], x['bike_year'], x['engine_size'],
                                                                'model_db'),
                                                        x['model']),
                                                    axis=1)

    new_data['fuzzy_model'] = new_data['fuzzy_result_model'].apply(unpack_tuple_name)
    new_data['fuzzy_model_score'] = new_data['fuzzy_result_model'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_model', inplace=True)

    # fuzzy match submodel
    new_data['fuzzy_result_submodel'] = new_data.apply(lambda x:
                                                    match_model(
                                                        choices(x['fuzzy_brand'], x['bike_year'], x['engine_size'],
                                                                'submodel_db'),
                                                        x['model']),
                                                    axis=1)

    new_data['fuzzy_submodel'] = new_data['fuzzy_result_submodel'].apply(unpack_tuple_name)
    new_data['fuzzy_submodel_score'] = new_data['fuzzy_result_submodel'].apply(unpack_tuple_score)
    new_data.drop(columns='fuzzy_result_submodel', inplace=True)

    # choose the best fuzzy match

    def is_best(fuzzy_model_score,
                fuzzy_submodel_score):

        """
        function to define the best fuzzy matching score
        """
        scores = [float(fuzzy_model_score),
                  float(fuzzy_submodel_score)]

        max_score = max(scores)
        max_score_postion = scores.index(max_score)
        return max_score_postion

    def best_score(fuzzy_model_score,
                   fuzzy_submodel_score):

        '''
        function to define the best fuzzy matching score
        '''
        scores = [float(fuzzy_model_score),
                  float(fuzzy_submodel_score)]

        return max(scores)

    new_data['fuzzy_model_score'] = new_data['fuzzy_model_score'].fillna(0)
    new_data['fuzzy_submodel_score'] = new_data['fuzzy_submodel_score'].fillna(0)

    new_data['is_best'] = new_data.apply(
        lambda x: is_best(
            x['fuzzy_model_score'],
            x['fuzzy_submodel_score']),
        axis=1)

    new_data['score'] = new_data.apply(
        lambda x: best_score(
            x['fuzzy_model_score'],
            x['fuzzy_submodel_score']),
        axis=1)

    new_data.dropna(subset=['is_best'], inplace=True)

    data_model = new_data.copy()[new_data.is_best == 0]
    data_submodel = new_data.copy()[new_data.is_best == 1]

    moto_database_model = moto_database.copy()
    moto_database_model.drop_duplicates(subset=['brand_db', 'model_db', 'year_db'], inplace=True)
    moto_database_submodel = moto_database.copy()
    moto_database_submodel.drop_duplicates(subset=['brand_db', 'submodel_db', 'year_db'], inplace=True)

    data_model = data_model.merge(
        moto_database_model,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_model', 'bike_year'],
        right_on=['brand_db', 'model_db', 'year_db'])

    data_submodel = data_submodel.merge(
        moto_database_submodel,
        how='left',
        left_on=['fuzzy_brand', 'fuzzy_submodel', 'bike_year'],
        right_on=['brand_db', 'submodel_db', 'year_db'])

    new_data = data_model.append(data_submodel)

    new_data.drop(
        columns=[
            'fuzzy_brand', 'fuzzy_brand_score',
            'fuzzy_model', 'fuzzy_model_score',
            'fuzzy_submodel', 'fuzzy_submodel_score',
            'is_best'],
        inplace=True)

    new_data = new_data[
        ['url', 'uniq_id', 'brand', 'brand_db', 'model', 'model_db', 'submodel_db', 'score', 'bike_year',
         'date_scrapped', 'mileage', 'bike_type', 'price',
         'engine_size', 'year_db', 'category_db', 'engine_type_db',
         'engine_size_db', 'power_db', 'torque_db', 'compression_db',
         'bore_x_stroke_db', 'fuel_system_db', 'cooling_system_db',
         'shaft_drive_db', 'wheels_db', 'dry_weight_db',
         'power_weight_ratio_db']]

    return new_data[new_data.score > 0]


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
    if X_pred.brand[0] != None:
        X_pred.brand = X_pred.brand.apply(remove_punctuations)

    if X_pred.model[0] != None:
        X_pred.model = X_pred.model.apply(remove_punctuations)

    def create_title_is_missing(brand, model, title):
        if title == None:
            return brand + " " + model
        return title

    X_pred.title = X_pred.apply(lambda x: create_title_is_missing(x.brand, x.model, x.title), axis=1)

    # import motorcycle databse
    motorcycle_database = pd.read_csv(moto_database)

    motorcycle_database.drop(columns=['model_inv_db', 'model_submodel_inv_db',
                                      'engine_type_db', 'torque_db', 'compression_db', 'cooling_system_db',
                                      'dry_weight_db',
                                      'power/weight_ratio_db', 'model_size_db', 'model_size_inv_db'], inplace=True)

    def concat(brand, submodel):
        return str(brand) + " " + str(submodel)

    motorcycle_database['brand_submodel_db'] = motorcycle_database.apply(
        lambda x: concat(x.brand_db, x.model_submodel_db), axis=1)

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
    X_pred.drop(columns=['brand', 'model', 'title'], inplace=True)

    X_pred[['brand', 'model']] = X_pred.fuzzy_brand.apply(lambda x: pd.Series(str(x).split(" ")))

    X_pred.drop(columns=['fuzzy_brand'], inplace=True)

    X_pred = X_pred.merge(motorcycle_database, how='left', left_on=['brand', 'model', 'bike_year'],
                          right_on=['brand_db', 'model_submodel_db', 'year_db'])
    X_pred.drop(columns=['model_submodel_db', 'year_db', 'brand_submodel_db', 'fuzzy_score'], inplace=True)

    return X_pred


def fuzzy_match_model(new_data, moto_database):
    # CLEAN TITLE
    new_data = new_data.dropna(subset=['title'])
    # lower and remove spaces
    new_data.brand = new_data.brand.str.lower()
    new_data.model = new_data.model.str.lower()
    # remove punctuation
    new_data.brand = new_data.brand.apply(remove_punctuations)
    new_data.model = new_data.model.apply(remove_punctuations)

    new_data = new_data.dropna(subset=['brand'])

    # MATCH TITLE
    def choices(year, brand, type_name):
        choices = moto_database[(moto_database.brand_db == brand) & (moto_database.year_db == year)][
            type_name].unique().tolist()
        return [str(x) for x in choices]

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

    def match_title(choices, to_match):
        return process.extractOne(to_match, choices)

    new_data['fuzzy_match'] = new_data.apply(
        lambda x: match_title(choices(x['bike_year'], x['brand'], 'model_submodel_db'), x['model']), axis=1)

    new_data['fuzzy_score'] = new_data['fuzzy_match'].apply(unpack_tuple_score)
    new_data['fuzzy_brand_model'] = new_data['fuzzy_match'].apply(unpack_tuple_name)
    new_data.drop(columns=['fuzzy_match'], inplace=True)

    return new_data


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
