import pandas as pd
import string
from rapidfuzz import process
import numpy as np
import os

PATH_TO_BIKES = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"

def fuzzy_match(X_pred):
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

    X_pred.title = X_pred.apply(lambda x: create_title_is_missing(x.brand, x.model, x.title), axis=1)

    # import motorcycle databse
    motorcycle_database = pd.read_csv(PATH_TO_BIKES)

    motorcycle_database.drop(
        columns=[ 'model_inv_db', 'model_submodel_inv_db','engine_type_db', 'torque_db',
        'compression_db', 'cooling_system_db', 'dry_weight_db',
        'power/weight_ratio_db', 'model_size_db', 'model_size_inv_db'], inplace=True)

    def concat(brand, submodel):
        return str(brand) + " " + str(submodel)

    motorcycle_database['brand_submodel_db'] = motorcycle_database.apply(lambda x: concat(x.brand_db, x.model_submodel_db), axis=1)

    def choices(year):
      '''
      function to return a list of models according to bike year
      '''
      choices = motorcycle_database[motorcycle_database.year_db == year].brand_submodel_db.unique().tolist()
      return [str(x) for x in choices]

    def match_model(choices, to_match):
      """
      function to match model
      """
      return process.extractOne(to_match, choices)

    X_pred["fuzzy_result"] = X_pred.apply(lambda x: match_model(choices(x.bike_year), x.title), axis=1)

    def unpack_tuple_name(result):
      '''
      function to unpack results from fuzzy matching
      '''
      try:
          return result[0]
      except:
          return np.nan

    def unpack_tuple_score(result):
      '''
      function to unpack results from fuzzy matching
      '''
      try:
          return result[1]
      except:
          return np.nan

    X_pred['fuzzy_score'] = X_pred['fuzzy_result'].apply(unpack_tuple_score)
    X_pred['fuzzy_brand'] = X_pred['fuzzy_result'].apply(unpack_tuple_name)
    X_pred.drop(columns=['fuzzy_result'], inplace=True)
    X_pred.drop(columns=['brand','model','title'], inplace=True)

    X_pred[['brand','model']] = X_pred.fuzzy_brand.apply(lambda x: pd.Series(str(x).split(" ")))

    X_pred.drop(columns=['fuzzy_brand'], inplace=True)

    X_pred = X_pred.merge(motorcycle_database,how='left', left_on=['brand', 'model', 'bike_year'], right_on=['brand_db', 'model_submodel_db', 'year_db'])
    X_pred.drop(columns=['brand_db','model_db','model_submodel_db','year_db', 'brand_submodel_db', 'fuzzy_score'], inplace=True)

    return X_pred


if __name__ == '__main__':
    X_pred = pd.DataFrame(
        {'uniq_id': ['ERT34983'],
         'brand': ['bMw.'],
         'model': ['f-800gS'],
         'title': [None],
         'price': [4500],
         'mileage': [5002],
         'bike_year': [2010],
         'engine_size': [None]})

    print(fuzzy_match(X_pred))
