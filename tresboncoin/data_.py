from tresboncoin.utils import km_per_year
from tresboncoin.utils import set_brand_and_model
from tresboncoin.utils import set_colums
from tresboncoin.parameters import concatenation_map
from tresboncoin.parameters import columns_to_keep
from datetime import datetime
from tresboncoin.fuzzy_match import fuzzy_match
import pandas as pd
import numpy as np
import os
from termcolor import colored

raw_data = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv"
history_data = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv"
moto_database = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"
ebay_db = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/ebay_db.csv"
#
motoplanete_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motoplanete.csv"
motomag_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motomag.csv"
fulloccaz_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/fulloccaz.csv"
motooccasion_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/moto-occasion.csv"
motoselection_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/moto-selection.csv"
as_24_FR_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/as_24_FR.csv"
as_24_BE_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/as_24_BE.csv"
as_24_BE_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/as_24_BE.csv"
lacentrale_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/lacentrale.csv"
leboncoin_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/leboncoin.csv"
motovente_csv = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motovente.csv"


def concat_df():
    ''' concatenate scrapped datasets from:
            fulloccaz
            motoplanete
            autoscout24
            moto-selection
            moto-occasion
            motomag
            lacentrale
            leboncoin
            motovente
    '''

    # ebay brand list
    ebay_brands = pd.read_csv(ebay_db)
    ebay_brands = ebay_brands["Make"].apply(lambda x: str(x).lower())
    ebay_brands = pd.DataFrame(ebay_brands).drop_duplicates().reset_index(drop=True)

    # motoplanete brand list
    mp_brands = ["Aprilia", "Benelli", "Beta", "Bimota", "BMW", "Buell", "CF MOTO", "Daelim", "Ducati", "Fantic",
                 "FB Mondial", "Gas Gas", "Gilera", "Harley-Davidson", "Honda", "Husqvarna", "Hyosung", "Indian",
                 "Kawasaki", "KTM", "Kymco", "Magpower", "Malaguti", "Mash", "Moto-Guzzi", "MV-Agusta", "Orcal",
                 "Rieju", "Royal-Enfield", "Sherco", "Suzuki", "SWM", "Sym", "Triumph", "Voxan", "Yamaha"]

    # fulloccaz brand list
    fo_brands = ["AC EMOTION", "ACCESS MOTOR", "APRILIA", "ARCTIC CAT", "BENELLI", "BETA", "BIMOTA", "BMW",
                 "BUELL", "CAN-AM", "CF MOTO", "DAELIM", "DERBI", "DUCATI", "FANTIC", "FB MONDIAL", "GAS GAS",
                 "GENERIC", "GILERA", "HARLEY DAVIDSON", "HARLEY-DAVIDSON", "HER CHEE", "HM", "HONDA", "HONGYI",
                 "HUSQVARNA", "HYOSUNG", "HYTRACK", "IMF scooter", "INDIAN", "IRBIT", "JM MOTORS", "JORDON", "JOTAGAS",
                 "KAWASAKI", "KEEWAY", "KSR MOTO", "KTM", "KYMCO", "LAMBRETTA", "LAZIO", "LIGIER", "LINHAI", "LONGJIA",
                 "Magnum", "MAGPOWER", "MALAGUTI", "MARTIN", "MASAI", "MASH", "MBK", "MOTO MORINI", "MOTO-GUZZI",
                 "MOTOCONFORT", "MOTRAC", "MV AGUSTA", "NECO", "NIU", "NORTON", "ORCAL", "PEUGEOT", "PIAGGIO", "POLARIS",
                 "QUADDY", "QUADRO", "RIEJU", "RIVAL MOTORS", "RIYA", "ROYAL ENFIELD", "SHERCO", "SUPER SOCO", "SUZUKI",
                 "SVM (SWM)", "SWM", "SYM", "TGB", "TNT MOTOR", "TRIUMPH", "VASTRO", "VESPA", "Victory Motorcycle",
                 "VOGE", "VOXAN", "WANGYE", "XINGYUE", "YAMAHA", "ZERO MOTORCYCLES"]

    # full brand list
    full_brand_list = [k.lower() for k in fo_brands] + \
                      [k.lower() for k in mp_brands] + \
                    list(ebay_brands.Make)

    # loading datasets
    data_motoplanete = pd.read_csv(motoplanete_csv)
    data_fulloccaz = pd.read_csv(fulloccaz_csv)
    data_motooccasion = pd.read_csv(motooccasion_csv)
    data_motoselection = pd.read_csv(motoselection_csv)
    data_as_24_FR = pd.read_csv(as_24_FR_csv)
    data_as_24_BE = pd.read_csv(as_24_BE_csv)
    data_motomag = pd.read_csv(motomag_csv)
    data_lacentrale = pd.read_csv(lacentrale_csv)
    data_leboncoin = pd.read_csv(leboncoin_csv)
    data_motovente = pd.read_csv(motovente_csv)

    # Cleaning datasets
    # MOTOPLANETE
    data_motoplanete["vehicle release date"] = pd.to_datetime(data_motoplanete["vehicle release date"])
    data_motoplanete["vehicle release date"] = data_motoplanete["vehicle release date"].apply(lambda x: int(x.strftime("%Y")))
    data_motoplanete["vehicle brand"] = data_motoplanete["vehicle brand"].apply(lambda x: str(x).lower())
    data_motoplanete = set_brand_and_model(data_motoplanete, "vehicle brand", r=full_brand_list)
    data_motoplanete["uniq_id"] = data_motoplanete["unique id"].apply(lambda x: "motoplanete-" + str(x))

    # FULLOCCAZ
    data_fulloccaz = data_fulloccaz[~data_fulloccaz["vehicle release date"].isnull()]
    data_fulloccaz["vehicle release date"] = pd.to_datetime(data_fulloccaz["vehicle release date"])
    data_fulloccaz["vehicle release date"] = data_fulloccaz["vehicle release date"].apply(lambda x: int(x.strftime("%Y")))
    data_fulloccaz["vehicle brand"] = data_fulloccaz["vehicle brand"].apply(lambda x: str(x).lower())
    data_fulloccaz = set_brand_and_model(data_fulloccaz, "vehicle brand", r=full_brand_list)
    data_fulloccaz["uniq_id"] = data_fulloccaz["unique id"].apply(lambda x: "fulloccaz-" + str(x))

    # AUTOSCOUT24
    data_as_24_FR["model"] = data_as_24_FR["model"].apply(lambda x: str(x).lower())
    data_as_24_FR = set_brand_and_model(data_as_24_FR, "model", r=full_brand_list)
    data_as_24_FR["uniq_id"] = data_as_24_FR["reference"].apply(lambda x: "autoscout24-" + str(x))
    data_as_24_FR["cylindree"] = data_as_24_FR["cylindree"].apply(lambda x: float(str(x).replace(" cm³", "").replace(".", "")))
    data_as_24_FR["date_scrapped"] = datetime.now()
    data_as_24_FR.rename(columns={"model": "old_model"}, inplace=True)

    # AUTOSCOUT24 BE
    data_as_24_BE["model"] = data_as_24_BE["model"].apply(lambda x: str(x).lower())
    data_as_24_BE = set_brand_and_model(data_as_24_BE, "model", r=full_brand_list)
    data_as_24_BE["uniq_id"] = data_as_24_BE["reference"].apply(lambda x: "autoscout24-BE-" + str(x))
    data_as_24_BE["cylindree"] = data_as_24_BE["cylindree"].apply(lambda x: float(str(x).replace(" cm³", "").replace(".", "")))
    data_as_24_BE["date_scrapped"] = datetime.now()
    data_as_24_BE.rename(columns={"model": "old_model"}, inplace=True)

    # LACENTRALE
    data_lacentrale["uniq_id"] = data_lacentrale["url"].apply(lambda x: "lacentrale-" + x.split("-")[-1].split(".")[0])
    data_lacentrale["bike_type"] = [np.nan] * data_lacentrale["url"].shape[0]
    data_lacentrale["date_scrapped"] = datetime.now()

    # LEBONCOIN
    data_leboncoin["uniq_id"] = data_leboncoin["url"].apply(lambda x: "leboncoin-" + x.split("/")[-1].split(".")[0])
    data_leboncoin["bike_type"] = [np.nan] * data_leboncoin["url"].shape[0]
    data_leboncoin["date_scrapped"] = datetime.now()

    # Dataset concatenation
    data_motoplanete.columns = set_colums(data_motoplanete, concatenation_map, "motoplanete")
    data_fulloccaz.columns = set_colums(data_fulloccaz, concatenation_map, "fulloccaz")
    data_motooccasion.columns = set_colums(data_motooccasion, concatenation_map, "moto-occasion")
    data_motoselection.columns = set_colums(data_motoselection, concatenation_map, "moto-selection")
    data_as_24_FR.columns = set_colums(data_as_24_FR, concatenation_map, "autoscout24")
    data_as_24_BE.columns = set_colums(data_as_24_BE, concatenation_map, "autoscout24_de")
    data_motomag.columns = set_colums(data_motomag, concatenation_map, "motomag")
    data_lacentrale.columns = set_colums(data_lacentrale, concatenation_map, "lacentrale")
    data_leboncoin.columns = set_colums(data_leboncoin, concatenation_map, "leboncoin")
    data_motovente.columns = set_colums(data_motovente, concatenation_map, "motovente")

    # Concatenation
    data = pd.concat([data_motoplanete[columns_to_keep],
                      data_fulloccaz[columns_to_keep],
                      data_motooccasion[columns_to_keep],
                      data_motoselection[columns_to_keep],
                      data_as_24_FR[columns_to_keep],
                      data_motomag[columns_to_keep],
                      # data_as_24_BE[columns_to_keep]
                      data_lacentrale[columns_to_keep],
                      data_leboncoin[columns_to_keep],
                      data_motovente[columns_to_keep]
                      ], axis=0, ignore_index=True)

    data.to_csv(raw_data, index=False)

    print(colored("Concat dataset saved as master/master_data.csv. Shape: " + str(data.shape), "green"))


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
    df = df[df["bike_year"] != "['']"]
    df["bike_year"] = df["bike_year"].apply(lambda x: int(float(x)))
    df = df[(df["bike_year"] >= 1900) & (df["bike_year"] <= datetime.now().year)]
    df = df[(df["mileage"] >= 100) & (df["mileage"] <= 150000)]
    df = df[(df["price"] >= 100) & (df["price"] < 40000)]
    df = df[(df["engine_size"] >= 49) & (df["engine_size"] < 2100)]

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
    # df = df[~df["engine_size"].isnull()]
    df.drop_duplicates(subset=['model_db', 'brand_db', 'price', 'engine_size_db', 'mileage', 'bike_year'], inplace=True)

    # remove categories with low count of bikes
    category_count_threshold = 200
    groupby_category = df.groupby('category_db').agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    drop_category = groupby_category[groupby_category .Count < category_count_threshold].index.to_list()
    drop_category.append('unspecified category')

    # remove brands with low count of bikes
    brand_count_threshold = 200
    groupby_brand = df.groupby('brand_db').agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    drop_brand = groupby_brand[groupby_brand.Count < brand_count_threshold].index.to_list()
    df = df[df.brand_db.isin(drop_brand) == False]

    # remove models with low count of bikes
    # model_count_threshold = 1
    # groupby_model = df.groupby(['model_db']).agg(Mean=('price', 'mean'), Std=('price', 'std'), Count=('price', 'count'))
    # drop_model = groupby_model[groupby_model.Count < model_count_threshold].index.to_list()
    # df = df[df.model_db.isin(drop_model) == False]

    # feature engineering
    df['km/year'] = df.apply(lambda x: km_per_year(x['mileage'], x['bike_year']), axis=1)
    # df['age'] = df.apply(lambda x: datetime.now().year - x.bike_year, axis=1)

    return df[['brand_db', 'bike_year', 'mileage', 'engine_size', 'km/year', "price"]]


if __name__ == '__main__':

    # concatenate scraping outputs
    concat_df()

    # fuzzy match
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
