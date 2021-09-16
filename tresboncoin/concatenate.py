from tresboncoin.utils import km_per_year
from tresboncoin.utils import set_brand_and_model
from tresboncoin.utils import set_colums
from tresboncoin.parameters import concatenation_map
from tresboncoin.parameters import columns_to_keep

from datetime import datetime
import pandas as pd
import numpy as np
import os
from termcolor import colored

from google.cloud import storage

# Raw data file path
raw_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv"
raw_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_data.csv"

# History data file path
history_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv"
history_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_with_fuzzy_and_cleaning.csv"

# reference database file path
moto_database = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"
ebay_db = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/ebay_db.csv"

# scraped csv files path
as_24_FR_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/as_24_FR.csv"
as_24_BE_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/as_24_BE.csv"
lacentrale_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/lacentrale.csv"
leboncoin_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/leboncoin.csv"
motoplanete_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motoplanete.csv"

# automated scraped csv file path
motomag_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motomag.csv"
fulloccaz_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/fulloccaz.csv"
motooccasion_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/moto-occasion.csv"
motoselection_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/moto-selection.csv"
motovente_csv_local = os.path.dirname(os.path.abspath(__file__)) + "/data/scraping_outputs/motovente.csv"

motomag_csv_gs = "gs://tresboncoin/tresboncoin/data/scraping_outputs/motomag.csv"
fulloccaz_csv_gs = "gs://tresboncoin/tresboncoin/data/scraping_outputs/fulloccaz.csv"
motooccasion_csv_gs = "gs://tresboncoin/tresboncoin/data/scraping_outputs/moto-occasion.csv"
motoselection_csv_gs = "gs://tresboncoin/tresboncoin/data/scraping_outputs/moto-selection.csv"
motovente_csv_gs = "gs://tresboncoin/tresboncoin/data/scraping_outputs/motovente.csv"


def concat_df(write_method='local', read_method='local'):
    """
    Concatenate scrapped datasets from:
        fulloccaz
        motoplanete
        autoscout24
        moto-selection
        moto-occasion
        motomag
        lacentrale
        leboncoin
        motovente

    Write:
        local: only local save
        both: local and google storage backup
    Read:
        local: read csv file from local
        gs: read csv file from google storage

    """
    # automated scraped csv file path
    if read_method == 'local':
        motomag_csv = motomag_csv_local
        fulloccaz_csv = fulloccaz_csv_local
        motooccasion_csv = motooccasion_csv_local
        motoselection_csv = motoselection_csv_local
        motovente_csv = motovente_csv_local

    elif read_method == 'gs':
        motomag_csv = motomag_csv_gs
        fulloccaz_csv = fulloccaz_csv_gs
        motooccasion_csv = motooccasion_csv_gs
        motoselection_csv = motoselection_csv_gs
        motovente_csv = motovente_csv_gs

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
                 "MOTOCONFORT", "MOTRAC", "MV AGUSTA", "NECO", "NIU", "NORTON", "ORCAL", "PEUGEOT", "PIAGGIO",
                 "POLARIS",
                 "QUADDY", "QUADRO", "RIEJU", "RIVAL MOTORS", "RIYA", "ROYAL ENFIELD", "SHERCO", "SUPER SOCO", "SUZUKI",
                 "SVM (SWM)", "SWM", "SYM", "TGB", "TNT MOTOR", "TRIUMPH", "VASTRO", "VESPA", "Victory Motorcycle",
                 "VOGE", "VOXAN", "WANGYE", "XINGYUE", "YAMAHA", "ZERO MOTORCYCLES"]

    # full brand list
    full_brand_list = [k.lower() for k in fo_brands] + \
                      [k.lower() for k in mp_brands] + \
                      list(ebay_brands.Make)

    # loading datasets
    data_motoplanete = pd.read_csv(motoplanete_csv_local)
    print('motoplanete', data_motoplanete.shape)
    data_fulloccaz = pd.read_csv(fulloccaz_csv)
    print('fulloccaz', data_fulloccaz.shape)
    data_motooccasion = pd.read_csv(motooccasion_csv)
    print('motooccasion', data_motooccasion.shape)
    data_motoselection = pd.read_csv(motoselection_csv)
    print('motoselection', data_motoselection.shape)
    data_as_24_FR = pd.read_csv(as_24_FR_csv_local)
    print('as_24_FR', data_as_24_FR.shape)
    data_as_24_BE = pd.read_csv(as_24_BE_csv_local)
    print('as_24_BE', data_as_24_BE.shape)
    data_motomag = pd.read_csv(motomag_csv)
    print('motomag', data_motomag.shape)
    data_lacentrale = pd.read_csv(lacentrale_csv_local)
    print('lacentrale', data_lacentrale.shape)
    data_leboncoin = pd.read_csv(leboncoin_csv_local)
    print('leboncoin', data_leboncoin.shape)
    data_motovente = pd.read_csv(motovente_csv)
    print('motovente', data_motovente.shape)

    print('Sum of all rows',
          (data_motoplanete.shape[0] +
           data_fulloccaz.shape[0] +
           data_motooccasion.shape[0] +
           data_motoselection.shape[0] +
           data_as_24_FR.shape[0] +
           data_as_24_BE.shape[0] +
           data_motomag.shape[0] +
           data_lacentrale.shape[0] +
           data_leboncoin.shape[0] +
           data_motovente.shape[0]))

    # Cleaning datasets
    # MOTOPLANETE
    data_motoplanete["vehicle release date"] = pd.to_datetime(data_motoplanete["vehicle release date"])
    data_motoplanete["vehicle release date"] = data_motoplanete["vehicle release date"].apply(
        lambda x: int(x.strftime("%Y")))
    data_motoplanete["vehicle brand"] = data_motoplanete["vehicle brand"].apply(lambda x: str(x).lower())
    data_motoplanete = set_brand_and_model(data_motoplanete, "vehicle brand", r=full_brand_list)
    data_motoplanete["uniq_id"] = data_motoplanete["unique id"].apply(lambda x: "motoplanete-" + str(x))

    # FULLOCCAZ
    data_fulloccaz["vehicle release date"] = pd.to_datetime(data_fulloccaz["vehicle release date"], errors='coerce')
    data_fulloccaz = data_fulloccaz[~data_fulloccaz["vehicle release date"].isnull()]
    data_fulloccaz["vehicle release date"] = data_fulloccaz["vehicle release date"].apply(
        lambda x: int(x.strftime("%Y")))
    data_fulloccaz["vehicle brand"] = data_fulloccaz["vehicle brand"].apply(lambda x: str(x).lower())
    data_fulloccaz = set_brand_and_model(data_fulloccaz, "vehicle brand", r=full_brand_list)
    data_fulloccaz["uniq_id"] = data_fulloccaz["unique id"].apply(lambda x: "fulloccaz-" + str(x))

    # AUTOSCOUT24
    data_as_24_FR["model"] = data_as_24_FR["model"].apply(lambda x: str(x).lower())
    data_as_24_FR = set_brand_and_model(data_as_24_FR, "model", r=full_brand_list)
    data_as_24_FR["uniq_id"] = data_as_24_FR["reference"].apply(lambda x: "autoscout24-" + str(x))
    data_as_24_FR["cylindree"] = data_as_24_FR["cylindree"].apply(
        lambda x: float(str(x).replace(" cm³", "").replace(".", "")))
    data_as_24_FR["date_scrapped"] = datetime.now()
    data_as_24_FR.rename(columns={"model": "old_model"}, inplace=True)

    # AUTOSCOUT24 BE
    data_as_24_BE["model"] = data_as_24_BE["model"].apply(lambda x: str(x).lower())
    data_as_24_BE = set_brand_and_model(data_as_24_BE, "model", r=full_brand_list)
    data_as_24_BE["uniq_id"] = data_as_24_BE["reference"].apply(lambda x: "autoscout24-BE-" + str(x))
    data_as_24_BE["cylindree"] = data_as_24_BE["cylindree"].apply(
        lambda x: float(str(x).replace(" cm³", "").replace(".", "")))
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

    if write_method == 'local':
        # Saving to local directory
        data.to_csv(raw_data_local, index=False)
        print(colored("Concat dataset saved as master/master_data.csv. Shape: " + str(data.shape), "green"))

    elif write_method == 'both':
        # Saving to local directory
        data.to_csv(raw_data_local, index=False)
        print(colored("Concat dataset saved as master/master_data.csv. Shape: " + str(data.shape), "green"))
        # Saving to Google Cloud Storage as a backup
        client = storage.Client()
        bucket = client.bucket("tresboncoin")
        blob = bucket.blob("tresboncoin/data/master/master_data.csv")
        blob.upload_from_filename(raw_data_local)
        print(colored("master_data.csv backup is on Google Cloud Storage. Shape: " + str(data.shape), "green"))


if __name__ == '__main__':
    # concatenate scraping outputs
    concat_df(read_method='local', write_method='local')