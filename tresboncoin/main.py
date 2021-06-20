from scraper import scraper
from data_ import concat_df, get_data, get_new_data, get_raw_data
from data_ import clean_raw_data, clean_data, append, get_motorcycle_db
from fuzzy_match import fuzzy_match
import argparse
from trainer import Trainer
import subprocess
import os


def main():
    # SCRAP DATA
    print('Scraping in progress')
    scraper()

    #  BUILD DATAFRAME
    print('Concat. in progress')
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

    '''
    # terminal parameter definition
    parser = argparse.ArgumentParser(description='TresBonCoin trainer')
    parser.add_argument('-m', action="store",
                        dest="modelname",
                        help='.joblib model name - default: model',
                        default="model")

    # getting optionnal arguments otherwise default
    results = parser.parse_args()

    # get data
    data_train = get_data()

    # clean data
    data_train = clean_data(data_train)

    # set X and y
    X = data_train.drop(["price"], axis=1)
    y = data_train["price"]

    # define trainer
    trainer = Trainer(X, y)
    trainer.set_pipeline()

    # get baseline scores
    # trainer.cross_validate_baseline()

    trainer.run()
#
    # saving trained model and moving it to models folder
    PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models/"
    trainer.save_model(model_name=results.modelname)
    subprocess.run(["mv", results.modelname + ".joblib", PATH_TO_LOCAL_MODEL])
    '''


if __name__ == '__main__':
    main()
