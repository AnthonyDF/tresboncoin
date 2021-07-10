# imports
import argparse
import subprocess
from termcolor import colored
from tresboncoin.data_ import get_data, concat_df, clean_raw_data
from tresboncoin.data_ import get_motorcycle_db, append
from tresboncoin.data_ import clean_data, get_new_data, get_raw_data
from tresboncoin.fuzzy_match import fuzzy_match
from tresboncoin.utils import custom_rmse
from sklearn.metrics import make_scorer
from tresboncoin.parameters import *
import os

# pipelines
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# sklearn
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import RobustScaler


# joblib
import joblib


# Update to change parameters to test
params = params_ETR
PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models/"


class Trainer():

    def __init__(self, X, y, params=params):
        """
            X: pandas DataFrame
            y: pandas Series
        """
        self.pipeline = None
        self.scorer = make_scorer(custom_rmse, greater_is_better=False)
        self.model = None
        self.params = params
        self.X = X
        self.y = y
        self.baseline_r2 = None
        self.baseline_rmse = None
        self.optimized_r2 = None
        self.optimized_rmse = None
        self.experiment_name = EXPERIMENT_NAME


    def set_pipeline(self):
        """ setting pipelines """

        # pipeline for multiclass features
        pipe_multiclass = Pipeline([
            ('encoder', OneHotEncoder(sparse=False, handle_unknown="ignore"))
        ])

        # applying encoder
        encoder = ColumnTransformer([
            ('textual', pipe_multiclass, ["brand_db", "category_db"])
        ], remainder='passthrough')

        # full preprocessor pipeline
        preprocessor = Pipeline([("encoder", encoder),
                                 ('scaler', RobustScaler())])
        # Setting full pipeline
        self.pipeline = Pipeline([
                                  ("preprocessor", preprocessor),
                                  ('model', self.params["model"])
                                 ])


    def cross_validate_baseline(self, cv=3):
        """ compute model baseline rmse and r2 scores """

        baseline = cross_validate(self.pipeline,
                                  self.X,
                                  self.y,
                                  scoring={"rmse": self.scorer, "r2": "r2"},
                                  cv=cv)
        self.baseline_r2 = baseline['test_r2'].mean()
        self.baseline_rmse = -baseline['test_rmse'].mean()
        print("Baseline " + type(self.params["model"]).__name__ + " model r2 score: " +
              str(self.baseline_r2))
        print("Baseline " + type(self.params["model"]).__name__ + " model rmse score: " +
              str(self.baseline_rmse))


    def run(self):
        """ looking for best parameters for the model and training """

        self.model = RandomizedSearchCV(self.pipeline,
                                        self.params["random_grid_search"],
                                        scoring=self.scorer,
                                        n_iter=1,
                                        cv=3,
                                        n_jobs=-1,
                                        verbose=2)
        self.model.fit(self.X, self.y)
        self.optimized_rmse = -self.model.best_score_
        print("Tuned " + type(self.params["model"]).__name__ + " model best rmse: " +
              str(round(self.optimized_rmse, 1)))
#
        # ### PRINT BEST PARAMETERS
        print("\n####################################\nBest parameters:")
        for k, v in self.model.best_params_.items():
            print(k, colored(v, "green"))
        print("####################################\n")


    def save_model(self, model_name):
        """ Save the model into a .joblib format """
        joblib.dump(self.model, model_name + ".joblib")
        print(colored("Trained model saved locally under " + model_name + ".joblib", "green"))


# terminal parameter definition
parser = argparse.ArgumentParser(description='TresBonCoin trainer')
parser.add_argument('-m', action="store",
                    dest="modelname",
                    help='.joblib model name - default: model',
                    default="model")

if __name__ == "__main__":
    # getting optionnal arguments otherwise default
    results = parser.parse_args()

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
    #trainer.cross_validate_baseline()

    trainer.run()

    # saving trained model and moving it to models folder
    trainer.save_model(model_name=results.modelname)
    subprocess.run(["mv", results.modelname + ".joblib", PATH_TO_LOCAL_MODEL])
