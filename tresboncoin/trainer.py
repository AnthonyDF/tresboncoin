# imports
import argparse
import subprocess
from termcolor import colored
from tresboncoin.data import get_data
from tresboncoin.data import clean_data
from tresboncoin.parameters import *

# pipelines
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

# sklearn
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import StandardScaler

# mlflow
from memoized_property import memoized_property
from mlflow.tracking import MlflowClient
import mlflow

# joblib
import joblib


# Update to change parameters to test
params = params_ETR


class Trainer():

    def __init__(self, X, y, params=params):
        """
            X: pandas DataFrame
            y: pandas Series
        """
        self.pipeline = None
        self.model = None
        self.X_test = None
        self.params = params
        self.X = X
        self.y = y
        self.baseline_scores = None
        self.optimized_scores = None
        self.experiment_name = EXPERIMENT_NAME


    def set_pipeline(self):
        """ setting pipelines """

        # pipeline for numeric features
        pipe_numeric = Pipeline([
            ('imputer', SimpleImputer())
        ])

        # pipeline for multiclass features
        pipe_multiclass = Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('encoder', OneHotEncoder(sparse=False))
        ])

        # pipeline for binary features
        pipe_binary = Pipeline([
            ('encoder', OneHotEncoder(sparse=False, drop='if_binary'))
        ])

        # scaling pipeline
        scaler = Pipeline([
            ('scaler', StandardScaler())
        ])

        # combining encoder pipelines
        encoder = ColumnTransformer([
            ('binary', pipe_binary, ["Sex"]),
            ('numeric', pipe_numeric, ["Age", "Fare"]),
            ('textual', pipe_multiclass, ["Pclass"])
        ])

        # full preprocessor pipeline
        preprocessor = Pipeline([("encoder", encoder),
                                 ("scaler", scaler)])
        # Setting full pipeline
        self.pipeline = Pipeline([
                                  ("preprocessor", preprocessor),
                                  ('model', self.params["model"])
                                 ])


    def cross_validate_baseline(self, cv=20):
        """ compute model baseline accuracy """

        baseline = cross_validate(self.pipeline,
                                  self.X,
                                  self.y,
                                  scoring="accuracy",
                                  cv=cv)
        self.baseline_accuracy = round(baseline["test_score"].mean(), 3)
        print("Baseline " + type(self.params["model"]).__name__ + " model accuracy: " +
              str(self.baseline_accuracy*100) + "%")

        # ### MLFLOW RECORDS
        self.mlflow_log_metric("Baseline accuracy", self.baseline_accuracy)
        self.mlflow_log_param("Model", type(self.params["model"]).__name__)



    def run(self):
        """ looking for best parameters for the model and training """

        self.model = RandomizedSearchCV(self.pipeline,
                                        self.params["random_grid_search"],
                                        scoring='accuracy',
                                        n_iter=500,
                                        cv=5,
                                        n_jobs=-1)
        self.model.fit(self.X, self.y)
        self.optimized_accuracy = round(self.model.best_score_, 3)
        print("Tuned " + type(self.params["model"]).__name__ + " model best accuracy: " +
              str(round(self.optimized_accuracy*100, 3)) + "%")

        # ### PRINT BEST PARAMETERS
        print("\n####################################\nBest parameters:")
        for k, v in self.model.best_params_.items():
            print(k, colored(v, "green"))
        print("####################################\n")

        # ### MLFLOW RECORDS
        self.mlflow_log_metric("Optimized accuracy", self.optimized_accuracy)
        for k, v in self.model.best_params_.items():
            self.mlflow_log_param(k, v)



    @memoized_property
    def mlflow_client(self):
        mlflow.set_tracking_uri(CUSTOMURI)
        return MlflowClient()

    @memoized_property
    def mlflow_experiment_id(self):
        try:
            return self.mlflow_client.create_experiment(self.experiment_name)
        except BaseException:
            return self.mlflow_client.get_experiment_by_name(self.experiment_name).experiment_id

    @memoized_property
    def mlflow_run(self):
        return self.mlflow_client.create_run(self.mlflow_experiment_id)

    def mlflow_log_param(self, key, value):
        self.mlflow_client.log_param(self.mlflow_run.info.run_id, key, value)

    def mlflow_log_metric(self, key, value):
        self.mlflow_client.log_metric(self.mlflow_run.info.run_id, key, value)



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

    # get data
    data_train, data_test = get_data()
#
    #  # clean data
    #  data_train = clean_data(data_train)
#
    #  # set X and y
    #  X = data_train.drop(["Survived"], axis=1)
    #  y = data_train["Survived"]
#
    #  # define trainer
    #  trainer = Trainer(X, y)
    #  trainer.set_pipeline()
#
    #  # get best accuracy
    #  trainer.cross_validate_baseline()
    #  trainer.run()
#
    #  # saving trained model and moving it to models folder
    #  trainer.save_model(model_name=results.modelname)
    #  subprocess.run(["mv", results.modelname + ".joblib", "models"])
