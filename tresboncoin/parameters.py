# sklearn
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.neighbors import KNeighborsRegressor


# others
from scipy import stats
import numpy as np

# MLFLOW PARAMETERS
MLFLOW_URI = "https://mlflow.lewagon.co/"
CUSTOMURI = ""
myname = "VictorBnnt"
EXPERIMENT_NAME = f"[FR] [Paris] [{myname}] TresBonCoin"

######################################################
# Random Forest Classifier model
######################################################
grid_ETR = {'model__max_depth': stats.randint(1, 1000),
            'model__min_samples_split': [2, 4, 6, 8, 10],
            "preprocessor__scaler": [StandardScaler(), RobustScaler(), MinMaxScaler()]
            }
params_ETR = {"random_grid_search": grid_ETR,
              "model": ExtraTreesRegressor()}
######################################################

######################################################
# KNeighborsRegressor Classifier model
######################################################
grid_KNR = {'model__weights': ['distance', 'uniform'],
            'model__algorithm': ['ball_tree', 'kd_tree'],
            'model__leaf_size': np.linspace(25, 40, 15),
            'model__p': [1, 2],
            "preprocessor__scaler": [StandardScaler(), RobustScaler(), MinMaxScaler()]
            }
params_KNR = {"random_grid_search": grid_KNR,
              "model": KNeighborsRegressor()}
######################################################
