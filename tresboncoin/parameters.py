# sklearn
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import ExtraTreesRegressor

# others
from scipy import stats

# MLFLOW PARAMETERS
MLFLOW_URI = "https://mlflow.lewagon.co/"
CUSTOMURI = ""
myname = "VictorBnnt"
EXPERIMENT_NAME = f"[FR] [Paris] [{myname}] TresBonCoin"

######################################################
# Random Forest Classifier model
######################################################
grid_ETR = {'model__n_estimators': stats.randint(1, 200),
            'model__max_depth': stats.randint(1, 40),
            'model__min_samples_split': [2, 4, 6, 8, 10],
            'model__criterion': ["gini", "entropy"],
            "preprocessor__encoder__numeric__imputer__strategy": ["mean", "median"],
            "preprocessor__scaler__scaler": [StandardScaler(), RobustScaler(), MinMaxScaler()]
            # 'degree': stats.randint(2, 3)
            }
params_ETR = {"random_grid_search": grid_ETR,
              "model": ExtraTreesRegressor()}
######################################################
