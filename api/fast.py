from fastapi import FastAPI
import pandas as pd
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware
from tresboncoin.data_ import get_data, clean_data
from tresboncoin.fuzzy_match import fuzzy_match_one
from tresboncoin.utils import km_per_year, get_model
import os

PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models/"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
def index():
    return "Root - Très bon coin"

@app.get("/predict_price")
def predict_price(uniq_id_, brand_, cc_, year_, mileage_, price_, model_, title_):
    model = get_model()

    # filtering inputs
    if title_ == "0" or title_ == "":
        title_ = None
    if model_ == "0" or model_ == "":
        model_ = None
    if brand_ == "0" or brand_ == "":
        brand_ = None
    if cc_ == "0" or cc_ == "":
        0
    if year_ == "0" or year_ == "":
        0
    if mileage_ == "0" or mileage_ == "":
        0
    if price_ == "0" or price_ == "":
        0

    # Fuzzy match from input parameters
    X_input = pd.DataFrame({'uniq_id': [uniq_id_],
                            'brand': [brand_],
                            'model': [model_],
                            'title': [title_],
                            'price': [float(price_)],
                            'mileage': [float(mileage_)],
                            'bike_year': [float(year_)],
                            'engine_size': [float(cc_)]})

    X_fuzzy_matched = fuzzy_match_one(X_input)

    print(X_fuzzy_matched.columns)

    if cc_ == "0":
        cc_ = X_fuzzy_matched.iloc[0]["engine_size_db"]

    X_pred = pd.DataFrame({"brand_db": [X_fuzzy_matched.iloc[0]["brand"]],
                           "bike_year": [X_fuzzy_matched.iloc[0]["bike_year"]],
                           "mileage": [X_fuzzy_matched.iloc[0]["mileage"]],
                           "engine_size": [X_fuzzy_matched.iloc[0]["engine_size_db"]],
                           "km/year": [km_per_year(X_fuzzy_matched.iloc[0]["mileage"], X_fuzzy_matched.iloc[0]["bike_year"])],
                           "category_db": [X_fuzzy_matched.iloc[0]["category_db"]]
                           })
    print(X_pred)

    y_pred = model.best_estimator_.predict(X_pred)

    y_pred_comparison_to_price = float(price_) - y_pred[0]

    if y_pred_comparison_to_price < 0:
        deal = "Good"
    else:
        deal = "Bad"

    return {"predicted_price": float(y_pred[0]),
            "deal": deal,
            "bike_year": int(X_fuzzy_matched["bike_year"].iloc[0]),
            "engine_size": int(X_fuzzy_matched["engine_size"].iloc[0]),
            "km/year": float(X_pred["km/year"].iloc[0]),
            "mileage": int(X_pred["mileage"].iloc[0]),
            "engine_size_db": int(X_fuzzy_matched["engine_size_db"].iloc[0]),
            "brand_db": str(X_fuzzy_matched["brand_db"].iloc[0]),
            "model_db": str(X_fuzzy_matched["model_db"].iloc[0])}


@app.get("/get_stats")
def get_stats():

    # get production model rmse
    model = get_model()

    # get number of lines after cleaning for model training
    data_train = get_data()
    data_train = clean_data(data_train)

    # get model params
    stats = dict()

    stats["last training"] = datetime.now().strftime("%d / %m / %Y")
    stats['model'] = str(model.estimator["model"])

    for k, v in model.best_params_.items():
        stats[k] = v

    # adding rmse and number of lines infos
    stats['model rmse'] = -round(model.best_score_, 0)
    stats['Number of lines after cleaning'] = data_train.shape[0]

    return stats
