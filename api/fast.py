from fastapi import FastAPI
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from tresboncoin.utils import get_model
from tresboncoin.fuzzy_match import fuzzy_match_one
from tresboncoin.utils import km_per_year


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
    model = get_model("model")

    # filtering inputs
    if title_ == "0":
        title_ = None
    if model_ == "0":
        model_ = None
    if brand_ == "0":
        brand_ = None

    # Fuzzy match from input parameters
    X_input = pd.DataFrame({'uniq_id': [uniq_id_],
                            'brand': [brand_],
                            'model': [model_],
                            'title': [title_],
                            'price': [float(price_)],
                            'mileage': [float(mileage_)],
                            'bike_year': [float(year_)],
                            'engine_size': [float(cc_)]})

    X_fuzzy_matched = fuzzy_match(X_input)

    if cc_ == "0":
        cc_ = X_fuzzy_matched.iloc[0]["engine_size_db"]

    X_pred = pd.DataFrame({"brand_db": [X_fuzzy_matched.iloc[0]["brand"]],
                           "bike_year": [X_fuzzy_matched.iloc[0]["bike_year"]],
                           "mileage": [X_fuzzy_matched.iloc[0]["mileage"]],
                           "engine_size": [X_fuzzy_matched.iloc[0]["engine_size_db"]],
                           "km/year": [km_per_year(X_fuzzy_matched.iloc[0]["mileage"], X_fuzzy_matched.iloc[0]["bike_year"])]
                           })
    print(X_pred)

    y_pred = model.best_estimator_.predict(X_pred)

    y_pred_comparison_to_price = float(price_) - y_pred[0]

    if y_pred_comparison_to_price < 0:
        deal = "Good"
    elif y_pred_comparison_to_price > 0:
        deal = "Bad"
    else:
        deal = "OK"
    return {"predicted": y_pred[0],
            "deal": deal}
