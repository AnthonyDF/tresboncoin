from fastapi import FastAPI
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from tresboncoin.utils import get_model
from tresboncoin.data_ import concat_df, get_raw_data, get_data, append
from tresboncoin.data_ import get_new_data, clean_raw_data, get_motorcycle_db
from tresboncoin.fuzzy_match import fuzzy_match_one, fuzzy_match
from tresboncoin.utils import km_per_year, get_model_cloud_storage


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
    model = get_model_cloud_storage()

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


@app.get("/process_data")
def process_data():
    concat_df()
    new_data = get_new_data(clean_raw_data(get_raw_data()), get_data())
    print("New data to be matched. Shape:" + str(new_data.shape))
    if not new_data.empty:
        print('Fuzzy match in progress, wait...')
        new_data_matched = fuzzy_match(new_data, get_motorcycle_db())
        print("Fuzzy match completed. Shape:" + str(new_data_matched.shape))
        history = append(new_data_matched, get_data())
        print("New dataframe avaialble. Shape:" + str(history.shape))
        return {"Fuzzy match csv file size (number of lines)": int(history.shape[0])}
    else:
        print('No new data to match')
        data = pd.read_csv("gs://tresboncoin/master_with_fuzzy_and_cleaning.csv")
        return {"Fuzzy match csv file size (number of lines)": int(data.shape[0])}

