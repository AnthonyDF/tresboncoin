from fastapi import FastAPI
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from tresboncoin.utils import get_model


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
def predict_price(brand_, cc_, year_, mileage_, price_, model_):
    model = get_model("model")

    # Fuzzy match from input parameters
    # TO ADD

    X_pred = pd.DataFrame({"category_db": ["scooter"],
                           "bike_year": [2016],
                           "mileage": [20000],
                           "km/year": [20000]
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
