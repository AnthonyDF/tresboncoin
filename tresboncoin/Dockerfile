FROM python:3.8.6-buster
COPY api /api
COPY data.py data.py
COPY parameters.py parameters.py
COPY utils.py utils.py
COPY trainer.py trainer.py
COPY requirements.txt /requirements.txt
COPY models/model.joblib models/model.joblib
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
CMD uvicorn api.fast:app --host 0.0.0.0 --port $PORT
