# ----------------------------------
#         API COMMANDS
# ----------------------------------

run_api:
	uvicorn api.fast:app --reload  # load web server with code autoreload

# ----------------------------------
#         HEROKU COMMANDS
# ----------------------------------

# streamlit:
# 	-@streamlit run titanic_app.py

# ----------------------------------
#         GCP COMMANDS
# ----------------------------------

# PROJECT_ID=subtle-bit-312409
# REGION=europe-west1
# BUCKET_NAME=titanic-survivors
#
# set_project:
# 	-@gcloud config set project ${PROJECT_ID}
#
# create_bucket:
# 	-@gsutil mb -l ${REGION} -p ${PROJECT_ID} gs://${BUCKET_NAME}
#
# upload_data:
#     # @gsutil cp train_1k.csv gs://wagon-ml-my-bucket-name/data/train_1k.csv
# 	-@gsutil cp ${LOCAL_PATH} gs://${BUCKET_NAME}/${BUCKET_FOLDER}/${BUCKET_FILE_NAME}
