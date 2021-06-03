import datetime
import os
import numpy as np
import pandas as pd

def save_page_list(site_name, req, page=1):
    datetime_1 = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_list_name = site_name + "-" + datetime_1 + "-" + str(page)
    with open("pages/" + page_list_name + ".html", "w")  as file:
        file.write(req.text)
        file.close()

def save_page_uniq(site_name, req, uniq_id):
    datetime_1 = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%M")
    page_name = site_name + "-" + uniq_id + "-" + datetime_1
    with open("annonces/" + page_name + ".html", "w")  as file:
        file.write(req.text)
        file.close()

def add_the_announce(df, uniq_id, price):
    if int(uniq_id) in list(df["unique id"]):
        index_ = df.index[df['unique id'] == int(uniq_id)].tolist()[0]

        # if an announce with same uniq id but different price is found, return true and add
        # or return False and skip
        return df.iloc[index_]["price"]!=price

    # else, return true and add
    return True

def save_temporary_data(csv_file_path_, url_, uniq_id_, price_, title_):

    # load csv if exists or starting from template
    data_path = csv_file_path_
    #
    if os.path.isfile(data_path) is False:
        df =  pd.DataFrame({"url": [np.nan],
                            "unique id": [np.nan],
                            "price": [np.nan],
                            "title": [np.nan]
                            })
    else:
        data = pd.read_csv(data_path)
        df =  pd.DataFrame({"url": [np.nan],
                            "unique id": [np.nan],
                            "price": [np.nan],
                            "title": [np.nan]
                            })

    # adding data
    df["url"] = url_
    df["unique id"] = uniq_id_
    df["price"] = price_
    df["title"] = title_

    # concatenate to csv and write
    try:
        data = pd.concat([data, df], axis=0)
        data.to_csv(path_or_buf = data_path, index=False)
    except:
        df.to_csv(path_or_buf = data_path, index=False)

    return
