import requests
from bs4 import BeautifulSoup
from PIL import Image
import os
import glob
import pandas as pd
import time
import random
import codecs
import shutil
from datetime import datetime
import re

def scraping_to_dataframe():
    step = 1
    
    def extract_int(sample):
        list = re.findall(r'\d+', sample)
        while True:
            try:
                res = int("".join(map(str, list)))
                break
            except ValueError:
                res = np.nan
                break
        return res
    success = 0
    fail = 0
    try:
        source = 'as_24'

        # Start time
        start_time = datetime.now()

        # directory of html annonces
        directory = 'annonces'

        # log update
        log_import = pd.read_csv('log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['started'],
                                'time': [datetime.now()],
                                'details': [""]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('log.csv', index=False)

        # import index
        index_df = pd.read_csv('as_24_annonces_light.csv')
        #loop sur le html de chaque annonce
        
        #ouverture de annonces_light dans lequel figure l'url pour le guid
        annonces_light = pd.read_csv('as_24_annonces_light.csv')
        
        for filename in [file for file in os.listdir(directory) if file.endswith(".html")]:
            #initialize the DataFrame
            #============================================================================
            columns=['reference', 'model', 'price', 'mileage', 'power_kW', 'power_CH', 
                             'État', 'Propriétaires préc.', 'Dernier contrôle technique', 'Garantie', 
                             "Carnet d'entretien", 'Marque', "N° d'annonce", 'Année', 'Couleur extérieure', 
                            'Type de peinture', 'Couleur originale', 'Carrosserie', 'Transmission', 'Vitesses', 
                            'Cylindrée', 'Cylindres', 'Poids à vide', 'Description','url']
            
            df_announces = pd.DataFrame(columns=columns)
            
            #Différentes parties de l'annonce et du DataFrame
            part_one_row = {"reference": np.nan,
                  "model": "",
                  "price": np.nan,
                  "mileage": 0,
                  "power_kW": 0,
                  "power_CH": 0}
            etat_features = ["État", "Propriétaires préc.", "Dernier contrôle technique", "Garantie", "Carnet d'entretien"]
            etat_row = {"État": "", 
                      "Propriétaires préc.": np.nan,
                      "Dernier contrôle technique": datetime(1970, 1, 1),
                      "Garantie": datetime(1970, 1, 1),
                      "Carnet d'entretien": np.nan}
            car_features = ["Marque", "N° d'annonce", "Année", "Couleur extérieure", "Type de peinture", "Couleur originale", "Carrosserie"]
            car_row = {"Marque": "", "N° d'annonce": np.NaN, 
                      "Année": [""], 
                      "Couleur extérieure": "", 
                      "Type de peinture": "", 
                      "Couleur originale": "", 
                      "Carrosserie": ""}
            transm_features = ["Transmission", "Vitesses", "Cylindrée", "Cylindres", "Poids à vide"]
            transm_row = {"Transmission": "", 
                          "Vitesses": np.nan, 
                          "Cylindrée": np.nan, 
                          "Cylindres": 0, 
                          "Poids à vide" : np.nan,
                          "Description" : "",
                          "url": ""
                         }
            
            #request depuis les fichiers html
            f = codecs.open(f"{directory}/{filename}", 'r')
            soup_ = BeautifulSoup(f, "html.parser")
            reference = filename.split('-')[1] #soit le guid donc ref = guid
            uniq_id = source + '-' + reference
#             file_name = source + '-' + announces_row['guid'] + '-' + start_time.strftime("%Y-%m-%d_%Hh%M")

            try:
                #première encart informations majeures de l'annonce
                guid = soup_.find("div", class_="sc-pull-right cldt-buttons cldt-hide-on-print cldt-version-one")
                guid = guid.find("a", class_="btn-watchlist").attrs['data-classified-guid']
                part_one_row["reference"] = "as24_" + guid
                part_one_row["model"] = soup_.find("div", class_="cldt-headline").find("span", class_="cldt-detail-makemodel sc-ellipsis").text
                part_one_row["price"] = extract_int(soup_.find("div", class_="cldt-price").find('h2').text)

                basic_data = soup_.find("div", class_='cldt-stage-basic-data')
                part_one_row["mileage"] = extract_int(basic_data.find_all("span", class_="sc-font-l cldt-stage-primary-keyfact")[0].string)
                part_one_row["power_kW"] = extract_int(basic_data.find_all("span", class_="sc-font-l cldt-stage-primary-keyfact")[2].string)
                part_one_row["power_CH"] = extract_int(basic_data.find_all("span", class_="sc-font-m cldt-stage-primary-keyfact")[0].string)

                #scrapping de la branche contenant les key/values de Etat
                etat = soup_.find("div", class_="cldt-categorized-data cldt-data-section sc-pull-left")
                #création liste des key et des valeurs => création dictionnaire de key, values
                etat_key = [i.text for i in etat.find_all("dt")]
                etat_value = [i.text.replace('\n', '') for i in etat.find_all("dd")]
                etat_dict = {k: v for k,v in zip(etat_key, etat_value)}
                #crée un dictionnaire correspondant à la ligne qu'il faudra ajouter au dataframe
                for k_site in [i.text for i in etat.find_all("dt")]:
                    for k_list in etat_features:
                        if k_site == k_list:
                            etat_row[k_list]=etat_dict[k_site]
                            
                #scrapping de la branche contenant les key/values de caractéristiques
                car = soup_.find("div", class_="cldt-categorized-data cldt-data-section sc-pull-right")
                #création liste des key et des valeurs => création dictionnaire de key, values
                car_key = [i.text for i in car.find_all("dt")]
                car_value = [i.text.replace('\n', '') for i in car.find_all("dd")]
                car_dict = {k: v for k,v in zip(car_key, car_value)}
                #crée un dictionnaire correspondant à la ligne qu'il faudra ajouter au dataframe
                for k_site in [i.text for i in car.find_all("dt")]:
                    for k_list in car_features:
                        if k_site == k_list:
                            car_row[k_list]=car_dict[k_site]
                            
                #scrapping de la branche contenant les key/values de caractéristiques
                transm = soup_.find_all("div", class_="cldt-categorized-data cldt-data-section sc-pull-left")[1]
                #création liste des key et des valeurs => création dictionnaire de key, values
                transm_key = [i.text for i in transm.find_all("dt")]
                transm_value = [i.text.replace('\n', '') for i in transm.find_all("dd")]
                transm_dict = {k: v for k,v in zip(transm_key, transm_value)}
                #crée un dictionnaire correspondant à la ligne qu'il faudra ajouter au dataframe
                for k_site in [i.text for i in transm.find_all("dt")]:
                    for k_list in transm_features:
                        if k_site == k_list:
                            transm_row[k_list]=transm_dict[k_site]
                transm_row["Description"]=soup_.find_all("div", class_="sc-grid-col-6 sc-grid-col-s-12")[1].text
                transm_row["Description"]=transm_row["Description"].replace('\n','').replace('Afficher plus','').replace('Afficher moins','')
                
                #========= test de l'ajout de l'url dans le dataframe ============
                #guid existe déjà plus haut
                guid_full = part_one_row["reference"].replace('as24_','')
                #identification de la ligne du guid dans annonces_light &extract url de annonce light
                url = annonces_light.loc[annonces_light['guid']==guid_full].iloc[0,3]
                transm_row["url"]=url                
                #=================================================================
                
                
                #========= ajouter la sauvegarde d'images ======================
                
                container = soup_.find_all('div',class_="gallery-picture")
                images_list = []
                uniq_id = 'as24' + '-' + guid
                
                if reference not in [file.split('-')[1] for file in glob.glob('img/*')]:
                    for image in container:
                        images_list.append(image.find("img").attrs['data-fullscreen-src'])

                    k = 0
                    for image_url in images_list[0:3]:
                        img_data = requests.get(image_url).content
                        with open(f'images/{uniq_id}-{k}.jpg', 'wb') as handler:
                            handler.write(img_data)
                            image = Image.open(f'images/{uniq_id}-{k}.jpg') 
                            ratio = image.size[0] / image.size[1]
                            image = image.resize((300,int(300/ratio)))
                            image.save(f'images/{uniq_id}-{k}.jpg',optimize = True, quality = 50)
                        k+=1
                #===============================================================
                
                #concaténation des différents dataframes
                csv_destination = 'as_24_annonces_full.csv'
                
                dall = {}
                dall.update(part_one_row)
                dall.update(etat_row)
                dall.update(car_row)
                dall.update(transm_row)
                df_dall = pd.DataFrame([dall])
                df_dall = df_dall.reindex(columns = columns)
                
                #dictionnaire pour renommer les colonnces
                rename_columns = {'État':'etat', 'Propriétaires préc.':'proprietaires_prec.', 
                                  'Dernier contrôle technique':'dernier_controle_technique', 'Garantie':'garantie', 
                                  "Carnet d'entretien":"carnet_dentretien", 'Marque':'marque', "N° d'annonce":"n_dannonce",
                                  'Année':'annee', 'Couleur extérieure':'couleur_exterieure',
                                  'Type de peinture':'type_de_peinture', 'Couleur originale':'couleur_originale',
                                  'Carrosserie':'carrosserie', 'Transmission':'transmission', 'Vitesses':'vitesses',
                                  'Cylindrée':'cylindree', 'Cylindres':'cylindres', 'Poids à vide':'poids_a_vide', 
                                  'Description':'description', 'url':'url'}
                
                #ici je renomme les colonnes du df
                df_dall = df_dall.rename(columns=rename_columns)
                
                # import history
                history = pd.read_csv(csv_destination)

                #concatenate new and history
                final_df = history.append(df_dall, ignore_index=True)
                
                # export to csv
                final_df.to_csv(csv_destination, index=False)
                print(step, ' : success scraping')
                success +=1
            except:
                print(step, ' : failed scraping')
                fail +=1
                
            # move file to vault after process
            # source path
            source_folder = f"annonces/{filename}"
            # destination path
            destination = f"annonces/vault/{filename}"
            # Move the content of
            # source to destination
            shutil.move(source_folder, destination)
            
#pas besoin sleep puisque scrap le fichier html en local
#             time.sleep(random.randint(1, 2))

            step +=1
        # End time
        end_time = datetime.now()
        td = end_time - start_time

        # log update
        log_import = pd.read_csv('log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['completed'],
                                'time': [datetime.now()],
                                'details': [f"{td.seconds/60} minutes elapsed"]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('log.csv', index=False)

    except (ValueError, TypeError, NameError, KeyError, RuntimeWarning) as err:
        print('failed moving file')
        # log update
        log_import = pd.read_csv('log.csv')
        log_new = pd.DataFrame({'source': [source],
                                'step': ['to dataframe'],
                                'status': ['error'],
                                'time': [datetime.now()],
                                'details': [err]})
        log = log_import.append(log_new, ignore_index=True)
        log.to_csv('log.csv', index=False)
    print('nombre de motos importées : ', success)
    print('nombre de motos non-importées : ', fail)
    print('ratio total : ', (success/(succes + fail)))
    print('over & out')

if __name__ == "__main__":
    scraping_to_dataframe()