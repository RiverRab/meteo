
#!/usr/bin/env python3
import pandas as pd
import psycopg2
pd.set_option('display.max_columns', None)

chemin_fichier = r'D:\Perso\dossier_partage\projet_script_base_donnée\donnees-synop-essentielles-omm.csv'
indices_colonnes = [0, 1, 6, 9, 64, 73, 74, 77, 79] 
df = pd.read_csv(chemin_fichier, usecols=indices_colonnes, sep=";")


# Affichage des premières lignes du DataFrame pour vérifier
print(df.head())

for index, row in df.iterrows():
    id_omm_station = row['ID OMM station']
    datetime_str = row['Date']
    vitesse_vent = row['Vitesse du vent moyen 10 mn']
    humidite = row['Humidité']
    temperature = row['Température (°C)']
    commune_name = row['communes (name)']
    commune_code = row['communes (code)']
    departement = row['department (name)']
    region = row['region (name)']
    
    # Division de la date et de l'heure
    date, time = datetime_str.split('T')
    heure, _ = time.split('+')
    
    # Maintenant, vous pouvez utiliser ces variables pour insérer les données dans votre base de données
    print(f"Ma station a pour id {id_omm_station}, la mesure a été prise le {date}à  {heure} sur la {commune_name} ayant pour {commune_code}, dans le {departement}, de la {region}. La temperature était de {temperature}, le taux d'humidité était de {humidite} et la vitesse du vent {vitesse_vent}")
