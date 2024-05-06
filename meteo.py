#/usr/bin/env python3

from meteo.conn import conn # type: ignore
########################################### Partie préparatoire pour insérer les données ###########################################
# Paramètres de connexion
ip = "127.0.0.1"
name = "riverrab"
db = "meteoproject"
port = "5432"
# password = ""

#Connection à la base de donneé
try:
except:
    print("Problème lors de la connexion, vérifier si le serveur et up ou les données entrées")
    
cur = conn.cursor()
# #####################################################################################################################################
# ##### type_id: vitesse_vent= 1 humidité= 2 degré=3
# ############################################ création des fonctions d'insertion ####################################################
vitesse_vent_id = 1
humidite_id = 2
temperature_id = 3

def get_or_create_region(region_name):
    if region_name == "Inconnu":
        print("Erreur region_name vide")
        return
    # Recherche d'une entrée existante
    cur.execute("SELECT id FROM region WHERE nom = %s;", (region_name,))
    result = cur.fetchone()
    
    if result:
        # Si l'entrée existe déjà, retourne l'ID existant
        return result[0]
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO region (nom) VALUES (%s) RETURNING id;", (region_name,))
        new_id = cur.fetchone()[0]
        return new_id
    
def get_or_create_departement(departement, region_id):
    if departement == "Inconnu":
        print("Erreur departement_name vide")
        return
    # Recherche d'une entrée existante
    cur.execute("SELECT id FROM departement WHERE nom = %s;", (departement,))
    result = cur.fetchone()
    
    if result:
        # Si l'entrée existe déjà, retourne l'ID existant
        return result[0]
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO departement (nom, region_id) VALUES (%s, %s) RETURNING id;", (departement, region_id))
        new_id = cur.fetchone()[0]
        return new_id

def get_or_create_commune(commune, code_commune, departement_id):
    if not commune or commune in ["Inconnu", "null", None]:
        print("Erreur : Nom de commune vide")
        return None
    if not departement_id or departement_id in ["Inconnu", "Nan", "null", None]:
        print("Erreur : ID de département vide")
        return None

    cur.execute("SELECT id FROM commune WHERE nom = %s;", (commune,))
    result = cur.fetchone()
        
    if not result:
        print(f"Insertion de la nouvelle commune : {commune}")
        cur.execute("INSERT INTO commune (nom, code_postal, departement_id) VALUES (%s, %s, %s) RETURNING id;", (commune, code_commune, departement_id))
        result = cur.fetchone()[0]
        conn.commit()
        return result
    return result[0]
        

    
def get_or_create_station(station_nom, commune_id):
    if not station_nom or station_nom == "Inconnu":
        print("Erreur: station_nom vide")
        return None  # Retourne None pour éviter les insertions invalides

    if not commune_id:
        print("Erreur: commune_id vide")
        return None  # Retourne None pour éviter les insertions invalides

    # Recherche d'une entrée existante
    cur.execute("SELECT id FROM station_meteo WHERE station = %s;", (station_nom,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO station_meteo (station) VALUES (%s) RETURNING id;", (station_nom,))
        new_id = cur.fetchone()[0]
        return new_id
def get_or_create_station_meteo_provienne_commune(station_id, commune_id):
    try:
        # Validation des entrées
        if not station_id or not commune_id:
            print("Les identifiants station_id et commune_id doivent être valides et non nuls.")
            return None
        if station_id or commune_id == None:
            return None
        # Conversion en entiers pour assurer la validité des types
        station_id_int = int(station_id)
        commune_id_int = int(commune_id)
        # Recherche d'une entrée existante
        cur.execute("SELECT id FROM station_meteo_provienne_commune WHERE station_meteo_id = %s AND commune_id = %s;", (station_id_int, commune_id_int,))
        result = cur.fetchone()
        if result:
            return
        else:
            cur.execute("INSERT INTO station_meteo_provienne_commune (station_meteo_id, commune_id) VALUES (%s, %s);", (station_id, commune_id,))
            conn.commit()
    except:
        print("Erreur avec la table d'association")
def get_or_create_donnee_meteo_temperature(temperature, date, heure, station_id):
    if not temperature or temperature == "NaN":
        print("Erreur: temperature vide")
        return None
    if not date:
        print("Erreur: date vide")
        return None
    if not heure:
        print("Erreur: heure vide")
        return None
    if station_id == None:
        print("erreur pour la temperature à cause de la station ID")
        return None
    # Recherche d'une entrée existante 
    cur.execute("SELECT id FROM meteoproject WHERE donnee = %s AND type_id = %s AND date = %s AND heure = %s AND station_meteo_id = %s;", (temperature, temperature_id, date, heure, station_id))
    result = cur.fetchone()
    
    if result:
        return 
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO meteoproject(donnee, type_id, date, heure, station_meteo_id) VALUES (%s, %s, %s, %s, %s) RETURNING id;", (temperature, temperature_id, date, heure, station_id))
        conn.commit()  # Commit the transaction


def get_or_create_donnee_meteo_humidite(humidite, date, heure,  station_id):
    if not humidite or humidite == "NaN":
        print("Erreur: humidite vide")
        return None  
    if not date:
        print("Erreur: date vide")
        return None  
    if not heure:
        print("Erreur: heure vide")
        return None  
    if station_id == None:
        print("erreur pour l'humidité à cause de la station ID")
        return None
    # Recherche d'une entrée existante 
    cur.execute("SELECT id FROM meteoproject WHERE donnee = %s AND type_id = %s AND date = %s AND heure = %s AND station_meteo_id = %s ;", (humidite, humidite_id, date, heure, station_id,))
    result = cur.fetchone()
    
    if result:
        return 
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO meteoproject(donnee, type_id, date, heure, station_meteo_id ) VALUES (%s, %s, %s, %s, %s) RETURNING id;", (humidite, humidite_id, date, heure, station_id,))
        return
    
def get_or_create_donnee_meteo_vent(vitesse_vent, date, heure,  station_id):
    if not vitesse_vent or vitesse_vent == "NaN":
        print("Erreur: vitesse_vent vide")
        return None  
    if not date:
        print("Erreur: date vide")
        return None  
    if not heure:
        print("Erreur: date vide")
        return None  
    if station_id == None:
        print("erreur pour la vitesse du vent à cause de la station ID")
        return None
    # Recherche d'une entrée existante 
    cur.execute("SELECT id FROM meteoproject WHERE donnee = %s AND type_id = %s AND date = %s AND heure = %s AND station_meteo_id = %s ;", (vitesse_vent, vitesse_vent_id, date, heure, station_id,))
    result = cur.fetchone()
    
    if result:
        return
    else:
        # Si l'entrée n'existe pas, insère la nouvelle entrée et retourne son ID
        cur.execute("INSERT INTO meteoproject(donnee, type_id, date, heure, station_meteo_id ) VALUES (%s, %s, %s, %s, %s) RETURNING id;", (vitesse_vent, vitesse_vent_id, date, heure, station_id,))
        