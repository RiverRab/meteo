#!/usr/bin/bash


#je définis les variables utiles pour se connecter sur le serveur + crée fichier_boucle qui me sera utile dans la partie suivant pour faire ma boucle
#cat /home/kali/dossier_partage/projet_script_base_donnée/donnee_synop_modifV1 | grep 2024 >/home/kali/dossier_partage/projet_script_base_donnée/donnée_2024
IP=127.0.0.1 # addresse à mettre en cours: 192.168.150.117
NOM=poulot
DB=station_meteo
port=5433 
export PGPASSWORD=1234

# je fais ma boucle et extrait mes variables  

while read -r ligne ;do
	#echo "le contenu de ma ligne est "${ligne}" elle est composé de "${date}", "${heure}", "${vitesse_vent}", "${temperature}","${humidite}","${commune_nom}","${commune_cp}","${departement}","${region}""
	date=$(echo ${ligne} |cut -d ";" -f 1 |tr "T" ":"| cut -d ":" -f1)
	heure=$(echo ${ligne} | cut -d ";" -f 1 |tr "T" ":"| cut -d ":" -f 2,3)
	vitesse_vent=$(echo ${ligne} | cut -d ";" -f 2)
	temperature=$(echo ${ligne} | cut -d ";" -f 3)
	humidite=$(echo ${ligne} | cut -d ";" -f 4)
	commune_nom=$(echo ${ligne} | cut -d ";" -f 5 |sed "s/'/''/g")
	commune_cp=$(echo ${ligne} | cut -d ";" -f 6 )
	departement=$(echo ${ligne} | cut -d ";" -f 7 |sed "s/'/''/g")
	region=$(echo ${ligne} | cut -d ";" -f 8 |sed "s/'/''/g")
	[ -z "${date}" ] || [ -z "${heure}" ] || [ -z "${temperature}" ] || [ -z "${commune_nom}" ] && echo "il manque des donnée je passe à la ligne suivante" && continue
	# je gere les données de la table date_table 
	#echo "mes donnée de ma table date_table sont "${heure}"et "${date}"ainsi que "${date_heure_existant}""
	date_heure_existant=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM date_table WHERE heure = '${heure}'AND date_prise = '${date}';")
    [ -z "${date_heure_existant}" ] && psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO date_table (heure, date_prise) VALUES ('${heure}', '${date}');"
	# je gère les donnees de ma table_regions
	region_existant=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM region WHERE nom = '${region}';")
	[ -n "${region}"  ] && [ -z "${region_existant}" ] && psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO region (nom) VALUES ('${region}');"
	echo "mes donnée de ma table table_region sont "${region}"et "${region_existant}""
	# Je gère les données de la table département
	departement_existant=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM departement WHERE nom = '${departement}';")
	region_existant2=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM region WHERE nom = '${region}';")
	[ -n ${departement} ] && [ -z "${departement_existant}" ] && [ -n "${region_existant2}" ] &&  psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO departement (nom, region_id) VALUES ('${departement}', ${region_existant2});"
	echo "mes donnée de ma table table_departement sont "${departement}"et "${departement_existant}" et "${region_existant2}""
	# je gère les données de ma table commune 
	commune_existant=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM commune WHERE nom = '${commune_nom}';")
	departement_existant2=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM departement WHERE nom = '${departement}';")
	[ -n "${commune_nom}" ] && [ -n "${departement_existant2}" ] &&  [ -z "${commune_existant}" ] && psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO commune (nom, code_postal, departement_id ) VALUES ('${commune_nom}', '${commune_cp}', ${departement_existant2});"
	echo "mes donnée de ma table table_commune sont "${commune_nom}"et "${departement_existant2}" et "${commune_existant}" et "${commune_cp}""
	# je gère les données de ma table meteo
	date_heure_existant2=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM date_table WHERE heure = '${heure}'AND date_prise = '${date}';")
	meteo_existant=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM donnee_meteo WHERE date_id = ${date_heure_existant2};")
	[ -n "${vitesse_vent}" ] && [ -n "${temperature}" ] && [ -n "${humidite}" ] && [ -n "${date_heure_existant2}" ] && [ -z "${meteo_existant}" ] && psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO donnee_meteo (temperature, vitesse_vent, humidite, date_id) VALUES ('${temperature}', '${vitesse_vent}', '${humidite}', ${date_heure_existant2});"
	#echo "mes donnée de ma table table_meteo sont "${vitesse_vent}"et "${temperature}" et "${humidite}" et "${date_heure_existant2}" ainsi que "${meteo_existant}" "
	# je gère les donnée de ma table de jointure donnee_meteo_provienne_commune
	meteo_existant2=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM donnee_meteo WHERE date_id = ${date_heure_existant2};")
	commune_existant2=$(psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -t -c "SELECT id  FROM commune WHERE nom = '${commune_nom}';")
	# [ -n "${meteo_existant2}"] && [ -n "${commune_existant2}"] && psql -h ${IP} -U ${NOM} -d ${DB} -p ${port}  -c "INSERT INTO donnee_meteo_provienne_commune (donnee_meteo_id, commune_id) VALUES (${meteo_existant2}, ${commune_existant2} );"
	if [ -n "${meteo_existant2}" ] && [ -n "${commune_existant2}" ]; then
    	psql -h ${IP} -U ${NOM} -d ${DB} -p ${port} -c "INSERT INTO donnee_meteo_provienne_commune (donnee_meteo_id, commune_id) VALUES (${meteo_existant2}, ${commune_existant2});"
	else
    	echo "Une des valeurs requises est vide, insertion annulée."
	fi

	#echo "mes donnée de ma table table_jointure sont "${meteo_existant2}"et "${commune_existant2}}" et c'est tout ceci étant la dernière table "
done < /home/kali/dossier_partage/projet_script_base_donnée/donnée_2024
