# import pandas as pd
# from iotdb.Session import Session

# # Configurer la connexion
# host = "127.0.0.1"
# port = "6667"
# user = "root"
# password = "root"
# session = Session(host, port, user, password)
# session.open()

# # Lire le fichier CSV
# csv_file = "/home/sandra/Downloads/ETTh1_without_missing.csv"
# data = pd.read_csv(csv_file)

# # Vérifier les colonnes
# required_columns = {"Id", "date", "OT"}
# if not required_columns.issubset(data.columns):
#     raise ValueError(f"Le fichier CSV doit contenir les colonnes suivantes : {required_columns}")

# # Boucle pour insérer les données
# for index, row in data.iterrows():
#     # Convertir la colonne 'date' en timestamp (millisecondes)
#     timestamp = int(pd.to_datetime(row['date']).timestamp() * 1000)
    
#     # Récupérer les valeurs nécessaires et les convertir en float
#     try:
#         value = float(row['OT'])  # Convertir OT en float
#     except ValueError:
#         print(f"Valeur non valide pour OT à la ligne {index}: {row['OT']}")
#         continue

#     device_id = f"root.sg1.device{row['Id']}"  # Utiliser 'Id' pour définir un périphérique unique
#     values = [value]
#     measurements = ["OT"]
#     data_types = ["FLOAT"]  # Utilisez "TEXT" si "FLOAT" échoue encore

#     # Insérer les données dans IoTDB
#     try:
#         session.insert_record(device_id, timestamp, measurements, data_types, values)
#     except Exception as e:
#         print(f"Erreur lors de l'insertion à la ligne {index}: {e}")
#         continue

# # Fermer la session
# session.close()

from iotdb.Session import Session
import csv
from datetime import datetime
from iotdb.utils.IoTDBConstants import TSDataType  # Pour spécifier les types de données

# Configurer la connexion à votre instance IoTDB
IOTDB_HOST = "127.0.0.1"
IOTDB_PORT = "6667"
USERNAME = "root"
PASSWORD = "root"

# Connexion à IoTDB
session = Session(IOTDB_HOST, IOTDB_PORT, USERNAME, PASSWORD)
session.open(False)

# Chemin vers votre fichier CSV
csv_file = "/home/sandra/Downloads/ETTh1_without_missing.csv"

# Nom du chemin des séries temporelles
timeseries_path = "root.votre_appareil.temperature"

# Fonction pour convertir la date au format AAAA:MM:JJ HH:MM:SS en timestamp
def convertir_date_en_timestamp(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")  # Adapter au format de votre CSV
    timestamp = int(dt.timestamp() * 1000)  # Conversion en millisecondes
    return timestamp

try:
    with open(csv_file, "r") as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Sauter l'en-tête du CSV

            for row in csv_reader:
                date_str, value = row[1], float(row[2])  # Adapter selon votre CSV
                timestamp = convertir_date_en_timestamp(date_str)
                # Insérer les données dans IoTDB
                session.insert_record(
                     device_id="root.sg1.device1.projetv1", 
                     timestamp=timestamp, 
                     measurements=["OT"], 
                     values=[value], 
                     data_types=[TSDataType.FLOAT])
                
                print(f"Inséré : date={date_str}, timestamp={timestamp}, value={value}")

    print("Insertion terminée avec succès.")

except Exception as e:
    print(f"Erreur lors de l'insertion : {e}")

finally:
    session.close()

