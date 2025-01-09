import csv
from datetime import datetime
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType

# Paramètres de connexion IoTDB
IOTDB_HOST = "127.0.0.1"
IOTDB_PORT = "6667"
USERNAME = "root"
PASSWORD = "root"

# Connexion à IoTDB
session = Session(IOTDB_HOST, IOTDB_PORT, USERNAME, PASSWORD)
session.open(False)

# Fichier CSV et chemin IoTDB
csv_file = "exchange_rate_with_missing.csv"
timeseries_path = "root.sg1.device1.projetv7"

# Fonction pour convertir une date au format AAAA/MM/JJ HH:MM en timestamp
def convertir_date_en_timestamp(date_str):
    dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
    timestamp = int(dt.timestamp() * 1000)  # Conversion en millisecondes
    return timestamp

# Fonction pour vérifier et convertir les valeurs
def checkIfRowEmpty(row):
    try:
        if row is None or row.strip() == '':
            return None
        return float(row)
    except ValueError:
        return None

# Mesures et Types de Données
measurements = ["A", "B", "C", "D", "E", "F", "G", "OT"]
data_types = [TSDataType.FLOAT] * len(measurements)

try:
    with open(csv_file, "r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Sauter l'en-tête du CSV

        for row in csv_reader:
            # print(row, len(row))
            
            date_str = row[0]
            values = [checkIfRowEmpty(row[i]) for i in range(1, 9)]
            timestamp = convertir_date_en_timestamp(date_str)
            
            # Filtrer les valeurs manquantes et ajuster les mesures/types
            filtered_measurements = [measurements[i] for i in range(len(values)) if values[i] is not None]
            filtered_values = [values[i] for i in range(len(values)) if values[i] is not None]
            filtered_data_types = [data_types[i] for i in range(len(values)) if values[i] is not None]
            
            if filtered_measurements:  # Insérer seulement s'il y a des données valides
                session.insert_record(
                    device_id=timeseries_path,
                    timestamp=timestamp,
                    measurements=filtered_measurements,
                    values=filtered_values,
                    data_types=filtered_data_types
                )
                print(f"Inséré : date={date_str}, timestamp={timestamp}, values={filtered_values}")
            else:
                print(f"Aucune donnée valide pour la ligne : {row}")

    print("Insertion terminée avec succès.")

except Exception as e:
    print(f"Erreur lors de l'insertion : {e}")

finally:
    session.close()
