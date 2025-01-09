import pandas as pd
from ydata_profiling import ProfileReport
from IPython.display import display, HTML
from iotdb.Session import Session
import json

IOTDB_HOST = "127.0.0.1"
IOTDB_PORT = "6667"
USERNAME = "root"
PASSWORD = "root"

# Connexion à IoTDB
session = Session(IOTDB_HOST, IOTDB_PORT, USERNAME, PASSWORD)
session.open(False)

query = "SELECT * FROM root.sg1.device1.projetv7"
result = session.execute_query_statement(query)

df = result.todf()

# Générer un rapport de profilage avec ydata-profiling
profile = ProfileReport(df, title="Demo", explorative=True)

# Calcul completude 
# Une complétude de 1 signifie que toutes les données sont valides (aucune donnée manquante), 
# tandis qu'une complétude de 0 signifierait que toutes les données sont manquantes.
report = json.loads(profile.to_json())

num_rows = report['table']['n']
missing_cells = report['table']['n_cells_missing']
number_variables = report['table']['n_var']
num_total_values = number_variables * num_rows

completude = 1-(missing_cells/num_total_values)

# Sauvegarder le rapport ydata-profiling
profile.to_file("demo.html")

# Ajout du calcul de la complétude au rapport
with open("demo.html", "a") as f:
    f.write(f"<script> const e = document.createElement('h1'); e.textContent = 'Complétude : {completude} '; document.querySelectorAll('.section-items')[0].appendChild(e);  const e2 = document.createElement('p'); e2.textContent = 'Une complétude de 1 signifie que toutes les données sont valides (aucune donnée manquante), et une complétude de 0 signifierait que toutes les données sont manquantes'; document.querySelectorAll('.section-items')[0].appendChild(e2);  </script>")

# --- ANALYSES COMPLÉMENTAIRES POUR LA PONCTUALITÉ ---
if 'time' in df.columns: 
    df['time'] = pd.to_datetime(df['time'], errors='coerce')  # Conversion en datetime
    df['timeliness_diff'] = df['time'].diff().dt.days  # Différence entre les dates
    avg_interval = df['timeliness_diff'].mean()
    missing_dates = df['time'].isnull().sum()
    
    # Ajouter les résultats au rapport
    with open("demo.html", "a") as f:
        f.write("<h2>Timeliness Analysis</h2>")
        f.write(f"<p>Average interval between dates: {avg_interval:.2f} days</p>")
        f.write(f"<p>Number of missing or invalid dates: {missing_dates}</p>")

print("Rapport enrichi de timeliness généré : demo.html")

# --- ANALYSES COMPLÉMENTAIRES POUR LA VALIDITÉ ---
validity_checks = {}

for col in df.columns:
    # Vérifier les colonnes numériques
    if df[col].dtype in ['int64', 'float64']:
        validity_checks[col] = {
            "min_value": df[col].min(),
            "max_value": df[col].max(),
            "out_of_range_count": df[(df[col] < 0) | (df[col] > 100)].shape[0]
        }
    # Vérifier les colonnes catégorielles
    elif df[col].dtype == 'object':
        unique_values = df[col].dropna().unique()  # Valeurs uniques sans NA
        validity_checks[col] = {
            "unique_values": unique_values,
            "invalid_values_count": df[~df[col].isin(unique_values)].shape[0]  # Invalide si hors liste
        }

# Ajouter les résultats au rapport HTML
with open("demo.html", "a") as f:
    f.write("<h2>Validity Analysis</h2>")
    for col, checks in validity_checks.items():
        f.write(f"<h3>Column: {col}</h3>")
        f.write(f"<p>{checks}</p>")
print("Rapport enrichi de validité généré : demo.html")

# --- ANALYSES COMPLÉMENTAIRES POUR LA COHÉRENCE ---
consistency_checks = {
    "duplicates_count": df.duplicated().sum(),
    "unique_values_per_column": {col: df[col].nunique() for col in df.columns},
}

# Ajouter les résultats au rapport HTML
with open("demo.html", "a") as f:
    f.write("<h2>Consistency Analysis</h2>")
    f.write(f"<p>Number of duplicate rows: {consistency_checks['duplicates_count']}</p>")
    for col, unique_count in consistency_checks['unique_values_per_column'].items():
        f.write(f"<p>Column {col}: {unique_count} unique values</p>")

print("Rapport enrichi de cohérence généré : demo.html")

display(HTML("demo.html"))

session.close()