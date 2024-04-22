import pymongo
from hdfs import InsecureClient

# Configura la conexión a MongoDB
client = pymongo.MongoClient('localhost', 27017)
db = client.contaminacion_2023
collection = db.EstacionesUnidas

# Ruta de salida local
output_path = '/home/melanie/Documents/BIGDATA/MAP_REDUCE/estaciones_no2.csv'

# Obtiene los datos de MongoDB y escribe en un archivo CSV local
with open(output_path, 'w') as local_file:
    for document in collection.find({}, {'_id': 0, 'id_est': 1, 'fecha': 1, 'NO2': 1}):
        estacion_id = document['id_est']
        fecha = document['fecha']
        no2 = document['NO2']
        local_file.write(f'{estacion_id},{fecha},{no2}\n')

print(f'Archivo CSV generado localmente en: {output_path}')

# Lee las primeras líneas del archivo local
with open(output_path, 'r') as local_file:
    for _ in range(5):  # Lee las primeras 5 líneas
        print(local_file.readline().strip())
