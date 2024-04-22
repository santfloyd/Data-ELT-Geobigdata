import pymongo
from pymongo import MongoClient
import pandas as pd
import pydoop.hdfs as hdfs


client=pymongo.MongoClient("localhost",27017) #'mongodb://localhost:27017'
db = client.proyecto_BG3
collection_avfrancia = db.av_francia

#union de colecciones
pipeline_merge = [
  {"$unionWith": {"coll": "bulevard_sud"}},
  {"$unionWith": {"coll": "centre"}},
  {"$unionWith": {"coll":"conselleria_meteo"}},
  {"$unionWith": {"coll": "moli_sol"}},
  {"$unionWith": {"coll": "nazaret"}},
  {"$unionWith": {"coll": "olivereta"}},
  {"$unionWith": {"coll": "pista_silla"}},
  {"$unionWith": {"coll": "politecnic"}},
  {"$unionWith": {"coll": "port_llit"}},
  {"$unionWith": {"coll": "port_moll"}},
  {"$unionWith": {"coll":"vivers"}},
  {"$out":"estaciones_merge"}
   ]

db.av_francia.aggregate(pipeline_merge)

collection_estaciones = db.estaciones_merge

#comprobaciones
print(collection_estaciones.estimated_document_count())

#mapeo del diccionario
def mapper(coleccion_meteo):
    
    pipeline_NO2 = [{"$unwind":"$lecturas"},
                {"$project":{"_id":0,"nombre":1,
                 "lecturas.NO2":1,
                 "lecturas.FECHA_COMPLETA":1}}]

    collection = list(coleccion_meteo.aggregate(pipeline_NO2))

    #lista output final
    list_dict = []

    #por cada entrada de estacion, lectura, fecha
    for i in collection:
        try:

            collection_dict = {}
            collection_dict['nombre'] = i['nombre'] 
            collection_dict['fecha']= i['lecturas']['FECHA_COMPLETA']
            collection_dict['NO2'] = i['lecturas']['NO2']
            #añade a la lista de diccionarios
            list_dict.append(collection_dict)
        except:
            pass
    
    return list_dict


def reducer(mapped_data):
    df = pd.DataFrame(mapped_data)    
        
    return df

#escribir a local y cargar a hdfs
def writeToLocal_Tohdfs(reduced_data):
    from_path = '/home/santiago/Desktop/Bigdata_scripts/hadoop/NO2_fecha_est.csv'
    to_path = 'hdfs://localhost:9000/input/NO2.csv'
    reduced_data.to_csv(from_path, index=False)
    hdfs.put(from_path, to_path)
    

#funcion map_reduce
def map_reduce(collection):
    mapped = mapper(collection)
    reduced = reducer(mapped) 
    write_data = writeToLocal_Tohdfs(reduced)
    return reduced  
    

reduccion = map_reduce(collection_estaciones)




