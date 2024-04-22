import pymongo
from pymongo import MongoClient
import os

client=pymongo.MongoClient("localhost",27017) #'mongodb://localhost:27017'
db = client.proyecto_BG3
collection_trafico = db.trafico_BG3

#funcion de mapeo 
def mapper_mongodb(collection): 
    
    #mongo pipeline
    #crea un campo isodate agrupa por isodate y sensor    
    addfield = { "$addFields": { "isodate": { "$toDate": "$HORA" } } }
    group = {"$group":{"_id":{"fecha":"$isodate","sensor":"$sensor"},"intensidad":{"$first":"$intensidad"}}}
    project = {"$project":{"_id":1,"intensidad":1}}
    #limit = {"$limit":1000}
    

    pipeline = [addfield, group, project] #limit
    #ejecucion mongo pipeline
    data_from_mongo = collection.aggregate(pipeline)
    
    #mapeo
    lista = []
    #mapeo de intensidad por sensor en lista de tuplas
    for doc in data_from_mongo:
        tupla = (doc["_id"]["sensor"], doc["intensidad"])
        lista.append(tupla)

    return lista

#funcion de reduccion
def reducer_mongodb(mapped_from_mongo):

    #diccionario para reduccion por sensor (key)
    sensor_dict = {}

    #por tupla añade al dict el sensor y como valor añade 
    #la primer lectura dentro de una lista
    for tupla in mapped_from_mongo:
        if tupla[0] not in sensor_dict:
            if tupla[1] is not None and (tupla[1]!=-1 or tupla[1]<5000):
                sensor_dict[tupla[0]] = [tupla[1]]
    #si ya existe el sensor añade la siguiente lectura a la lista
    # de cada sensor 
        else:
            if tupla[1] is not None and (tupla[1]!=-1 or tupla[1]<5000):
                sensor_dict[tupla[0]].append(tupla[1])

    #por cada sensor en el dict calcula el promedio
    for key in sensor_dict:
        #dos formas de tratar nulos
        #omitir valores None en lecturas
        sensor_dict[key] =  int(sum(x for x in sensor_dict[key] if x != None) / len(sensor_dict[key]))
        #reemplazar None por 0
        #sensor_dict[key] =  sum(x if x != None else 0 for x in sensor_dict[key]) / len(sensor_dict[key])
        #print(sensor_dict[key])
        
    return sensor_dict

#funcion map_reduce
def map_reduce(collection):
    mapped_data = mapper_mongodb(collection)
    reduced_date = reducer_mongodb(mapped_data) 
    return reduced_date  

reduccion = map_reduce(collection_trafico)

#salida con orden valor - sensor
for key, value in sorted(reduccion.items()):
    print("{} {}".format(value, str(key)))