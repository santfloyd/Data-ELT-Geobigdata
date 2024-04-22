from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F #importante importar de esta manera o se confunden las librerias max, min y otras
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("proyecto").getOrCreate()

#pregunta 1
print("pregunta 1")
#funcion de transformacion de tipos de datos y formateo de fecha
def format_transform(line):
    try:
        if len(line) >= 4:
            fecha = datetime.strptime(line[0].split('+')[0], '%Y-%m-%dT%H:%M:%S')
            sensor = str(line[1])
            intensidad = int(line[3])
            return fecha, sensor, intensidad
        else:
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
try:
    #lectura de los ficheros en objeto rdd
    rdd = spark.sparkContext.wholeTextFiles('/home/santiago/Desktop/shared_bigdata_def/mayo/')
except Exception as e:
    print(f"Error: {e}")

#limpieza de los datos:
    #I) excluir el fichero de nombre especifico
    #II) excluir el encabezado de cada fichero
    #III) convertir a un solo rdd plano
    #IV) dividir cada linea por separador ','
    #V) excluir filas con None
    #VI) aplicar funcion de transformacion de tipos
    #ViI) excluir Nones y valores -1 y 5000 errores de lectura
    #VIII) mapeo de los valores para procesar despues
rdd_limpio = rdd.filter(lambda x: x[0] != '2023-5-15T11H45m.csv') \
                    .map(lambda x: x[1].split('\n')[1:]) \
                    .flatMap(lambda x: x) \
                    .map(lambda x: x.split(';')) \
                    .filter(lambda x: x is not None) \
                    .map(format_transform) \
                    .filter(lambda x: x is not None) \
                    .filter(lambda x: x is not None and x[2] not in [-1, 5000]) \
                    .map(lambda x: (x[1], x[0], x[2])) \

# rdd_limpio.collect()
# first_lines= rdd_limpio.take(1)
# for line in first_lines:
#     print(line)

#mapea valores necesarios
#agrupa por key, primer valor de la tupla
#calcula el promedio de los valores en cada agrupacion
#ordena por valor
rdd_agrupado = rdd_limpio.map(lambda x: (x[0],x[2])) \
                    .groupByKey() \
                    .mapValues(lambda x: sum(x) / len(x)) \
                    .sortBy(lambda x: x[1], ascending=False) 

rdd_agrupado.collect()
first_lines= rdd_agrupado.take(15)
for line in first_lines:
    print(line)

#pregunta 2
print("pregunta 2")
#.mapValues(list) \ para debug despues del groupby usar esta linea para ver 
#los valores de la agrupacio-
rdd_dia_mean = rdd_limpio.map(lambda x: ((x[1].day, x[0]), x[2])) \
                          .groupByKey() \
                          .mapValues(lambda values: sum(values) / len(values)) \
                          .map(lambda x: (x[0][0], x[0][1], x[1])) \
                          .filter(lambda x: x[1] == "101") \
                          .sortBy(lambda x: x[0], ascending=True)
                          

rdd_dia_mean.collect()
first_day = rdd_dia_mean.take(31)
for line in first_day:
    print(line)

#pregunta 3
print("pregunta 3")
#funcion para calcular el maximo de valores promediados
#identica a la funcion de reduccion realizada con mapreduce en hadoop
def dia_max(tupla):
    sensor = tupla[0]
    dia_lectura = tupla[1]
    
    promedios_dia = {}
    for dia, lectura in dia_lectura:
        if dia in promedios_dia:
            total, numElements = promedios_dia[dia]
        else:
            total, numElements = (0,0)
        total += lectura
        numElements += 1
        promedios_dia[dia] = (total, numElements)
    
    max_promedio = 0
    max_dia = None

    for dia, (total, numElements) in promedios_dia.items():
        promedio = float(total/numElements)
        
        if promedio > max_promedio:
            max_promedio = promedio
            max_dia = dia
    return  sensor, max_promedio, max_dia
#  .mapValues(list) \
rdd_sensor_max = rdd_limpio.map(lambda x: (x[0], (x[1].day, x[2]))) \
                          .groupByKey() \
                           .map(dia_max) \
                           .filter(lambda x: x[0] == "111")

# #filtrar para el sensor 111 el dia deberia ser el 24

rdd_sensor_max.collect()
records = rdd_sensor_max.take(10)
for line in records:
    print(line)


# rdd_dia_max = rdd_limpio.map(lambda x: (x[1].day, x[2])) \
#                           .groupByKey() \
#                            .mapValues(lambda values: sum(values) / len(values)) \
#                            .sortBy(lambda x: x[0], ascending=True)

# rdd_dia_max.collect()
# records = rdd_dia_max.take(30)
# for line in records:
#     print(line)

#pregunta 4
print("pregunta 4")
rdd_sensores_dia_promedio = rdd_limpio.filter(lambda x: x[2] != 5000) \
                                .map(lambda x: (x[1].day, x[2])) \
                                .groupByKey() \
                                .mapValues(list) \
                                .mapValues(lambda values: sum(values) / len(values)) \
                                .sortBy(lambda x: x[0], ascending=True)

rdd_sensores_dia_promedio.collect()
records = rdd_sensores_dia_promedio.take(1)
for line in records:
    print(line)

#pregunta 5
print("pregunta 5")
rdd_estaciones = spark.sparkContext.textFile("/home/santiago/Desktop/Bigdata_scripts/spark/estaciones_no2.csv")

def format_transform2(line):
    try:
        if len(line) >= 3 and line[2] != 'None' and line[2] != '':
            fecha = datetime.strptime(line[1], '%Y-%m-%dT%H:%M:%S.%f')
            estacion = str(line[0])
            NO2 = int(line[2])
            return fecha, estacion, NO2
        else:
            return None
    except ValueError as ve:
        print(f"ValueError: {ve}, Line: {line}")
        return None
    except Exception as e:
        print(f"Error: {e}, Line: {line}")
        return None


rdd_estaciones_dia_promedio = rdd_estaciones.map(lambda x: x.split('\n')) \
                                .flatMap(lambda x: x) \
                                .map(lambda x: x.split(',')) \
                                .map(format_transform2) \
                                .filter(lambda x : x is not None) \
                                .map(lambda x: (x[0].day, x[2])) \
                                .groupByKey() \
                                .mapValues(lambda values: sum(values) / len(values)) \
                                .sortBy(lambda x: x[0], ascending=True)

                                
rdd_estaciones_dia_promedio.collect()
records = rdd_estaciones_dia_promedio.take(30)
for line in records:
    print(line)               


schema_NO2 = StructType([
    StructField("dia", StringType(), True),
    StructField("NO2", DoubleType(), True)
      
])

schema_trafico = StructType([
    StructField("dia1", StringType(), True),
    StructField("trafico", DoubleType(), True)
    
])


#pregunta 6
print("pregunta 6")
df_NO2 = spark.createDataFrame(rdd_estaciones_dia_promedio, schema=schema_NO2)
df_trafico = spark.createDataFrame(rdd_sensores_dia_promedio, schema=schema_trafico)

df_joined = df_trafico.join(df_NO2, df_trafico.dia1 == df_NO2.dia, how='inner')
df_joined.show()

correlation = df_joined.select(F.col('trafico'), F.col('NO2'))\
                        .stat.corr('trafico', 'NO2')

print("Correlacion NO2 Y TRAFICO:", correlation)

pandas_df_NO2 = df_NO2.toPandas()
pandas_df_trafico = df_trafico.toPandas()


fig,ax = plt.subplots()

ax.plot(pandas_df_NO2.dia,pandas_df_NO2.NO2,color='red',marker='o', linestyle='-', linewidth=2, markersize=8)
#twinx para generar otro eje y
ax2 = ax.twinx()
ax2.plot(pandas_df_trafico.dia1,pandas_df_trafico.trafico,color='blue',marker='o', linestyle='-', linewidth=2, markersize=8)



plt.xlabel('dia')
ax.set_ylabel('Tráfico', color='blue')
ax2.set_ylabel('NO2', color='red')
plt.title('Promedio por dia NO2 y trafico. Correlacion:{correlacion}'.format(correlacion=correlation))

ax.legend(loc='upper left', bbox_to_anchor=(0.8, 1.0), fontsize='medium')  
ax2.legend(loc='upper left', bbox_to_anchor=(0.8, 0.9), fontsize='medium') 

plt.show()



