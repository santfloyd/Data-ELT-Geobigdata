from mrjob.job import MRJob
from datetime import datetime
from mrjob.step import MRStep
#comando en cmd
#python3 mrjob_P1_7.py -r hadoop hdfs:///input/NO2.csv

class MRJobP1_7(MRJob):
    def mapper(self, key, line):        
        line_read=line.split(',')
        try:
            #incluir en bloque try porque al leer del hdfs 
            # tambien se incluye el header, daría error
            nombre = line_read[0]
            fecha=line_read[1]
            fecha1=datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S.%f")
            dia=fecha1.day
            lectura=int(line_read[2])
            yield nombre, (dia, lectura)                        
        except:
            pass
    
    def reducer(self, nombre, tupla):
        promedios_dia = {}
        for dia, lectura in tupla:
            try:
                if dia in promedios_dia:
                    total, numElements = promedios_dia[dia]
                else:
                    total, numElements = (0, 0)
                total += lectura
                numElements += 1
                # actualiza los valores calculados
                promedios_dia[dia] = (total, numElements)
                    
            except:
                pass
        for dia, (total, numElements) in promedios_dia.items():
            try:
                yield dia, (nombre, float(total / numElements))
            except:
                pass

    def mapper2(self, dia, tupla):
        nombre=tupla[0]
        promedio=tupla[1]
        yield nombre, (promedio,dia)

    def reducer2(self, nombre, tupla):

        yield nombre, max(tupla)
    #incluye una funcion step que usa el atributo
    #MRStep de MRJobs para definir los steps
    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(mapper=self.mapper2,
                        reducer=self.reducer2)]

        
        
if __name__=='__main__':
    MRJobP1_7.run()



