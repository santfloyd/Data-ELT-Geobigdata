from mrjob.job import MRJob
from datetime import datetime
from mrjob.step import MRStep

class MRJobP1_8(MRJob):
    def mapper(self, key, line):        
        line_read=line.split(';')
        try:
            #incluir en bloque try porque al leer del hdfs 
            # tambien se incluye el header, daría error
            fecha=line_read[0].replace('+02:00','+0200')
            fecha1=datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%S%z') 
            dia_semana=fecha1.weekday() #Return the day of the week as an integer, where Monday is 0 and Sunday is 6
            sensor=line_read[1]
            intensidad=int(line_read[3])
            yield sensor, (dia_semana, intensidad)                        
        except:
            pass
    
    def reducer(self, sensor, tupla):
        promedios_dia = {}
        for dia_semana, lectura in tupla:
            try:
                if dia_semana in promedios_dia:
                    total, numElements = promedios_dia[dia_semana]
                else:
                    total, numElements = (0, 0)
                total += lectura
                numElements += 1
                # actualiza los valores calculados
                promedios_dia[dia_semana] = (total, numElements)
                    
            except:
                pass
        for dia_semana, (total, numElements) in promedios_dia.items():
            try:
                yield dia_semana, float(total / numElements)
            except:
                pass
    
    def reducer2(self, dia_semana, avg):
        total=0
        numElements=0
        for x in avg:
            try:
                total+=x
                numElements +=1
            except:
                pass
        try:
            yield dia_semana, float(total/ (numElements))
        except:
            pass

    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducer2)]

if __name__=='__main__':
    MRJobP1_8.run()



