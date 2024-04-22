from mrjob.job import MRJob
from datetime import datetime
from mrjob.step import MRStep

class MRJobP1_9(MRJob):
    def mapper(self, key, line):        
        line_read=line.split(',')
        try:
            #incluir en bloque try porque al leer del hdfs 
            # tambien se incluye el header, daría error
            no2=int(line_read[0])
            fecha=line_read[1]
            fecha1=datetime.strptime(fecha, '%d/%m/%YH%H') 
            dia_semana=fecha1.weekday() #Return the day of the week as an integer, where Monday is 0 and Sunday is 6            
            estacion=line_read[2]
            yield estacion, (dia_semana, no2)                        
        except:
            pass
    
    def reducer(self, estacion, tupla):
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
    MRJobP1_9.run()



