from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MRJobP1_5(MRJob):
    def mapper(self, key, line):
        line_read=line.split(';')
        try:
            nombre=str(line_read[1])
            lectura=int(line_read[3])
            fecha=line_read[0].replace('+02:00','+0200')
            fecha1=datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%S%z') 
            year = fecha1.year 
            month = fecha1.month
            dia=fecha1.day
            hour=fecha1.hour
            minute=fecha1.minute
            second = fecha1.second
            if lectura==-1 or lectura>5000:
                lectura=None
                
            yield nombre, (dia, lectura)
        except:
            pass

    def reducer(self, nombre, tupla):
        promedios_dia = {}
        
        for x, y in tupla:
            try:
                #la siguiente devuelve un valor para la key x
                #si no la encuentra le da un valor por defecto
                #es util para evitar el Keyerror cuando no se encuentra la key
                #total, numElements = promedios_hora.get(x, (0, 0))
                #equivalente:
                if x in promedios_dia:
                    total, numElements = promedios_dia[x]
                else:
                    total, numElements = (0, 0)
                total += y
                numElements += 1
                #actualiza los valores calculados
                promedios_dia[x] = (total, numElements)
                
            except:
                pass
        
        for dia, (total, numElements) in promedios_dia.items():
            try:
                yield dia, (nombre, float(total/ (numElements)))
            except:
                pass
        #trabaja con la salida del reducer anterior
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
    MRJobP1_5.run()
