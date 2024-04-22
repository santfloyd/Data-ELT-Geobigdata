from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MRJobP1(MRJob):
    def mapper(self, key, line):
        line_read=line.split(';') #try separador es coma, con los otros es ;
        try:
            fecha=line_read[0].replace('+02:00','+0200')
            fecha1=datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%S%z')
            dia=fecha1.day
            hour=fecha1.hour
            lectura=int(line_read[3])
            if lectura==-1 or lectura>5000:
                lectura=None
            yield dia, (hour, lectura)
        except:
            pass

    def reducer(self, dia, tupla):
        promedios_hora = {}
        
        for x, y in tupla:
            try:
                #la siguiente devuelve un valor para la key x
                #si no la encuentra le da un valor por defecto
                #es util para evitar el Keyerror cuando no se encuentra la key
                #total, numElements = promedios_hora.get(x, (0, 0))
                #equivalente:
                if x in promedios_hora:
                    total, numElements = promedios_hora[x]
                else:
                    total, numElements = (0, 0)
                total += y
                numElements += 1
                #actualiza los valores calculados
                promedios_hora[x] = (total, numElements)
                
            except:
                pass
        
        for hora, (total, numElements) in promedios_hora.items():
            try:
                yield dia, (hora, int(total/ (numElements)))
            except:
                pass
    #trabaja con la salida del reducer anterior
    def mapper2(self, dia, tupla):
        hora=tupla[0]
        promedio=tupla[1]
        yield dia, (promedio, hora)

    def reducer2(self, dia, tupla):

        yield dia, max(tupla)
    #incluye una funcion step que usa el atributo
    #MRStep de MRJobs para definir los steps
    def steps(self):
        return [MRStep(mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(mapper=self.mapper2,
                        reducer=self.reducer2)]

if __name__=='__main__':
    MRJobP1.run()