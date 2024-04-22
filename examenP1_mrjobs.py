from mrjob.job import MRJob
from datetime import datetime

class MRJobP1(MRJob):
    def mapper(self, key, line):
        line_read=line.split(';') #puede ser separador es ";" o ","
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
        


if __name__=='__main__':
    MRJobP1.run()