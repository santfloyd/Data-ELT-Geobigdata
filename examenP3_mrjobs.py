from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MRJobP1(MRJob):
    def mapper(self, key, line):
        line_read=line.split('\t') #try separador es coma, con los otros es ;
        try:
            
            temperatura = int(line_read[3])
            presion = int(line_read[5])
            veloci_viento_max = float(line_read[9])

            yield veloci_viento_max, (temperatura, presion)
        except:
            pass

    def reducer(self, velo, tupla):

        total_x = 0
        total_y = 0
        contador = 0
        try:
            for x, y in tupla:
                total_x+=x
                total_y+=y
                contador += 1

                promedio_x = total_x/contador
                promedio_y = total_y/contador
            yield velo, (promedio_x, promedio_y)
        except:
            pass

            

        
if __name__=='__main__':
    MRJobP1.run()