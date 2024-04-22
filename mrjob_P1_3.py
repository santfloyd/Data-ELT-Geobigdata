from mrjob.job import MRJob
from datetime import datetime

class MRJobP1_3(MRJob):
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
                
            yield nombre, (lectura, (year, month, dia, hour, minute, second))
        except:
            pass

    def reducer(self, nombre, tupla):
        #variables para almacenar los valores si se cumplen condiciones
        max_value = None
        min_value = None
        tupla_max = None
        tupla_min = None
  
        for x, y in tupla:
            try:
                if max_value is None or x > max_value:
                   
                   max_value = x
                   tupla_max = (max_value, y)
                   
                
                if min_value is None or x < min_value:
                    min_value = x
                    tupla_min = (min_value, y)      
                   
                
                
                
                
            except:
                pass
        
        yield nombre, (tupla_max, tupla_min)

if __name__=='__main__':
    MRJobP1_3.run()