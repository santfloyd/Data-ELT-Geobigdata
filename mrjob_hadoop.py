from mrjob.job import MRJob

class MRJobMongo(MRJob):
    def mapper(self, key, line):
        line_read=line.split(';')
        try:
            nombre=str(line_read[1])
            lectura=int(line_read[3])
            if lectura==-1 or lectura>5000:
                lectura=None
            yield nombre, lectura
        except:
            pass

    def reducer(self, nombre, lectura):
        total=0
        numElements=0
        for x in lectura:
            try:
                total+=x
                numElements +=1
            except:
                pass
        try:
            yield int(total/ (numElements)), nombre
        except:
            pass
if __name__=='__main__':
    MRJobMongo.run()
