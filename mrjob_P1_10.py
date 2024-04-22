import boto3
import time

s3 = boto3.resource("s3")

#bucket_name = ("santiagobigdata"+str(time.time()))
bucket_name = ("santiagobigdata505")
print('Creating bucket ', bucket_name)

s3.create_bucket(Bucket = bucket_name,
                  ObjectOwnership = 'ObjectWriter',
                CreateBucketConfiguration={'LocationConstraint': 'sa-east-1'},
                   ObjectLockEnabledForBucket=False)
client = boto3.client('s3')

client.put_public_access_block(Bucket=bucket_name, 
                               PublicAccessBlockConfiguration={'BlockPublicAcls':False,
                                                               'IgnorePublicAcls':False,
                                                               'BlockPublicPolicy':False,
                                                               'RestrictPublicBuckets':False})
client.put_bucket_acl(ACL='public-read-write', Bucket = bucket_name)

bucket = s3.Bucket(bucket_name)

for bucket_list in s3.buckets.all():
    print(bucket_list.name)

filename = '/home/santiago/Desktop/Bigdata_scripts/hadoop/estaciones_concat.csv'

bucket.upload_file(filename, 'bigdata/estaciones.csv', ExtraArgs={'ACL':'public-read'})
#bucket.download_file('bigdata/estaciones.csv', '/home/santiago/Desktop/Bigdata_scripts/hadoop/estaciones_concat.csv')

for obj in bucket.objects.all():
    print('nombre del bucket: ', obj.bucket_name)
    print('fichero: ', obj.key)


#fichero_borrar='bigdata/estaciones.csv'
#bucket.Object(fichero_borrar).delete()
#bucket.objects.all().delete()
#bucket.delete()

