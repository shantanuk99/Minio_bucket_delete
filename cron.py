import logging
# logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)

logging.basicConfig(filename=r'/home/kumarshantanu/Desktop/example.log', format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T",  level=logging.DEBUG)


from minio import Minio
try:
    mc = Minio('localhost:9000',
                    'ROOTNAME',
                    'CHANGEME123',secure=False) 
    logging.info('Successfully Initialized Minio')
except:
    logging.warning('Unable to instantiate Minio client')
    
def count_obj(mc, bucket_name) -> int:

        try:
            logging.info(f'Bucket name: {bucket_name} Trying to delete the bucket')
            mc.remove_bucket(bucket_name)
        except Exception as e :
            logging.warning('Bucket is not empty')

bucket_name = 'bucket0'
count_obj(mc, bucket_name)