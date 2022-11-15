#!/usr/bin/python3.8
import logging
logging.basicConfig(filename=r'/home/kumarshantanu/Desktop/cron_logs.log', format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T",  level=logging.INFO)
logging.info('Cronjob started')

def main():
    # logging.info('Cronjob started')
    
    try:
        from minio import Minio
    except:
        logging.warning('Cannot find Minio')
    try:
        mc = Minio('localhost:9000',
                        'ROOTNAME',
                        'CHANGEME123',secure=False) 
        logging.info('Successfully Initialized Minio')
    except:
        logging.warning('Unable to instantiate Minio client')
        

    bucket_name = 'bucketx'
    try:
        
        logging.info(f'Bucket : {bucket_name} Trying to delete the bucket')
        mc.remove_bucket(bucket_name)
        logging.info(f'Bucket : {bucket_name} Successfully deleted')
        
    except Exception as e :
        
        logging.warning(e)


if __name__ == '__main__':
    main()    