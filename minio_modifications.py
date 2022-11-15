from minio import Minio 
from minio.commonconfig import ENABLED, Filter
from datetime import datetime, timedelta
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule
from minio.deleteobjects import DeleteObject
import urllib3
import warnings
import subprocess
import shlex
import time

import logging
logging.basicConfig(filename=r'bucket_delete_logs.log', format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T",  level=logging.INFO)

warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool







def main():
    MINIO_URL = 'storage-fiq-5386add9-c708-4ebe.fiq-dev.com'
    ACCESS_KEY='fortressiq'
    SECRET_KEY='mn2smDXjJW9cWbrpG5'
    class MinioBucket:
        def __init__(self ) -> None:
            # self.httpClient = urllib3.PoolManager(timeout=urllib3.Timeout.DEFAULT_TIMEOUT,cert_reqs='CERT_NONE')
            self.minioClient = Minio('localhost:9000',
                                    'ROOTNAME',
                                    'CHANGEME123',secure=False) 
                                    # secure=True, http_client=self.httpClient)

        # def delete_everything(self):
        #     for bucket_name in self.minioClient.list_buckets():
        #         self.del_obj(str(bucket_name),self.minioClient.list_objects(str(bucket_name)))
        #         self.minioClient.remove_bucket(str(bucket_name))
        #     print('everything is deleted')

        def drop_bucket(self,bucket_name):

            config = LifecycleConfig(
                [
                    Rule(ENABLED,
                        rule_id='drop_bucket',
                        rule_filter=Filter(prefix=''),
                        expiration=Expiration(days=1)
                    )
                ]
            )                
            self.minioClient.set_bucket_lifecycle(bucket_name, config)
            self.change_time(bucket_name)
            self.start_cronjob()
        
        def del_bucket_threadpool(self, bucket_name):
            with ThreadPool(10) as pool:
                _ = pool.map(lambda x: self.del_obj_and_bucket(str(bucket_name),x), self.minioClient.list_objects(str(bucket_name)) )
            # self.del_obj( self.minioClient.list_objects(str(bucket_name)))
            self.minioClient.remove_bucket(str(bucket_name))

        def del_obj_from_all_buckets(self,object_name):
            bucket_lst = [str(bucket) for bucket in self.minioClient.list_buckets()]
                
            for bucket_name in bucket_lst:
                self.minioClient.remove_object(bucket_name,object_name)
                    
        def count_obj(self, bucket_name) -> int:
            count = self.minioClient.list_objects(bucket_name , recursive=True)
            return len(list(count))


        def del_obj_and_bucket(self, bucket_name, obj):
            '''uses remove_objects method to remove every object.'''
            # logging.info('Deleting bucket using inbuilt method remove_objects.')
            # obj = self.minioClient.list_objects(bucket_name, recursive=True)
            self.minioClient.remove_object(bucket_name ,obj.object_name)
       
            # self.minioClient.remove_bucket(bucket_name)
                
        def del_bucket_script(self, bucket_name):
            # print(shlex.split(rf'~/mc rm --recursive --force local_storage/{bucket_name}'))
            
            subprocess.call(shlex.split(rf'/home/kumarshantanu/mc rm --recursive --force local_storage/{bucket_name}'))
            subprocess.call(shlex.split(rf'/home/kumarshantanu/mc rb --force local_storage/{bucket_name}'))
                
        def change_time(self, bucket_name):
            next_date = (datetime.now() + timedelta(days=2)).date()
            next_date = next_date.isoformat()
            
            subprocess.call(shlex.split("timedatectl set-ntp false")) 
            subprocess.call(shlex.split("sudo date -s '%s'" % next_date))
            subprocess.call(shlex.split("sudo hwclock -w"))
            
        # def start_cronjob(self):
        #     with open('/etc/crontab', 'r+') as fh:
        #         lines = fh.readlines()
        #         lines.append(r'* * * * * root /home/kumarshantanu/Documents/Minio_bucket_task/venv_minio/bin/python3.8 /home/kumarshantanu/Documents/Minio_bucket_delete/cron.py')
        #         fh.writelines(lines)
        #     subprocess.call(shlex.split('sudo service cron reload'))
                    
    mc = MinioBucket()

    # subprocess.call(shlex.split(rf'/home/kumarshantanu/mc rm --recursive --force local_storage/{bucket_name}'))

    # mc.drop_bucket('bucket0')
    def upload_image(bucket_name,min_client,count):
            for j in range(count): 
                min_client.fput_object(bucket_name, f'image{j}.jpg', 'sample.jpg')
                if j % 1000 == 0 and j != 0:
                    print(f'{j} images uploaded!')
            print(f'BUCKET NAME: {bucket_name} filled with 1000 images')

    def make_bucket(min_client, bucket_name):
        # for i in range(3):
            try:
                # min_client.make_bucket(f'bucket{i}')
                min_client.make_bucket(bucket_name)
                upload_image(bucket_name, min_client, 30_000)
            except Exception as e:
                print(e)
                
    # make_bucket(mc.minioClient,'bucket0')
    def benchmark():
        total_time = 0
        for i in range(10):
            # recording time for bucket deletion
            st_time = time.time()
            # mc.del_obj_and_bucket('bucket0')
            mc.del_bucket_threadpool('bucket0')
            total_time += time.time() - st_time
            make_bucket(mc.minioClient, 'bucket0')
            # creating and refilling the bucket
            
        logging.info(f'Using Threadpool: Bucket deleted successfully in {round(total_time/10,3)} seconds')
    benchmark()
if __name__ == '__main__':
    main()
        
