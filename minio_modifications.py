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
# from multiprocessing.pool import ThreadPool



warnings.filterwarnings("ignore")


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
    
    # def del_bucket(self, bucket_name):
    #     with ThreadPool(10) as pool:
    #         _ = pool.map(lambda x: self.del_obj_and_bucket(str(bucket_name),x), self.minioClient.list_objects(str(bucket_name)) )
    #     # self.del_obj( self.minioClient.list_objects(str(bucket_name)))
    #     self.minioClient.remove_bucket(str(bucket_name))

    def del_obj_from_all_buckets(self,object_name):
        bucket_lst = [str(bucket) for bucket in self.minioClient.list_buckets()]
        if (len(bucket_lst)>10):
            print(bucket_lst)
            
        for bucket_name in bucket_lst:
            self.minioClient.remove_object(bucket_name,object_name)
                
    def count_obj(self, bucket_name) -> int:
        count = self.minioClient.list_objects(bucket_name , recursive=True)
        return len(list(count))

    def del_obj_and_bucket(self ,bucket_name ):
        lis_of_objects = self.minioClient.list_objects(bucket_name, recursive=True)
       
        errors = self.minioClient.remove_objects(bucket_name ,[DeleteObject(str(obj.object_name)) for obj in lis_of_objects])
        for error in errors:
            print("error occurred when deleting object", error)
        self.minioClient.remove_bucket(bucket_name)
            
    def change_time(self, bucket_name):
        next_date = (datetime.now() + timedelta(days=3)).date()
        next_date = next_date.isoformat()
        
        subprocess.call(shlex.split("timedatectl set-ntp false")) 
        subprocess.call(shlex.split("sudo date -s '%s'" % next_date))
        subprocess.call(shlex.split("sudo hwclock -w"))
        
        # st_time = time.time()
        # while True:
        #     time.sleep(60*5)
        #     obj_count = self.count_obj(bucket_name)
        #     print(obj_count)
        #     if obj_count <= 0:
        #         try:
        #             self.minioClient.remove_bucket(bucket_name)
        #             print("Bucket Deleted successfully")
        #         except:
        #             print("Bucket is empty but can't be deleted")
        #         break
        # print(f'Total time {time.time() - st_time} seconds')
                
mc = MinioBucket()

# date_now = str(datetime.utcnow() + timedelta(seconds=1))
# formatted_date = f'{date_now[0]}T{date_now[1]}Z'

mc.drop_bucket('bucket0')
# result = mc.minioClient.get_bucket_lifecycle('bucket0')


def upload_image(bucket_name,min_client,count):
        for j in range(783_999,count): 
            print(j)
            min_client.fput_object(bucket_name, f'image{j}.jpg', 'sample.jpg')
            if j % 1000 == 0 and j != 0:
                print(f'{j} images uploaded!')
        print(f'BUCKET NAME: {bucket_name} filled with 1000 images')

def make_buckets(min_client):
    for i in range(1):
        try:
            # min_client.make_bucket(f'bucket{i}')
            print('yes')
            upload_image(f'bucket{i}',min_client, 1_000_000)
        except:
            pass

            
            
# make_buckets(mc.minioClient)
    
