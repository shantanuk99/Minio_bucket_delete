from minio import Minio

mc = Minio('localhost:9000',
                    'ROOTNAME',
                    'CHANGEME123',secure=False) 

def count_obj(mc, bucket_name) -> int:
    count = 0
    for obj in mc.list_objects(bucket_name , recursive=True):
        count += 1
        if count != 0:
            print(f"There are still objects in bucket: {bucket_name}")        
            break
        
    if count == 0:
        print(f'Bucket name: {bucket_name} is empty trying to delete the bucket')
        try:
            mc.remove_bucket(bucket_name)
        except Exception as e :
            print(e)

bucket_name = 'bucket0'
count_obj(mc, bucket_name)