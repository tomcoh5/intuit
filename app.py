import boto3

from PIL import Image
import datetime
import os

import logging
import mysql.connector
import datetime
import jinja2

def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)


def lecho(line):
    print(line)
    logging.info(line)


def delete_s3_items(items_for_deletion, bucket):
    global no_error_in_runtime
    for item in items_for_deletion:
        try:
            s3.Object(bucket, item).delete()
            lecho(f"{item} from {bucket} has been deleted")
        except Exception as e:
            no_error_in_runtime = True
            lecho(f"Error {e}, cant delete {item} from {bucket}")



def divide_bucket(bucket_name):
    global no_error_in_runtime
    my_bucket = s3.Bucket(bucket_name)
    list_of_items_delete = []
    list_of_items_for_resizing = []
    for file in my_bucket.objects.all():
        get_tags_response = s3_client.get_object_tagging(
            Bucket=bucket_name,
            Key=file.key,
        )
        if "TagSet" in get_tags_response:
            for some_dict in get_tags_response["TagSet"]:
                resized_key = False
                resized_value = False
                for key , value  in some_dict.items():
                    if key == "Key" and value == "resized":
                        resized_key = True
                    if key == "Value" and value == "true":
                        resized_value = True
                if resized_key is True and resized_value is True:
                    list_of_items_delete.append(file.key)
                else:
                    list_of_items_for_resizing.append(file.key)
        else:
            list_of_items_for_resizing.append(file.key)

        return list_of_items_delete, list_of_items_for_resizing


def download_images_from_s3_bucket(list_of_images, bucket_name):
    global no_error_in_runtime
    dict_of_verified_images = {}
    for item in list_of_images:
        if not str(item).endswith("jpg") or not str(item).endswith("png"):
            continue
        file_name = item.split("/")[-1]
        try:
            s3.Bucket(bucket_name).download_file(item, file_name)
            lecho(f"Download of file {file_name} succeeded")
            file_size = os.path.getsize(file_name)
            readable_file_size = humanbytes(file_size)
            dict_of_verified_images[file_name] = readable_file_size
        except Exception as e:
            no_error_in_runtime = True
            lecho(f"Download of file {file_name} failed ....")
            lecho(f"Error is {e}")
    return dict_of_verified_images

def from_image_to_thumbnail(dict_of_images):
    global no_error_in_runtime
    dict_of_new_images = {}
    for image,image_size in dict_of_images.items():

        image_name,prefix = image.split(".")
        thumbnail_image_name = f"{image_name}_thumbnail.{prefix}"
        try:
            image = Image.open(image)
            image.thumbnail((100,90))
            image.save(thumbnail_image_name)
            lecho(f"Created thumnbail from {image_name} to {thumbnail_image_name}")
            thumbnail_file_size = os.path.getsize(thumbnail_image_name)
            readable_thumbnail_file_size = humanbytes(thumbnail_file_size)
            dict_of_new_images[thumbnail_image_name] = [image_name,image_size,readable_thumbnail_file_size]
        except Exception as e:
            no_error_in_runtime = True
            lecho(f"Error {e}")

    return dict_of_new_images

def upload_log(log_name,bucket_name):
    global no_error_in_runtime
    lecho(f"uploading log {log_name} to {bucket_name}")
    try:
        s3.Bucket(bucket_name).upload_file(log_name, log_name)
        lecho(f"Log {log_name} uploaded to {bucket_name} ")
    except Exception as e :
        lecho(f"Error {e}, cant upload log file ")


def upload_objects_to_s3(dict_of_images, bucket_name):


    global no_error_in_runtime

    dict_of_uploaded_mages = {}
    for thumbnail, list_of_props in dict_of_images.items():
        try:
            s3.Bucket(bucket_name).upload_file(thumbnail, thumbnail)
            lecho(f"{thumbnail} has been uploaded to bucket {bucket_name}")
            now = datetime.datetime.now()
            date_formatting = now.strftime("%Y-%m-%d %H:%M:%S")
            list_of_props.append(date_formatting)
            dict_of_uploaded_mages[thumbnail] = list_of_props
        except Exception as e:
            no_error_in_runtime = True
            lecho(f"Error {e}, couldn't upload {thumbnail} to {bucket_name}")
    return dict_of_uploaded_mages


def tag_s3_objects(dict_of_images_to_tag,bucket):
    global no_error_in_runtime
    list_of_images_that_cant_be_tagged = []
    for thumbnail,real_image_name  in dict_of_images_to_tag.items():
        try:
            s3_client.put_object_tagging(
                Bucket=bucket,
                Key=real_image_name,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'resized',
                            'Value': 'true'
                        },
                    ]
                }
            )

        except Exception as e:
            no_error_in_runtime = True
            list_of_images_that_cant_be_tagged.append(real_image_name)
            lecho(f"Error {e}, couldn't tag {real_image_name} in {bucket}")

    lecho(f"list of images that can't be tagged {list_of_images_that_cant_be_tagged}")



def send_mail(destination_mail, bucket_of_log,log_name):
    email_client = boto3.client('ses')
    try:
        email_client.send_email(
            Destination={
                'ToAddresses': [
                    destination_mail
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': f'You have an error in your lambda function. Please check the log file located in {bucket_of_log} , log name is {log_name}',
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': 'ERROR',
                },
            },
            Source='lambda@my-aws-lambda.com',
        )
    except Exception as e:
        lecho("Error Couldn't send mail")
        lecho(e)


def insert_to_rds(host,user,password,database,table,dict_of_images):
    try: 
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        lecho(f"connected to {host} in {database}")
        mycursor = mydb.cursor()
    
        list_of_tuples = []
        for thumbnail , list_of_props in dict_of_images.items():
    
            new_tuple = (thumbnail, list_of_props[0], list_of_props[1], list_of_props[2], list_of_props[3])
            list_of_tuples.append(new_tuple)
    
        sql = f"INSERT INTO {table} (thumbnail_image_name, image_name, image_size, readable_thumbnail_file_size, file_uploaded_date) VALUES {list_of_tuples}"
    
        mycursor.execute(sql)
    
        mydb.commit()
        lecho("record inserted")
        
    except Exception as e:
        lecho(f" failed to insert to the database {e}")


def create_static_html_page(title="static website for images",bucket_name=""):
    s3.Bucket(bucket_name).download_file("template.html", "template.html")
    outputfile = 'static.html'
    my_bucket = s3.Bucket(bucket_name)
    list_of_lists = []
    for file in my_bucket.objects.all():
        new_list = [file.key, f"this is {file.key}"]
        list_of_lists.append(new_list)
    subs = jinja2.Environment(loader=jinja2.FileSystemLoader('./')).get_template('template.html').render(title=title, mydata=data)
    with open(outputfile, 'w') as f:
        f.write(subs)
    s3.Bucket(bucket_name).upload_file(outputfile, outputfile)

time_str_for_log = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
log_file_name = f"{time_str_for_log}.log"
logging.basicConfig(
    filename= log_file_name,
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

no_error_in_runtime = False
bucket_of_images = os.getenv('image_bucket')
output_bucket = os.getenv('thumbnail_bucket')
host = os.getenv("host")
username = os.getenv("username")
password = os.getenv("password")
table = os.getenv("table")
db_name = os.getenv("database")

s3 = boto3.resource('s3')

s3_client = boto3.client("s3")
items_to_delete, items_for_resizing = divide_bucket(bucket_of_images)
delete_s3_items(items_to_delete,bucket_of_images)
verified_images = download_images_from_s3_bucket(items_for_resizing,bucket_of_images)
new_images = from_image_to_thumbnail(verified_images)
uploaded_images = upload_objects_to_s3(new_images, output_bucket)

tag_s3_objects(uploaded_images,bucket_of_images)

upload_log(log_name=log_file_name, bucket_name=bucket_of_images)
if no_error_in_runtime is True:
    lecho("There were errors during run time please check the log file")
    send_mail(destination_mail="tomcoh5@gmail.com",bucket_of_log=bucket_of_images,log_name=log_file_name)
else:
    insert_to_rds(host=host, user=username, password=password, table=table, database=db_name,dict_of_images=uploaded_images)
    lecho("No error durning runtime")
create_static_html_page()
