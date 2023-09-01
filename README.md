# sftp_connection

import paramiko
import boto3

# SFTP server details
sftp_host = ''
sftp_port = 22
sftp_username = ''
sftp_password = ''
remote_file_path = ''
archive_folder=''

# Amazon S3 details
s3_bucket_name = ''
s3_object_key = ''

try:
    # Establish SFTP connection
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print(sftp.listdir())
    # Upload the file from SFTP to S3 directly
    s3 = boto3.client(
        's3',
        region_name='',
        aws_access_key_id='',
        aws_secret_access_key=''
    )
    print("Successfully connectted to sFTP!")
except Exception as error:
    print(f"Job is failed due to: {error}")

try:
    with sftp.file(remote_file_path, 'r') as sftp_file:
        s3.upload_fileobj(sftp_file, s3_bucket_name, s3_object_key)
        print(f"Successfully getting file from {remote_file_path}, writed to {s3_bucket_name}")
except Exception as error_2:
    print(f"Failed!: {error_2}")

try:
    archive_path = archive_folder + os.path.basename(remote_file_path)
    sftp.rename(remote_file_path, archive_path)
    print(f"File removed from the {remote_file_path}, writed to {archive_path}")
    sftp.close()
    transport.close()
except Exception as err:
    print(f"Failed {err}")
    sftp.close()
    transport.close()
