import boto3
import time
import pandas as pd
import subprocess
from subprocess import Popen, PIPE
from datetime import datetime
from sqlalchemy import create_engine
import sys
from contextlib import closing
import os
import shutil
import logging
import zipfile
import shlex
import re

mysqlbackup_pswd = 'password'

email_pswd = 'password'
user = "source@gmail.com"
recipient = ["target@gmail.com"]


def send_email(subject, body):
    import smtplib

    FROM = user
    TO = recipient if isinstance(recipient, list) else [recipient]
    SUBJECT = subject
    TEXT = body

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s""" % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(user, email_pswd)
        server.sendmail(FROM, TO, message)
        server.close()
        print('successfully sent the mail')

    except Exception as e:
        print("failed to send mail")


class VolumeHandler(object):
    """
    This Class uses Python API to connect AWS to create, attach, mount, detach, delete Volume to an instance.
    This is only needed when the created backup image file size is too big and requires a separate disk to handle without
    compromising DB performance
    """

    def __init__(self):
        self.session = boto3.Session(profile_name='admin')
        self.client = self.session.client("ec2")
        self.vol_id = ''

    def create_volume(self, size):
        """
        Function to Create a Volume in AWS based on the given size
        :param size: int, disk size in GB
        :return: None
        """
        vol = self.client.create_volume(Size=size, AvailabilityZone='ap-southeast-2a', VolumeType='gp2')
        print("##### CREATE A VOLUME#####")
        # Check if the Volume is ready and available
        curr_vol = self.client.describe_volumes(VolumeIds=[vol["VolumeId"]])["Volumes"][0]

        while curr_vol["State"] == 'creating':
            curr_vol = self.client.describe_volumes(VolumeIds=[vol["VolumeId"]])["Volumes"][0]
            print("Current Volume Status: ", curr_vol["State"])
            time.sleep(1)

        print("##### VOLUME STATUS #####")
        print(curr_vol["State"])
        self.vol_id = vol["VolumeId"]

    def add_name_tag(self, vol_id, tag_name):
        """
        Function to add a NAME tag to a Volume
        :param vol_id: str, AWS Volume ID
        :param tag_name: str, tagged name
        :return: None
        """
        self.client.create_tags(Resources=[vol_id], Tags=[{"Key": "Name", "Value": tag_name}])

    def attach_volume(self, ins_id, vol_id):
        """
        Function to attach a Volume to an EC2 Instance
        :param ins_id: str, AWS EC2 instance ID
        :param vol_id: str, AWS Volume ID
        :return: None
        """
        # Attach Volume to an Instance
        response = self.client.attach_volume(Device="/dev/sdo", InstanceId=ins_id, VolumeId=vol_id)
        print("##### ATTACH A VOLUME #####")

        # Check attach status
        curr_vol = self.client.describe_volumes(VolumeIds=[vol_id])["Volumes"][0]["Attachments"][0]
        print("#################")
        while curr_vol["State"] == 'attaching':
            curr_vol = self.client.describe_volumes(VolumeIds=[vol_id])["Volumes"][0]["Attachments"][0]
            print("Attaching Volume to Instance: ", curr_vol["State"])
            time.sleep(1)

    def mount_volume(self, size):
        """
        Function to mount the created Volume to the EC2 instance, validate the Volume using the disk size.
        The function bedefault mount the Volume to /temp_restore directory and create a path /temp_restore/temp_backup
        :param size: int, Volume size in GB
        :return: None
        """
        cmd = "lsblk"
        p = Popen(shlex.split(cmd), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        print(output.decode('utf-8'))

        cmd = "sudo nvme list"
        p = Popen(shlex.split(cmd), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        print(output.decode('utf-8'))

        cmd = "sudo nvme list | grep 'Amazon Elastic Block Store' | tail -n1 | awk '{ print $1 }'"
        device = subprocess.getoutput(cmd)
        print(device)
        cmd = "sudo nvme list | grep 'Amazon Elastic Block Store' | tail -n1 | awk '{ print $11 }'"
        storage = subprocess.getoutput(cmd)
        print(storage)
        if storage == str(size):
            cmd = "sudo mkfs.xfs " + device
            subprocess.call(cmd, shell=True)
            cmd = "sudo mount " + device + " /temp_restore"
            subprocess.call(cmd, shell=True)

            cmd = "df -h"
            p = Popen(shlex.split(cmd), stdin=PIPE, stdout=PIPE, stderr=PIPE)
            output, err = p.communicate()
            print(output.decode('utf-8'))

            cmd = "sudo chown mysql:mysql /temp_restore"
            subprocess.call(cmd, shell=True)
            cmd = "mkdir /temp_restore/temp_backup"
            subprocess.call(cmd, shell=True)

    def detach_volume(self, vol_id):
        """
        Function to detach a Volume from an EC2 instance
        :param vol_id: str, AWS Volume ID
        :return: None
        """
        # Detach Volume
        cmd = "sudo umount /temp_restore"
        subprocess.call(cmd, shell=True)
        response = self.client.detach_volume(VolumeId=vol_id)
        print("##### DETACH VOLUME #####")

        # Check detach status
        curr_vol = self.client.describe_volumes(VolumeIds=[vol_id])["Volumes"][0]["Attachments"][0]
        while curr_vol["State"] == 'detaching':
            curr_vol = self.client.describe_volumes(VolumeIds=[vol_id])["Volumes"][0]["Attachments"]
            if curr_vol:
                curr_vol = curr_vol[0]
                print("Attaching Volume to Instance: ", curr_vol["State"])
                time.sleep(1)
            else:
                break

    def delete_volume(self, vol_id):
        """
        Function to delete a AWS Volume based on the Volume ID. By default, it is assumed the Volume was mounted to
        /temp_restore, hence it umounts it after deletion
        :param vol_id: str, AWS Volume ID
        :return: None
        """
        os.chdir("/home/mysql/")
        cmd = "sudo umount /temp_restore"
        subprocess.call(cmd, shell=True)
        print("##### DELETE VOLUME #####")
        cmd = "sudo umount /temp_restore"
        subprocess.call(cmd, shell=True)
        response = self.client.delete_volume(VolumeId=vol_id)


class MysqlBackup(object):
    """
    MySQL DB Weekly backup class, this class is used to perform weekly backup for thoes schemas defined in the meta data
    table. It also creates backups for all the table structures, stored procedures, functions for all the schemas
    """

    def __init__(self, logdir, logfile, logger, suffix):
        # self.engine = engine
        self.temp = logdir + "mysqlbackup"
        self.logfile = logdir + logfile
        self.s3folder = "s3://yours3repo"
        self.suffix = suffix
        self.logger = logger
        self.errs_schema = list()

    def db_connect(self):
        """
        Initialize MySQL DB connection
        :return: connection engine object
        """
        hostname = 'mysql_db'
        username = 'root'
        database = 'chinook'
        password = 'password'
        args = "mysql+pymysql://" + username + ":" + password + "@" + hostname + ":3306/" + database
        engine = create_engine(args)
        return engine

    def _get_schemas(self, field):
        """
        Select all the distinct schema from MySQL DB except system schemas
        :return: list, all the valid schemas
        """
        sql = ""
        self.logger.info(sql)
        with closing(self.db_connect().connect()) as connection:
            schemas = pd.read_sql_query(sql, connection)
            schemas = schemas['table_schema'].values.tolist()
        return schemas

    def _get_tables(self, field):
        sql = "SELECT DISTINCT table_name as table_name from information_schema.Tables where engine = 'InnoDB' and table_schema = '" + field + "';"
        self.logger.info(sql)
        with closing(self.db_connect().connect()) as connection:
            tables = pd.read_sql_query(sql, connection)
            tables = tables['table_name'].values.tolist()
        return tables

    def _backup(self, schema_name, table_name, work_dir):
        cmd = "/usr/bin/mysqlbackup --user=root --password=" + mysqlbackup_pswd + " --host=localhost --backup-dir=" + work_dir + " --backup-image=" + schema_name + "." + table_name + ".mbi --include-tables='^" + schema_name + "\." + table_name + "$' --use-tts=with-full-locking backup-to-image"
        p = Popen(shlex.split(cmd), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        err = err.decode("utf-8")
        regex = re.compile(r"mysqlbackup completed OK", re.IGNORECASE)
        if bool(regex.search(err[-40:])):
            pass
        else:
            self.errs_schema.append(schema_name)
        self.logger.info(cmd)
        # zip the entire directory
        cmd = "Zipping mysqlbackup output file " + schema_name + "." + table_name + ".sql ..."
        self.logger.info(cmd)
        cwd = os.getcwd()
        os.chdir(work_dir)
        cmd = "zip -r " + schema_name + "." + table_name + ".zip ."
        subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.chdir(cwd)
        self.logger.info(cmd)

    def _file_upload(self, schema_name, table_name, work_dir):
        cmd = "/usr/local/bin/aws2 s3 cp " + work_dir + "/" + schema_name + "." + table_name + ".zip " + self.s3folder + "/Schema/" + self.suffix + "/" + schema_name + "/" + schema_name + "." + table_name + ".zip"
        subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.logger.info(cmd)

    def log_upload(self, logfile):
        cmd = "/usr/local/bin/aws2 s3 cp " + logfile + " " + self.s3folder + "/Log/" + os.path.basename(logfile)
        subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.logger.info(cmd)

    def _backup_mysqldump(self, schemas):
        """
        Function to backup table structures, stored procedures, functions for the given schemas
        :param schemas: list, schema name list
        :return: None
        """
        for schema in schemas:
            work_dir = self.temp + "/" + self.suffix
            if not os.path.exists(work_dir):
                os.mkdir(work_dir)

            # within each schema, retrieve all the tables not starting with etl,tmp or temp
            tables = self._get_tables(schema)
            tables = list(filter(self._filter_tables, tables))
            # backup table structures, stored procedures, functions and table data, with comment, no database creation
            if not os.path.exists(work_dir + "/" + schema):
                os.mkdir(work_dir + "/" + schema)
            for table in tables:
                cmd = "mysqldump -h mysql_db -u root -p" + mysqlbackup_pswd + " --comments -n --single-transaction " + schema + " " + table + " > " + work_dir + "/" + schema + "/" + table + ".sql"
                print(cmd)
                subprocess.call(cmd, shell=True)
                self.logger.info(cmd)

            # zip output files
            # cwd = os.getcwd()
            # os.chdir(work_dir + "/" + schema)
            # with zipfile.ZipFile(schema + '.zip', 'w', zipfile.ZIP_DEFLATED) as zip_obj:
            #     # chdir to mysqldump output directory to avoid zip file created with full path
            #     zip_obj.write(schema + '.sql')
            # # os.remove(schema + ".sql")
            # os.chdir(cwd)
            # Upload to S3
            """
            cmd = "/usr/local/bin/aws2 s3 cp " + work_dir + "/" + schema + ".zip " + self.s3folder + "/StoredProcedure/" + self.suffix + "/" + schema + ".zip"
            subprocess.call(cmd, shell=True)
            self.logger.info(cmd)
            """

    def _filter_tables(self, table):
        """
        A function to filter out temporary tables, temporary table starts with etl or tmp or temp
        :param table: str, table name
        :return: Bool, True or False
        """
        if table.startswith("etl") or table.startswith("tmp") or table.startswith("temp"):
            return False
        else:
            return True

    def main(self):
        # Backup Stored Procedures and Functions
        cmd = "SELECT DISTINCT TABLE_SCHEMA as table_schema FROM information_schema.TABLES"
        with closing(self.db_connect().connect()) as connection:
            schemas = pd.read_sql_query(cmd, connection)
            schemas = schemas['table_schema'].values.tolist()
        try:
            # Remove system default schemas
            schemas.remove('mysql')
            schemas.remove('performance_schema')
            schemas.remove('sys')
            schemas.remove('information_schema')
        except Exception as e:
            print(e)
            print("Error removing system schemas from list")
            sys.exit()
        self.logger.info(cmd)
        print("schemas")
        print(schemas)
        self._backup_mysqldump(schemas)
        print("Finish main run!")

        """
        # Backup Schemas
        schemas = self._get_schemas(None)
        table_count = 0
        for schema in schemas:
            tables = self._get_tables(schema)
            tables = list(filter(self._filter_tables, tables))
            table_count += len(tables)

        for schema in schemas:
            tables = self._get_tables(schema)
            tables = list(filter(self._filter_tables, tables))
            for table in tables:
                work_dir = self.temp + "/" + self.suffix + "/" + schema + "/" + table
                if os.path.exists(self.temp):
                    shutil.rmtree(self.temp)
                os.makedirs(work_dir)
                # Backup
                self._backup(schema, table, work_dir)
                # Upload to S3
                self._file_upload(schema, table, work_dir)
        self.log_upload(self.logfile)
        """


if __name__ == "__main__":

    """
    # AWS RHEL PROD EC2 Instance ID
    ins_id = "i-0000000000000"
    vol_obj = VolumeHandler()
    vol_obj.create_volume(666)
    vol_obj.add_name_tag(vol_obj.vol_id, "TEST_VOLUME")
    vol_obj.attach_volume(ins_id, vol_obj.vol_id)
    vol_obj.mount_volume(715.11)
    """

    now = datetime.now()
    suffix = ""
    logdir = '/'
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    if not os.path.exists(logdir + "mysqlbackup"):
        os.mkdir(logdir + "mysqlbackup")
    logfile = "backup_log_" + suffix + ".txt"

    logging.basicConfig(filename=logdir + logfile,
                        filemode='w',
                        format='%(asctime)s,%(name)s,%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    logger = logging.getLogger("MysqlBackup")
    obj = MysqlBackup(logdir, logfile, logger, suffix)
    obj.main()

    """
    # configure email sending details
    subject = "MySQL backup completed on  " + now + "!"
    tablelist = obj._get_schemas("weekly")

    # construnct the main body
    body = "MySQL schema backup completed on " + now + "!\n" + "Below schemas were backed up : " + "\n\n" + str(
        tablelist) + "\n\n\n\n"
    msg = "logfile location: " + logdir + logfile + "\n"
    body += msg

    if obj.errs_schema:
        msg = "ERROR!!! There schemas were not backed up!!!\n" + ",".join(obj.errs_schema) + "\n"
        body += msg

    msg = "S3 location " + obj.s3folder + "\n"
    body += msg
    msg = "Stored Procedures and Functions were backed up successfully!\n"
    body += msg
    send_email(subject, body)
    vol_obj.detach_volume(vol_obj.vol_id)
    vol_obj.delete_volume(vol_obj.vol_id)
    """
    print("***************************")
