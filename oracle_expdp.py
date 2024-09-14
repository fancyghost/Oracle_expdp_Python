import sys
import os
import logging
from logging.handlers import RotatingFileHandler
import subprocess
import json
from datetime import datetime
#import requests
import  http.client
import time
import hashlib

'''
HOW TO USE:
1.edit config file: expdpconfig.json.and make sure the parameter is OK.
{
"SCHEMAS" : ["USER1","USER2"],
"DIRECTORY":"BACKUP",
"DUMPFILE_PREFIX":"abc",
"LOGFILE_PREFIX":"abc",
"PARALLEL":"1",
"BACKUPDIR":"/backup/xxx",
"OSS_BUCKET":"xxx",
"CONN_USER_PASS":" not necessary example: username/passs "
}

# make sure zstd and ossutil64 is already installed in user system and path is in $PATH 

2.change const FOR python config if needed

3.python3 oracle_expdp.py config=/backup/xxx/xxx.json

4.using crontab example
# 0 3 * * * export PATH=/usr/bin:/usr/local/bin:$PATH && source ~/.bash_profile && cd /home/oracle/scripts && python3 /home/oracle/scripts/oracle_expdp.py config=/home/oracle/scripts/expdpconfig.json 

5.todo list
-[X] alarm 
-[] using expdp parfile
-[] using include | exclude
'''


#const FOR python config
LOGFMT = '[%(asctime)s] [%(levelname)s] %(message)s'
LOGFILE = 'expdp_python.log'
#DO NOT CHANGE!
REQUIRED_KEYS = ['SCHEMAS', 'DIRECTORY', 'DUMPFILE_PREFIX', 'LOGFILE_PREFIX','PARALLEL','BACKUPDIR','OSS_BUCKET']
MAX_LENGTH = 10
MAX_PARALLEL = 4
SURETY_DIR = '/backup'
#keep hours for backup file
CLEAN_MAX_HOURS = 3
LOG_MAX_SIZE_MB = 50
LOG_MAX_BACKUP_COUNT = 2
EXPDP_TIMEOUT_MINUS = 120
COMPRESS_TIMEOUT_MINUS = 30
OSSUTIL_TIMEOUT_MINUS = 30
WORKDIR = "/home/oracle/scripts"
ALARM_URL= "xxxxxx"
ALEMR_API="xxxx"

def get_logger(LOGFILE):
    logging.basicConfig(format=LOGFMT, level=logging.INFO)
    logger = logging.getLogger()
    handler = RotatingFileHandler(filename=LOGFILE, maxBytes=1024*1024*LOG_MAX_SIZE_MB, backupCount=LOG_MAX_BACKUP_COUNT)
    handler.setFormatter(logging.Formatter(LOGFMT))
    logger.addHandler(handler)
    return logger

#init LOOGER
logger = get_logger(LOGFILE)


def clean_backup_dir(config):
    logger.info("Begin clean_backup_dir")
    now = datetime.now()
    for file in os.listdir(config['BACKUPDIR']):
        file_path = os.path.join(config['BACKUPDIR'], file)
        #charge the file_path is a file
        if os.path.isfile(file_path):
            file_last_modify_time = os.path.getmtime(file_path)
            #Use NOW and file_last_modify_time to determine if a file was last modified more than 3 hours ago.
            file_nochange_seconds = (now - datetime.fromtimestamp(file_last_modify_time)).total_seconds()
            if file_nochange_seconds > 3600 * CLEAN_MAX_HOURS:
                os.remove(file_path)
                logger.info(f"before backup, delete file {file_path}, because the file last change time > {CLEAN_MAX_HOURS} hours ")
            else:
                logger.info(f"before backup, keep file {file_path},  because the file last change time < {CLEAN_MAX_HOURS} hours ")
    logger.info("End clean_backup_dir")

def clean_backupfailed_file(config,file_prefix):
    logger.info("Begin clean_backupfailed_file")
    for file in os.listdir(config['BACKUPDIR']):
        file_path = os.path.join(config['BACKUPDIR'], file)
        #charge the file_path is a file
        if os.path.isfile(file_path) and file.startswith(file_prefix):
            os.remove(file_path)
            logger.info(f"delete file {file_path}, because the backup is failed.")
    logger.info("End clean_backupfailed_file")

def check_config(config):
    '''
    no retuern , only check parameters
    '''
    logger.info("Begin check_config")

    #check parameter type
    if not isinstance(config, dict):
        logger.error("parameter config is not a dict")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)

    #check REQUIRED parameters
    missing_keys = [k for k in REQUIRED_KEYS if k not in config]
    if missing_keys:
        logger.error(f"Missing required parameters: {missing_keys}. Required: {REQUIRED_KEYS}")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)

    #check schemas parameter must be list
    if not isinstance(config['SCHEMAS'], list):
        logger.error("parameter SCHEMAS is not a list. for example:\"SCHEMAS\" : [\"USER1\",\"USER2\"]")
        alarm_to_prometheus(ALARM_URL,"config check failed")
        sys.exit(1)

    #check DUMPFILE_PREFIX,LOGFILE_PREFIX length
    if not isinstance(config['DUMPFILE_PREFIX'], str) or not isinstance(config['LOGFILE_PREFIX'], str):
        logger.error("parameter DUMPFILE_PREFIX or LOGFILE_PREFIX is not a string")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)
    if len(config['DUMPFILE_PREFIX']) >MAX_LENGTH or len(config['LOGFILE_PREFIX']) >MAX_LENGTH:
        logger.error(f"parameter DUMPFILE_PREFIX or LOGFILE_PREFIX is too long must be less than {MAX_LENGTH}")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)

    #check parallel parameter
    if int(config['PARALLEL']) > MAX_PARALLEL:
        logger.error(f"parameter PARALLEL is too large must be less than {MAX_PARALLEL}")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)
    
    #check backupdir legitimacy
    if config['BACKUPDIR'].startswith(SURETY_DIR):
        logger.info(f"parameter BACKUPDIR start with {SURETY_DIR},is ok")
    else:
        logger.error(f"parameter BACKUPDIR must start with {SURETY_DIR}")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"config check failed")
        sys.exit(1)



    logger.info("End check_config , config check pass")
def ExpdbCommandCreate(config) -> str:
    '''
    create expdp command return string
    config is dict
    '''
    logger.info("Begin ExpdbCommandCreate")

    #using DUMPFILE_PREFIX and LOGFILE_PREFIX to create zhe dumpfile_name and logfile_name
    find_dmp_file_name_prefix = config['DUMPFILE_PREFIX'] + '_' + datetime.now().strftime("%Y%m%d%H")
    dumpfile_name = config['DUMPFILE_PREFIX'] + '_' + datetime.now().strftime("%Y%m%d%H%M") + '_%U.dmp'
    logfile_name = config['LOGFILE_PREFIX'] + '_' + datetime.now().strftime("%Y%m%d%H%M") + 'exp.log'

    #charge CONN_USER_PASS exist,if not exist ,using dba
    if "CONN_USER_PASS" not in config:
        CONN_USER_PASS = "'/ as sysdba '"
    else:
        CONN_USER_PASS = config["CONN_USER_PASS"]

    #schemas list to str
    SCHEMAS = ','.join(config['SCHEMAS'])

    #parfile --NOT IMPLEMENTED

    #INCLUDE,EXCLUDE--NOT IMPLEMENTED

    ExpdpCmd_List = [
        "expdp",
        CONN_USER_PASS,
        "directory="+config['DIRECTORY'],
        "SCHEMAS="+SCHEMAS,
        "dumpfile="+dumpfile_name,
        "logfile="+logfile_name,
        "parallel="+config['PARALLEL'],
        "cluster=N"
    ]
    logger.info(f"End ExpdbCommandCreate zhe expdpcmd is: {ExpdpCmd_List}")

    return ExpdpCmd_List,find_dmp_file_name_prefix,logfile_name

def read_last_line_with_prefix(config,file_path,prefix_content) -> int:
    file_full_name = os.path.join(config['BACKUPDIR'], file_path)
    try:
        logger.info(f"Begin read_last_line_with_prefix")
        with open(file_full_name, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            last_line = f.readline().decode()
            if prefix_content in last_line:
                logger.info(f"the {prefix_content} in {file_full_name},the dmp is ok")
                return 0
            else:
                logger.warning(f"the {prefix_content} not in {file_full_name},the dmp has error")
                return -1
    except Exception as e:
        logger.error(f"Failed to read last line from {file_full_name}: {e}")
        return -1

def find_files_for_this_backup(config) ->list:
    logger.info("Begin find_files_for_this_backup")
    back_files_list= []
    for file in os.listdir(config['BACKUPDIR']):
        file_path = os.path.join(config['BACKUPDIR'], file)
        #charge the file_path is a file
        if os.path.isfile(file_path) and file.startswith(find_dmp_file_name_prefix):
            logger.info(f"find the dmp file {file_path}")
            back_files_list.append(file_path)
    logger.info(f"End find_files_for_this_backup,the file list is {back_files_list}")
    return back_files_list

def compress_file_zstd(file_list) ->list:
    logger.info("Being compress file wiht zstd")
    compressed_file_list=[]
    if len(file_list) ==0:
        logger.error("can not find any file for compress")
        return compressed_file_list
    #being execute zstd
    for file in file_list:
        compressed_file_name = file + '.zst'
        compress_cmd_list = [
            'zstd',
            file,
            '-T4',
            '--rm'
            ,'-v'
            ,'-o'
            ,compressed_file_name
            ]
        try:
            zstd_process = subprocess.run(compress_cmd_list,check=True,timeout=COMPRESS_TIMEOUT_MINUS * 60)
            retcode = expdp_process.returncode
            if retcode == 0:
                logger.info(f"compress {file} to {compressed_file_name} success")
                compressed_file_list.append(compressed_file_name)
            else:
                logger.error(f"compress {file} to {compressed_file_name} failed")
        except Exception as e:
            logger.error(f"compress {file} to {compressed_file_name} failed with err: "+str(e))
    
        
    logger.info("End compress file wiht zstd")
    return compressed_file_list


def upload_to_oss_with_ossutil(file_list):
    logger.info("Begin upload_to_oss_with_ossutil")
    if len(file_list) ==0:
        logger.error("can not find any file for compress")
        return
    for file in file_list:
        file_name_without_path = file.split('/')[-1]
        oss_bucket_fullname = 'oss://'+config['OSS_BUCKET']+'/'+file_name_without_path
        compress_cmd_list = [
            'ossutil64',
            'cp',
            file,
            oss_bucket_fullname
            ]
        try:
            ossutil_process = subprocess.run(compress_cmd_list,check=True,timeout=OSSUTIL_TIMEOUT_MINUS * 60)
            retcode = ossutil_process.returncode
            if retcode == 0:
                logger.info(f"upload {file} to {oss_bucket_fullname} success")
            else:
                logger.error(f"upload {file} to {oss_bucket_fullname} failed")
        except Exception as e:
            logger.error("upload file to oss failed with err: "+str(e))


def gen_md5_hash(input_string):
    # Create an MD5 hash object
    md5 = hashlib.md5()

    # Update the hash object with the bytes-like object (must be encoded to bytes)
    md5.update(input_string.encode('utf-8'))

    # Get the hexadecimal representation of the hash
    md5_hash = md5.hexdigest()

    return md5_hash

def alarm_to_prometheus(uri,api,messages):
    current_timestamp = int(time.time())
    fingerprint = gen_md5_hash(str(current_timestamp))
    request_body = {
        "status": "firing",
        "cate": "dbbackup",
        "cluster": "default",
        "group": "xxxx",
        "rule_name": "xxx",
        "rule_note": messages,
        "severity": "P4",
        "tags": {
        },
        "rule_id": 0,
        "callbacks": [],
        "annotations": {
        },
    }

    headers = {
    'Cache-Control': 'no-cache',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json'
    }

    try:
        request_body_str = json.dumps(request_body)
        conn = http.client.HTTPConnection(uri)
        conn.request("POST", api, request_body_str, headers)
        res = conn.getresponse()
        data = res.read()
        logger.warning("send alart to prometheus")
        logger.warning(data.decode("utf-8"))
    except Exception as e:
        logger.error("send alarm to prometheus failed with err: "+str(e))
    finally:
        conn.close()



if __name__ == '__main__':
    logger.info("start expdp")

    #work dir
    try:
        os.chdir(WORKDIR)
    except Exception as e:
        logger.error("change work dir failed with err: "+str(e))
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"change work dir failed")
        sys.exit(1)



    sys_parameter = {}
    #default configfile
    sys_parameter["config"]="./expdpconfig.json"
    for parameter in sys.argv[1:]:
        if '=' in parameter:
            key, value = parameter.split('=', 1)
            sys_parameter[key] = value

    #1.read expdpconfig,change to dict
    try:
        with open(sys_parameter["config"], 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"parase config failed with err: "+str(e))
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"parse config failed")
        sys.exit(1)


    #2. clean backup_file
    clean_backup_dir(config)

    #2.check config file
    check_config(config)

    #3.make expdp command
    expdpcmd_list,find_dmp_file_name_prefix,logfile_name = ExpdbCommandCreate(config)

    #4.execute expdp command -- long time not async
    logger.info("Begin execute expdp command")
    try:
        expdp_process = subprocess.run(expdpcmd_list,check=True,timeout=EXPDP_TIMEOUT_MINUS * 60)
        retcode = expdp_process.returncode
    except subprocess.CalledProcessError as e:
        retcode = -1
        logger.error("run expdpcmd failed with err: "+str(e))
    except Exception as e:
        retcode = -1
        logger.error("run expdpcmd failed with err: "+str(e))
    logger.info(f"End execute expdp command with result_code: {retcode}")

    #5.check expdp result
    expdp_res_code = read_last_line_with_prefix(config,logfile_name,"successfully completed")
    if retcode == 0 and expdp_res_code==0:
        logger.info("expdp success")
    else:
        #shoud add alarm
        logger.error("expdp failed")
        logger.info("beacuse expdp failed ,clean dmpfile!")
        clean_backupfailed_file(config,find_dmp_file_name_prefix)
        logger.error("exit expdp beacuse expdp failed")
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"expdp failed")
        sys.exit(1)
    
    #6. compress dmp files
    try:
        backup_file_list = find_files_for_this_backup(config)
        compressed_file_list = compress_file_zstd(backup_file_list)
    except Exception as e:
        clean_backupfailed_file(config,find_dmp_file_name_prefix)
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"compress dmp files failed")
        logger.error("compress dmp files failed with err: "+str(e))

    #7. upload to oss
    try:
        upload_to_oss_with_ossutil(compressed_file_list)
    except Exception as e:
        clean_backupfailed_file(config,find_dmp_file_name_prefix)
        logger.error("upload to oss failed with err: "+str(e))
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"backup file upload to oss failed")

    #8. clean local dump file

    try:
        logger.info("Being clean local dump file for today backup mission")
        clean_backupfailed_file(config,find_dmp_file_name_prefix)
        logger.info("End clean local dump file for today backup mission")
    except Exception as e:
        alarm_to_prometheus(ALARM_URL,ALEMR_API,"clean local file failed")
        logger.error("clean local dump file for today backup mission fail!")








