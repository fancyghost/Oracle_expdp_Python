# Oracle_expdp_Python
using expdp backup oracle database11G

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
