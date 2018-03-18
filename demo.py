"""
Project: 
Author: ManuelG
Created: 28-Dec-17 19:29
License: MIT
"""

import paramiko

# key = paramiko.RSAKey.from_private_key_file('./keys/id_rsa')
# c = paramiko.SSHClient()
# c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
# c.connect(hostname='130.89.217.201', port=22,username='geosmartsys', password='Gevent@2126', pkey=key)
#
# c.close()

paramiko.util.log_to_file('demo2_sftp.log')
pkey = paramiko.RSAKey.from_private_key_file('./keys/id_rsa', 'CeP#geo-event')

host = '130.89.217.201'
port = 22
transport = paramiko.Transport((host, port))



password = 'Gevent@2126'
user = 'geosmartsys'
transport.connect(username=user,  pkey=pkey)

sftp = paramiko.SFTPClient.from_transport(transport)

sftp.close()


