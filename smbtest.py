from smb.SMBConnection import SMBConnection

# There will be some mechanism to capture userID, password, client_machine_name, server_name and server_ip
# client_machine_name can be an arbitary ASCII string
# server_name should match the remote machine name, or else the connection will be rejected

import pymongo

client = pymongo.MongoClient('10.101.167.107');
db = client['ant'];
table = db['yoka_news'];
news=open('yoka_news','w');
set={}
for r in table.find():
    if '正文' not in r:
        continue
    if r['url'] in set:
        continue;
    s= ['url','正文','标题'];
    s= [r[l].strip() for l in s];
    news.write('\t'.join(s)+'\n')
    set[r['url']]=r['url']
news.close()
exit();


import  os
os.listdir('smb://10.101.167.107/AntShare/mongodb')

userID='desert.zym'
password='2wsx3edc@@'
server_name='10.101.167.107';
conn = SMBConnection(userID, password, "", "", use_ntlm_v2 = True)
assert conn.connect(server_name, 139)


shareslist = conn.listShares()
for i in shareslist:
    print(i.name);
f = open('temp.txt', 'wb')  # 就是要下载下来存放的那个文件的壳子
conn.retrieveFile('AntShare', 'mongodb.log', f)  # 它会把文件写在f里面
f.close()


f = open('tools.json', 'rb')
conn.storeFile('AntShare', 'picture/tools.json', f)
f.close()
#


