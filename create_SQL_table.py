############ Convert user feature vector into SQL table
### This script converts dictionary of user location preferences into a SQL table titled 'userLocation'

import pymysql as mdb

con = mdb.connect('localhost', 'root', 'password', 'muse') #host, user, password, #database
con.autocommit(False)
cur = con.cursor()
cur.execute("USE muse;")

SQLname = 'userLocation'
keyList = range(0,len(locOpts))

############## Create table, columns here (Column names converted to ints)
var = ' ( userID varchar(100), '
for col in range(0,len(keyList)):
    var = var + '`' + str(keyList[col]) + '`' + ' ' + 'int, '

var = var[:-2] + ')'

cmd = 'CREATE TABLE ' +  SQLname  + var + ';'
print cmd
cur.execute(cmd)
##############


users = userJobLocationVectors.keys()
############# Fill out rows here
allRows = userJobLocationVectors
c=0
for row in users:
  c=c+1
  if c%10000 == 0: ## Commit every 10k rows
    print c, ' of ', len(users)
    cur.execute('COMMIT;')
  cols = '(userID, ' +  '`' + '`,`'.join(str(x) for x in keyList) + '`)'
  data = '("' + row + '", ' + ', '.join(str(x) for x in allRows[row]) + ')'

  cmd = 'INSERT INTO ' + SQLname + cols + ' VALUES ' + data + ';'
  cur.execute(cmd)
##################
cur.execute('COMMIT')