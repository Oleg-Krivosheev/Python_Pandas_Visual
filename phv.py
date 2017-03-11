#from pyhive import presto

#cursor = presto.connect

#presto.connect(host='38.101.195.54', port=10000, username='admin:admin', database='ds').cursor()


#import pyhs2

#with pyhs2.connect(host='38.101.195.54',
#                   port=10000,
#                   authMechanism="PLAIN",
#                   user='admin',
#                   password='admin',
#                   database='ds') as conn:
#    with conn.cursor() as cur:
#       #Show databases
#        print(cur.getDatabases())

        #Execute query
#        cur.execute("select * from airports limit 50")

        #Return column info from query
#        print(cur.getSchema())

        #Fetch table results
#        for i in cur.fetch():
#            print(i)

from pyhive import hive
from TCLIService.ttypes import TOperationState

cursor = hive.connect(host='38.101.195.54', port=10000, username='admin:admin', database='ds').cursor()

cursor.execute('SELECT * FROM airports LIMIT 10', async=True)

status = cursor.poll().operationState
while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    logs = cursor.fetch_logs()
    for message in logs:
        print(message)

    # If needed, an asynchronous query can be cancelled at any time with:
    # cursor.cancel()

    status = cursor.poll().operationState

print(cursor.fetchall())
