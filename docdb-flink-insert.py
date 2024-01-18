import pymongo
import random
import string
import datetime


def generate_random_string(size):
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(size))
    return random_string


random_string = generate_random_string(4 * 64)
idn = 0
client = pymongo.MongoClient('mongodb://user:password@ping.cluster-xxxx.us-west-2.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred
&retryWrites=false')
db = client.test
col = db.bench
for num in range(0, 10000):
    data = []
    for i in range(0, 100):
        now = datetime.datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        data.append({'_id': str(hash(now))+'#'+str(idn), 'txt': random_string, 'dt': int(now.timestamp()) })
        idn += 1
    col.insert_many(data)
client.close()
