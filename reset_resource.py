from datapusher import Datapusher
import json
import csv
import sys

if(len(sys.argv) >= 3):
    resource_id = sys.argv[1]
    data_file = sys.argv[2]
else:
    print("needs to be python reset_resource.py <resource_id> <input_file.csv> [<server>]")
    exit(-1)
if(len(sys.argv)==-4):
    server = sys.argv[3]
else:
    server = "Staging"

with open('fields.json') as f:
    fields = json.load(f)

with open('ckan_settings.json') as f:
    settings = json.load(f)

dp = Datapusher(settings, server=server)

# Delete datastore
dp.delete_datastore(resource_id)

# Create Datastore
dp.create_datastore(resource_id, fields, keys='CRASH_CRN')

data = []
i = 0
with open(data_file) as f:
    dr = csv.DictReader(f)
    for row in dr:
        i+=1
        if i % 1000 == 0:
            print(i)

        if 'Reject to 311' not in row['REQUEST_TYPE']:
            data.append(row)

dp.upsert(resource_id, data, method='upsert')