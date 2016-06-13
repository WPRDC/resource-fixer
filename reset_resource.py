from datapusher import Datapusher
import json
import csv
import sys
import re
from collections import OrderedDict
from datetime import datetime
#import ftfy
#import codecs

upload_in_chunks = True

def upsert_data(resource_id,data):
    if modify_datastore:
    #    r = dp.upsert(resource_id, ftfy.fix_text(data), method='upsert')
        r = dp.upsert(resource_id, data, method='upsert')
        if r.status_code != 200:
            print(r.text)
        else:
            print("Data successfully stored.")
        print("Status code: %d" % r.status_code)

def type_or_none(convert_to,s):
    if s == '':
        return None
    try:
        typed = convert_to(s)
    except:
        raise ValueError('Unable to convert %s to type %s.' % (s,str(convert_to)))
        return None
    else:
        return typed

def preload_field_mapper(wd):
    # This function takes the given working directory, finds the
    # 'extra_fields.json' file, and uses the encoded dict as a
    # supplemental data dictionary, which it returns to seed
    # field_mapper.

    field_mapper = {}
    with open(wd+'/extra_fields_dict.json') as f:
        extra_fields_dict = json.load(f)
    for field in extra_fields_dict.keys():
        field_mapper[field] = extra_fields_dict[field]
    return field_mapper

modify_datastore = True

if(len(sys.argv) >= 4):
    resource_id = sys.argv[1]
    data_file = sys.argv[2]
    supplied_key = sys.argv[3]
else:
    print("needs to be python reset_resource.py <resource_id> <input_file.csv> <supplied_key> [<server>]")
    exit(-1)
if(len(sys.argv)== 5):
    server = sys.argv[4]
else:
    server = "Staging"

# The 'fields.json' file should be in the same directory as the input data file.
wd = '/'.join(data_file.split('/')[:-1]) # The working directory
fields_file = wd+'/fields.json'
with open(fields_file) as f:
    fields = json.load(f)

with open('ckan_settings.json') as f:
    settings = json.load(f)

if modify_datastore:
    dp = Datapusher(settings, server=server)

    # Delete datastore
    dp.delete_datastore(resource_id)

field_mapper = preload_field_mapper(wd)

# Obtain the headings from the CSV file and use their order to
# maintain consistency with the other uploaded resources that
# do not need to be repaired with this script.
d_fi = open(data_file,'r')
#d_fi = codecs.open(data_file,'r',encoding='UTF-8')
original_headings = re.sub(r'[\r\n]*','',d_fi.readline())
original_order = original_headings.split(',')
d_fi.close()

# [ ] Write a function to check that assumed values are correct.

# Add all fields to field_mapper.
for field in fields:
    field_mapper[field["id"]] = field["type"]

reordered_fields = []
for heading in original_order:
    reordered_fields.append({"id": heading, "type": field_mapper[heading]})

# # Add the new fields to the fields list obtained from
# # the data dictionary.
# data_dictionary_ids = [d.keys()[0] for d in fields]
# for new_id in field_mapper.keys():
#     if new_id not in data_dictionary_ids:
#         fields.append({"id": new_id, "type": field_mapper[new_id]})

if modify_datastore:
    # Create Datastore
    #dp.create_datastore(resource_id, reordered_fields, keys='CRASH_CRN')
    if supplied_key in ['NONE','None','none']:
        dp.create_datastore(resource_id, reordered_fields)
    else:
        dp.create_datastore(resource_id, reordered_fields, keys=supplied_key)

data = []
i = 0
with open(data_file) as f:
    dr = csv.DictReader(f)

    for row in dr:
        # row is a dict with keys equal to the CSV-file column names
        # and values equal to the corresponding values of those parameters.
        # FIX FIELD TYPES HERE
        for column in row.keys():
            if column not in field_mapper.keys():
                print("column = %s" % (column))
                raise ValueError('A field without a recognized type was found.')
                break

            if field_mapper[column] == "numeric":
                row[column] = type_or_none(float,row[column])
            elif field_mapper[column] == "int":
                if column in ['OwnerZip'] and len(row[column]) > 5:
                    print(row)
                    row[column] = row[column][:5] # Truncate ZIP+4 codes to 5 digits.
                row[column] = type_or_none(int,row[column])
            elif field_mapper[column] == "text":
                row[column] = str(row[column])
            elif field_mapper[column] == "timestamp":
#                row[column] = datetime.strptime(row[column], "%Y-%m-%dT%H:%M:%S")
                row[column] = row[column]
            else:
                raise ValueError('A field without a recognized type was found.')
        reordered_row = OrderedDict([(fi['id'],row[fi['id']]) for fi in reordered_fields])
        data.append(reordered_row)
        i += 1
        if i % 5000 == 0:
            if upload_in_chunks:
                if i > 0:
                    upsert_data(resource_id,data)
                data = []
            print(i)

upsert_data(resource_id,data)
