from datapusher import Datapusher
import json
import csv
import sys
import re
import datetime
from collections import OrderedDict


def type_or_none(convert_to, s):
    if s == '':
        return None
    try:
        typed = convert_to(s)
    except:
        raise ValueError('Unable to convert %s to type %s.' % (s, str(convert_to)))
        return None
    else:
        return typed


modify_datastore = True

if len(sys.argv) >= 3:
    resource_id = sys.argv[1]
    data_file = sys.argv[2]
else:
    print("needs to be python reset_resource.py <resource_id> <input_file.csv> [<server>]")
    exit(-1)
if len(sys.argv) == 4:
    server = sys.argv[3]
else:
    server = "Staging"

with open('fields.json') as f:
    fields = json.load(f)

with open('ckan_settings.json') as f:
    settings = json.load(f)

if modify_datastore:
    dp = Datapusher(settings, server=server)

    # Delete datastore
    dp.delete_datastore(resource_id)


# Obtain the headings from the CSV file and use their order to
# maintain consistency with the other uploaded resources that
# do not need to be repaired with this script.
d_fi = open(data_file, 'r')
original_headings = re.sub(r'[\r\n]*', '', d_fi.readline())
original_order = original_headings.split(',')
d_fi.close()

# Add all fields to field_mapper.
field_mapper = {}
for field in fields:
    field_mapper[field["id"]] = field["type"]

reordered_fields = []
for heading in original_order:
    reordered_fields.append({"id": heading, "type": field_mapper[heading]})


if modify_datastore:
    # Create Datastore
    dp.create_datastore(resource_id, reordered_fields)

data = []
i = 0
with open(data_file) as f:
    dr = csv.DictReader(f)

    for row in dr:
        # row is a dict with keys equal to the CSV-file column names
        # and values equal to the corresponding values of those parameters.
        for column in row.keys():
            if column not in field_mapper.keys():
                print("column = %s" % (column))
                raise ValueError('A field without a recognized type was found.')
                break

            if field_mapper[column] == "numeric":
                row[column] = type_or_none(float, row[column])
            elif field_mapper[column] == "int":
                row[column] = type_or_none(int, row[column])
            elif field_mapper[column] == "text":
                row[column] = str(row[column])
            elif field_mapper[column] == "time":
                if row[column]:
                    row[column] = datetime.datetime.strptime(row[column], "%H:%M").strftime("%H:%M")
                else:
                    row[column] = None
            else:
                raise ValueError('A field without a recognized type was found.')

        reordered_row = OrderedDict([(fi['id'], row[fi['id']]) for fi in reordered_fields])
        data.append(reordered_row)
        i += 1
        if i % 250 == 0:
            print(i)

if modify_datastore:
    r = dp.upsert(resource_id, data, method='insert')
    if r.status_code != 200:
        print(r.text)
    else:
        print("Data successfully stored.")
    print("Status code: %d" % r.status_code)
