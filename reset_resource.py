from datapusher import Datapusher
import json
import csv
import sys

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


modify_datastore = True

if(len(sys.argv) >= 3):
    resource_id = sys.argv[1]
    data_file = sys.argv[2]
else:
    print("needs to be python reset_resource.py <resource_id> <input_file.csv> [<server>]")
    exit(-1)
if(len(sys.argv)== 4):
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

    # Create Datastore
    dp.create_datastore(resource_id, fields, keys='CRASH_CRN')

field_mapper = {}

# Pre-load some mappings for columns that come up in Crash Data
# datasets but do _not_ appear in the data dictionary.
# These can be overwritten if there do happen to be definitions
# in fields.json.
field_mapper["CRASH_COUNTY"] = "text" # example value: '02'
field_mapper["DRIVER_18YR"] = "int"
field_mapper["DRIVER_19YR"] = "int"
field_mapper["DRIVER_20YR"] = "int"
field_mapper["DRIVER_50_64YR"] = "int"
field_mapper["DRIVER_COUNT_18YR"] = "int"
field_mapper["DRIVER_COUNT_19YR"] = "int"
field_mapper["DRIVER_COUNT_20YR"] = "int"
field_mapper["DRIVER_COUNT_50_64YR"] = "int"
field_mapper["DRUG_RELATED"] = "int" # PRESUMED
#(even though ILLEGAL_DRUG_RELATED is "numeric")

field_mapper["DRUGGED_DRIVER"] = "int"
# This is being PRESUMED to be a Boolean.

field_mapper["EST_HRS_CLOSED"] = "int" # example value: '3'
field_mapper["HIT_PARKED_VEHICLE"] = "int"
field_mapper["TOTAL_UNITS"] = "int"
field_mapper["WORK_ZONE_IND"] = "text" # example value: 'N'


# [ ] Write a function to check that assumed values are correct.

for field in fields:
    field_mapper[field["id"]] = field["type"]

data = []
i = 0
with open(data_file) as f:
    dr = csv.DictReader(f)

    for row in dr:
        # row is a dict with keys equal to the CSV-file column names
        # and values equal to the corresponding values of those parameters.
        # FIX FIELD TYPES HERE
        for column in row.keys():
#            print(column,row[column])

            #if column == 'SPEC_JURIS_CD' and row[column] != '':
            #    print("SPEC_JURIS_CD: %s" % row[column])
            # In the 2009 file, the SPEC_JURIS_CD column (which represents
            # whether the crash occurred in a special jurisdiction is mostly empty.
            # Occasionally, it has the value "0" (which means that the jurisdiction
            # is not special, as indicated in the data dictionary.)

            if column not in field_mapper.keys():
                print("column = %s" % (column))
                raise ValueError('A field without a recognized type was found.')
                break

            if field_mapper[column] == "numeric":
                row[column] = type_or_none(float,row[column])
            elif field_mapper[column] == "int":
                row[column] = type_or_none(int,row[column])
            elif field_mapper[column] == "text":
                row[column] = str(row[column])
            else:
                raise ValueError('A field without a recognized type was found.')
        #break
        data.append(row)
        i += 1
        if i % 250 == 0:
            print(i)

if modify_datastore:
    r = dp.upsert(resource_id, data, method='upsert')
    print("Status code: %d" % r.status_code)
    print(r.text)
