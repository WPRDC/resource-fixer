# Pre-load some mappings for columns that come up in Crash Data
# datasets but do _not_ appear in the data dictionary.
# These can be overwritten if there do happen to be definitions
# in fields.json.
import json

output = open("extra_fields_dict.json", 'w')

def field_mapper_seed():
    field_mapper = {}
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
    return field_mapper

data_dict_list = field_mapper_seed()
pretty_print_output = json.dumps(data_dict_list, indent=4, sort_keys=True)
output.write(pretty_print_output)
output.close()
