# resource-fixer
This script provides a workaround for a bug that sometimes occurs when uploading 
a dataset to a CKAN instance. In the importing of a CSV file to the CKAN
database (during which the contents of the CSV file are transformed into
a datastore), CKAN attempts to auto-detect the type of each column through
random sampling of rows. This sampling is too sparse and can sometimes incorrectly
assign a type to a field, which can lead to an error at some point during the
import process (e.g., when trying to assign a string to an integer field). If 
the import process involves the data being added to the datastore in chunks 
(rather than processing the whole CSV file and then uploading it as a single
datastore upsert), the resulting CKAN repository can have a datastore that is
missing some of the rows from the original CSV file. 

This script takes as inputs the CKAN resource number and the CSV file to be
uploaded. It also uses a data dictionary for the resource. This data dictionary is 
stored in the file "fields.json". This data dictionary is currently being generated by 
a separate script from a data dictionary provided along with the original CSV
file by the organization that generated the data. Since there were fields 
in the CSV file that were not accounted for by the original data dictionary,
some additional fields are being inserted in this script.