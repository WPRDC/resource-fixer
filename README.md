# resource-fixer
This script provides a workaround for a bug that sometimes occurs when
uploading  a dataset to a CKAN instance. In the importing of a CSV file to
the CKAN database (during which the contents of the CSV file are
transformed into a datastore), CKAN attempts to auto-detect the type of
each column through random sampling of rows. This sampling is too sparse
and can sometimes incorrectly assign a type to a field, which can lead to
an error at some point during the import process (e.g., when trying to
assign a string to an integer field). If the import process involves the
data being added to the datastore in chunks (rather than processing the
whole CSV file and then uploading it as a single datastore upsert), the
resulting CKAN repository can have a datastore that is missing some of the
rows from the original CSV file.

In this generalized version, the script takes as inputs the CKAN resource
number, the name of the CSV file to be uploaded, and the key column
(which is necessary due to a bug in CKAN). The server to upload the data
to may also optionally be specified as the last command-line argument.

Each set of data to be uploaded and its associated JSON files specifying
its fields should be kept in its own directory.

This script uses two data dictionaries for the resource. The first data
dictionary is stored in the file "fields.json". A second data dictionary
to add extra fields (not included in the primary fields.json, for cases
where the initial data dictionary is incomplete) can be generated by
running a properly customized version of the "extra_fields.py" script
(found in "sample-directory"). This generates "extra_fields.json", which
the reset_resource script also looks for. In most cases,
"extra_fields.json" can consist of just an empty dictionary (i.e, the
entire file contents can be "{}").
