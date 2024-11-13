### creating local postgres database
requirements:
installed Docker

command for creating postgres database:
```shell
docker pull postgres
```
```shell
docker run -d  -p 5432:5432 -e POSTGRES_USER=ibm -e POSTGRES_PASSWORD=SomeRandomPassword! -e POSTGRES_DB=exercise postgres
```

## run generate_tables.py
This script is generating tables in postgres database.

## open two terminals and in first run generate_profiles.py and in the second extract_and_transform.py
# generate_profiles.py 
 This script generates fake customer profiles using the Faker library and inserts them into a PostgreSQL database. It logs the process and introduces random delays between inserts.

# extract_and_transform.py
This script extracts new customer profiles from a PostgreSQL database, transforms them into JSON, and appends them to a file. It tracks the last extraction timestamp using a watermark and updates it after each fetch.
