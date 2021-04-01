
#### Sparkfy with Redshift
   
   Redshift is a database on AWS that is easy to provision and maintain, and can be configured to have automatic backups of data and table schematics. The Sparkfy database was fully provisioned in Redshift to take advantage of the benefits mentioned.

This database was made to facilitate analyzes that answer which songs are heard most by users.

The dimension tables are:
- `users`, has the data for each user;
- `songs`, has the data for each song;
- `artists`, has the data for each artists;
- `time`, has the time data for each song;
- `staging_events`, has the raw data that comes from the application;
- `staging_songs`, has the raw data of the songs.


The actual table is:
- `songsplays`, has the specific data that is used by analysts to analyze customers' musical preferences.

## Instructions for running the application.
#### Menu:
- ##### `Requirements`
To run the application you must first have an AWS account and set up the redshift environment. For this you can use the [`official documentation`](https://docs.aws.amazon.com/redshift/index.html).
After configuring your environment, place the following information about your environment in the `dwh.cfg` file:
- HOST
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_PORT
- ARN


- ##### `Commands to run the application.`
##### 1 - Delete the tables if they already exist and create them again. Open the terminal use the command:           
        python3 create_tables.py

##### 2 - To run the ETL process use the command:
        python3 etl.py
        python3 etl.py