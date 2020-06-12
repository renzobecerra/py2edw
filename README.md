# py2edw
A high level Python wrapper for remotely connecting to SQL databases and querying using pandas dataframes.

- SQL commands can be executed using the SQL operator.
- All queries input and outout using pandas dataframes.
- Optional ssh tunnel connection

#### Connectors:
- postgresql
- mysql

#### Import:
`from py2edw import postgresql, mysql`

#### db  & ssh parameter format:
`db_params = {"database": <db_name>, "user": <username>, "password": <password>, "host": <host>}`

`ssh_params = {"ssh_ip": <ip_address>, "ssh_port": <port>, "ssh_username": <username>, "ssh_password": <password>, "remote_bind_ip": <local_ip>, "remote_bind_port": <local_port>}`

#### Example code:
`py2edw = mysql.py2edw(db_params, ssh_params=False)`

`py2ew.start_connection()`

`py2edw.import_DataFrame("select * from table_name")`

`py2edw.close_connection()`

#### Avaliable operators:
- sql
- import
- insert
- upsert
- start connection
- close connection
- show tables

`py2edw.help()` for details
