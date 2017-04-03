# SQL Server → PostgreSQL tool
The tool creates the tables in PostgreSQL database and copies data from SQL Server database into them.
When the copying is finished the tool verifies that the data in each field of each column 
are the same in both databases (incuding the binary data using md5 hases).  
The tool supports conversion of following:
* **Columns** (it converts the data types to the compatible ones)
* **Identities** (it creates sequences and configures columns to use them)
* **Primary keys** (including compositional keys)
* **Indices** (the included columns are ignored. If 2 indices have the same columns the 2nd one will not be created)
* **Foregin keys** (including the changes propagation mode (cascade/no action/set null))

## How to use
To copy the database you should do following:
1. Get credentials of both databases.
2. Create a new database in PostgreSQL
3. Modify the connection strings in `App.config`
4. Set the source database and schema in `App.config`
5. Change the other paramters in `App.config` (optional)
6. Run the application

## Configuration parameters
In `App.config` you can change following parameters:
* **MsConStr** - SQL Server connection string.
* **PgConStr** - PostgreSQL connection string.
* **MsDbName** - SQL Server source database.
* **MsSchema** - SQL Server source schema.
* **ChunkSize** - The size of the buffer in rows. The tool gets the source data in pages. 
This parameters sets the size of the page in rows.
* **Fps** - The progressbar update rate in Frames Per Second.

## Minimal system requirements
The default chunk value of _100 000_ rows can cause memory consumption up to **2.5 GB**

## The tool performance
The tool was tested on a machine with following specs:
* i5 CPU
* 32GB RAM
* SATA SSD
* Windows 10

Both databases were installed at the same SSD (it was also the system drive).  
The copying speed was ~ **1 GB / 1 minute**.  
The verification speed was about the same.