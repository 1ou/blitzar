# blitzar
[![Build Status](https://travis-ci.org/toxa108/blitzar.svg?branch=master)](https://travis-ci.org/toxa108/blitzar)
[![codecov](https://codecov.io/gh/toxa108/blitzar/branch/master/graph/badge.svg)](https://codecov.io/gh/toxa108/blitzar)

The Blitzar is a timeseries database with sql syntax. 
The data storing in the b+tree data structure on the disk. 

Capabilities (according to release 1.0.0)
======

User
------
```
The database doesn't support permissions yet.
There is only a capability to create User with login and password to manage databases and tables. 
```

Structure
------
```
User is able to create databases and tables inside of them.
```

Data types
------
```
- short (2 bytes)
- int (4 bytes)
- long (8 bytes)
- varchar(n bytes)
```

Indexes
------
```
The tables are support primary (clustered) indexes. 
That's why there is capability of creation only 1 index per table. Index is able to contain 1 to N columns. 
```

Queries
------
```
User can insert, select data with conditions like (>, <, =, <>, >=, <=). 
The join syntax is not available because there are no relations between tables. 
For now there is ability only to insert data, the update and delete operations are not supported on the data yet.

create database database_name;
use database_name;
create table database_name (time long not null primary key, value long not null);
insert into database_name (time , value) values (30000, 200);
select * from database_name;
select * from database_name where time = 30000;
select value from database_name where time > 30000 and time < 40000;
```

Benchmarks
------
```
The efficiency of the library wasn't the goal of blitzar that's why there are no high expectations.
``` 

### Road Map

#### Interim release: 0.1.0
- [x]  Storing databases
- [x]  Storing tables
- [x]  Server
- [x]  Service discovery
- [x]  Process sql queries

#### Interim release: 0.2.0
- [x]  Thread safe version of database
- [ ]  Bloom filter
- [ ]  Buffer pool
- [x]  Code coverage > 70% 

#### Interim release: 0.3.0
- [ ]  User managment
- [ ]  Batch insert
- [ ]  Benchmarks
- [ ]  Code coverage > 80% 

#### Interim release: 0.4.0
- [ ]  Query specification (byte protocol)
- [ ]  Code coverage > 85% 

#### Release: 1.0.0 
- [ ]  Multicolumn indexes
- [ ]  Integration tests
- [ ]  Add and remove columns
- [ ]  Code coverage > 90%

#### future plans
- Database transactions
- Distributed version

