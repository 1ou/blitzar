# blitzar

The Blitzar is a distributed timeseries database with sql syntax. 
The data storing in the b+tree data structure on the disk. 

Create database:
```create database database_name;```

Use database:
```use database_name;```

Create table: 
```create table database_name (time long not null primary key, value long not null);```

Insert into table: 
```insert into database_name (time , value) values (30000, 200);```

Select from table: 
```select * from database_name;```

Select from table with condition: 
```select * from database_name where time = 30000;```

Select from table with range condition: 
```select * from database_name where time > 30000 and time < 40000;```

### First raw release: 0.0.1

#### Todo
- [x]  Storing databases
- [x]  Storing tables
- [x]  Server
- [x]  Service discovery
- [ ]  Process sql queries (in progress)
- [ ]  Locking - thread safe version of database (in progress)
- [ ]  Database transactions

Future:

- [ ]  Benchmarks
- [ ]  Distributed version
