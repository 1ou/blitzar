# blitzar

Distributed timeseries database. With SQL syntax.

Create database: ```create database database_name;```

Use database: ```use database_name;```

Create table: ```create table database_name (time long not null primary key, value long not null);```

Insert into table: ```insert into database_name (time , value) values (30000, 200);```

Select from table: ```select * from database_name;```

Select from table with condition: ```select * from database_name where time = 30000;```


First raw release: 0.0.1

Todo
1. Storing databases +
2. Storing tables
    - metadata +
    - data in b-tree +
3. Server +
4. Process sql queries +
5. Database transactions
6. Service discovery +

Next:

1. Distributed version
