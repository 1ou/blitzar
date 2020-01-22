# blitzar

Distributed timeseries database. With SQL syntax.

Create database: ```create database_name;```

Use database: ```use database_name;```

Create table: ```create table example (time long not null primary key, value long not null);```

Insert into table: ```insert into example (time , value) values (30000, 200);```


Todo:

1. Storing databases +
2. Storing tables
    - metadata +
    - data in b-tree +
3. Server +
4. Translate sql queries
5. Database transactions
6. Service discovery +
7. Distributed version
