# SQL Queries on the CMS 2010 Medicare Beneficiary Data #

## Project Goals ##

The purpose of this project was to create thoughtful SQL queries for analyzing CMS’ 2010 Medicare Beneficiary data. This project includes an ipython notebook with example SQL queries that were created based on a subset of this data. It also includes a python script that utilizes the *psycopg2* Python package to create separate functions that make use of an SQL query to get data from the database and returns the results from the query in JSON format. 

### CMS’ 2010 Medicare Beneficiary Data ###

This project makes use of a subset of CMS’ synthetic 2010 Medicare Beneficiary data included within the data directory of this repository. These csv files were used to create  two tables within a PostgreSQL database.

### Python’s *psycopg2* ###

Psycopg is a PostgreSQL database adapter for Python. It is used to connect to a SQL database, and allows you to create and execute SQL queries within Python. 

