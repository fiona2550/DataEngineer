import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

# DROP TABLES

business_staging_table_drop = "DROP TABLE if exists business_staging";
review_staging_table_drop = "DROP TABLE if exists review_staging";
business_location_drop = "DROP TABLE if exists business_location";
business_category_drop = "DROP TABLE if exists business_category";
business_operation_drop = "DROP TABLE if exists business_operation";
review_date_drop = "DROP TABLE if exists review_date";
review_text_drop = "DROP TABLE if exists review_text";
business_review_drop = "DROP TABLE if exists business_review";

#Create Staging Tables

business_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS business_staging(
"business_id" character varying (200),
name character varying (200),
"city" character varying (100),
"address" character varying (600),
latitude decimal(10,8),
longitue decimal (11,8),
postal_code character varying (50),
review_count int,
stars decimal (18,2),
state character(10),
categories character varying (3000),
hours character varying (3000),
is_open character varying (10)

)""")

review_staging_table_create = ("""
CREATE TABLE IF NOT EXISTS "review_staging" (

"review_id" character varying (400),
"business_id" character varying (200),
cool character varying (200),
date timestamp,
funny int,
stars decimal (18,2),
text character varying (max),
useful int,
user_id character varying (200)
)""")

#Create Fact and Dimension Tables
business_location_create= (""" CREATE TABLE IF NOT EXISTS business_location(
"business_id" character varying (200),
name character varying (200),
"city" character varying (100),
"address" character varying (600),
latitude decimal(10,8),
longitue decimal (11,8),
postal_code character varying (50),
state character(10))

""")

business_category_create= (""" CREATE TABLE IF NOT EXISTS business_category(
category_id int IDENTITY (0,1),
category_name character varying (500))

""")

business_operation_create = (""" CREATE TABLE IF NOT EXISTS business_operation(
"business_id" character varying (200),
review_count int,
stars decimal (18,2),
is_open character varying (10))

""")

review_date_create = (""" CREATE TABLE IF NOT EXISTS review_date(
date timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int)

""")

review_text_create =  (""" CREATE TABLE IF NOT EXISTS review_text(
"review_id" character varying (400),
"business_id" character varying (200),
cool character varying (200),
funny int,
stars decimal (18,2),
text character varying (max),
useful int)
""")

business_review_create =  (""" CREATE TABLE IF NOT EXISTS business_review(
identity_id int IDENTITY(0,1),
"business_id" character varying (200),
"review_id" character varying (400),
date timestamp)

""")


# COPY STAGING TABLES
staging_business_copy = (""" 

copy business_staging from 's3://projectforudacity/yelp_academic_dataset_business.json'

credentials 'aws_iam_role={}'

json 'auto';

""").format(config.get("IAM_ROLE", "ARN"))

staging_review_copy = (""" 

copy review_staging from 's3://projectforudacity/yelp_academic_dataset_review.json'

credentials 'aws_iam_role={}'

 json 'auto';

""").format(config.get("IAM_ROLE", "ARN"))

# Insert tables

business_location_insert = ("""
Insert into business_location(
"business_id", 
name,
"city" ,
"address",
latitude ,
longitue ,
postal_code ,
state )
select distinct 
"business_id", 
name,
"city" ,
"address",
latitude ,
longitue ,
postal_code ,
state
from business_staging
""")
##################category#########################
business_category_insert = (
"""
Insert into business_category(
"business_id", 
name,
"city" ,
"address",
latitude ,
longitue ,
postal_code ,
state )
select distinct 
"business_id", 
name,
"city" ,
"address",
latitude ,
longitue ,
postal_code ,
state
from business_staging

"""

)
business_operation_insert = ("""
Insert into business_operation(

"business_id" ,
review_count,
stars ,
is_open )
select distinct 
"business_id" ,
review_count,
stars ,
is_open

from business_staging
""")


review_date_insert = (""" INSERT INTO review_date(
date ,
hour ,
day ,
week ,
month ,
year ,
weekday) 

SELECT DATE,
extract (hr from DATE ) as hour,
extract (d from  DATE ) as day,
extract (w from  DATE ) as week,
extract (mon from  DATE ) as month,
extract (yr from  DATE ) as year,
extract (dw from  DATE ) as weekday 
from review_staging

""") 

review_text_insert = ("""INSERT INTO review_text(
"review_id" ,
"business_id" ,
cool ,
funny ,
stars ,
text ,
useful )

select distinct 
"review_id" ,
"business_id" ,
cool ,
funny ,
stars ,
text ,
useful
from review_staging
""")

business_review_insert = ("""
INSERT INTO business_review(
date ,
"business_id" ,
"review_id" 
 )

select r.date, b.business_id, review_id from 
business_staging b inner join review_staging r on b.business_id = r.business_id
""")
    
drop_table_queries = [business_staging_table_drop,review_staging_table_drop,
business_location_drop, 
business_category_drop , 
business_operation_drop, 
review_date_drop ,
review_text_drop ,
business_review_drop ]

create_staging_table_queries = [business_staging_table_create,review_staging_table_create]
create_fact_dimension_table_queries = [business_location_create,business_category_create,business_operation_create,review_date_create,review_text_create, business_review_create]
copy_table_queries = [staging_business_copy,staging_review_copy ]
insert_table_queries = [business_location_insert,business_operation_insert, review_date_insert,review_text_insert,business_review_insert  ]