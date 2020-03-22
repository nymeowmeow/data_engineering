# Immigration Data Modelling

### 1. Project Description

The aim of this project is to build a data processing pipeline, that import i94 immigration related data from various data source, transform and clean the data before storing to a data warehouse designed for immigration data.

The data warehouse will support various queries to allow user to gain insight to immigration related problems, such as travel purpose, vsia type and other more complex queries.

### 2. Data Source

Data Sources in the project includes the following:
* I94 Immigration data sas7bdat files
* airport-codes_csv.csv
* GlobalLandTemperaturesByCity.csv
* I94_SAS_Labels_Descriptions.SAS
* us-cities-demographics.csv

### 3 Data Schema

#### State Table
~~~
CREATE TABLE IF NOT EXISTS dimState (
code varchar PRIMARY KEY,
name varchar(32)
);
~~~

#### TransportMode Table
~~~
CREATE TABLE IF NOT EXISTS dimTransportMode (
code int PRIMARY KEY,
mode varchar(64)
);
~~~

#### Country Table
~~~
CREATE TABLE IF NOT EXISTS dimCountry (
code int PRIMARY KEY,
name varchar(64)
);
~~~

#### VisaCategory Table
~~~
CREATE TABLE IF NOT EXISTS dimVisaCategory (
code int PRIMARY KEY,
type varchar(64)
);
~~~

#### dimCity Table
~~~
CREATE TABLE IF NOT EXISTS dimCity (
city bigint PRIMARY KEY,
name varchar(128),
state varchar,
median_age float,
male_population bigint,
female_population bigint,
total_population bigint,
verterans_population bigint,
foreign_born bigint,
average_household_size float,
native_population bigint,
asian_population bigint,
black_population bigint,
latino_population bigint,
white_population bigint,
FOREIGN KEY(state) REFERENCES dimState(code)
);
~~~

#### dimWeather 
~~~
CREATE TABLE IF NOT EXISTS dimWeather (
city bigint NOT NULL REFERENCES dimCity(city),
"date" date NOT NULL,
average_temperature float,
PRIMARY KEY(city, date)
);
~~~

#### dimAirPort Table
~~~
CREATE TABLE IF NOT EXISTS dimAirport (
id varchar PRIMARY KEY,
type varchar(128),
name varchar(128) NOT NULL,
elevation int,
country int REFERENCES dimCountry(code),
state varchar REFERENCES dimState(code),
municipality varchar(128),
gps_code varchar(64),
iata_code varchar(64),
local_code varchar(64),
latitude numeric(9,6),
longitude numeric(9,6)
);
~~~

#### factImmigration Table
~~~
CREATE TABLE IF NOT EXISTS factImmigration (
cicid bigint PRIMARY KEY,
year int,
month int,
citizen int REFERENCES dimCountry(code),
resident int REFERENCES dimCountry(code),
transport_mode int REFERENCES dimTransportMode(code),
arrival_state varchar REFERENCES dimState(code),
visa_category int REFERENCES dimVisaCategory(code),
occupation varchar(128),
birth_year int,
gender char(1),
age int,
visa_type varchar(32),
port_city bigint REFERENCES dimCity(city),
arrival_date date,
departure_date date
);
~~~

### 4 Data Processing
* data from various data sources are first into spark data frame, then clean it and format into proper format ready to be saved to redshift database
* Then the data in spark data frame are written into redshift database.

### 5 Data dictionary 

The data dictionary for the Fact and Dimension tables are as below

##### Fact Table: factImmigration

| Column |  Description | Source |
| ------ | ------------ | ------ |
| cicid | unique identifier in I94 immigration data set | I94 immigration sas7dat files |
| year| year of arrival| I94 immigration sas7dat files |
| month | month of arrival | I94 immigration sas7dat files |
| citizen | country of citizenship, id which is used to join with dimCountry Table | dimCountry table |
| resident | country of residence, id which is used to join with dimCountry table | dimCountry table |
| transport_mode | transport mode id which is used to join with dimTransportMode table | dimTransportMode table |
| arrival_state | arrival state code used to join with dimState table | dimState table |
| visa_category | visa category, whcih is used to join with dimVisaCategory table | dimVisaCategory table |
| occupation | occupation | I94 immigration sas7dat files |
| birth_year | year of birth | I94 immigration sas7dat files |
| gender | gender, M or F | I94 immigration sas7dat files |
| age| age | I94 immigration sas7dat files |
| visa_type | visa type | I94 immigration sas7dat files |
| port_city | port of arrival, city id which is used to join with dimCity table | dimCity table |
| arrival_date | date of arrival | I94 immigration sas7dat files |
| departure_date | date of departure | I94 immigration sas7dat files |

##### Dimension Table: DimWeather

| Column | Description | Source |
| ------ | ----------- | ------ |
| city | city id used to join with dimCity | dimCity table |
| date | date of the temperature recording | GlobalLandTemperatureByCities.csv |
| average_temperature | average temperature for a given date | GlobalLandTemperatureByCities.csv |

##### Dimension Table: dimVisaCategory

| Column | Description | Source |
| ------ | ----------- | ------ |
| code | visa category code | I94 immigration sas7dat files |
| type | visa category type | I94 immigration sas7dat files |


##### Dimension Table: DimAirPort

| column | Description | Source |
| ------ | ----------- | ------ |
| id | airport unique id | airport-codes_csv.csv |
| type | string | airport-codes_csv.csv |
| name | string | airport-codes_csv.csv |
| elevation | integer | airport-codes_csv.csv |
| country | country code which is used to join with dimCountry table | dimCountry table |
| state | state code of the airport, which is used to join with dimState table | dimState table |
| municipality | municpal of the airport | airport-codes_csv.csv |
| gps_code | gps identifier of the airport | airport-codes_csv.csv |
| iata_code |  IATA code of the airport | airport-codes_csv.csv |
| local_code | local identifier of the airport | airport-codes_csv.csv |
| latitude | latitude of the airport | airport-codes_csv.csv |
| longitude | longitude of the airport |  airport-codes_csv.csv |

##### Dimension Table: DimCountry

| Column | Description | Source |
| ------ | ----------- | ------ |
| code   | country code | I94_SAS_Labels_Descriptions.SAS |
| name   | country name | I94_SAS_Labels_Descriptions.SAS |


##### Dimension Table: DimCity

| column | Description | Source |
| ------ | ----------- | ------ |
| city | city id, which is generated automatically and serve as primary key | auto generated |
| name | name of the city | us-cities-demographics.csv |
| state | state code, which is used to join with dimState table | dimState table |
| median_age | median age of the given city | us-cities-demographics.csv |
| male_population | male population of the given city | us-cities-demographics.csv |
| female_population | female population of the given city | us-cities-demographics.csv |
| total_population | total population of the given city | us-cities-demographics.csv |
| verterans_population | population of verterans in the given city | us-cities-demographics.csv |
| foreign-born | population of foreign born in the given city | us-cities-demographics.csv |
| average_household_size | average household size in the given city | us-cities-demographics.csv |
| native_population | native population in the given city | us-cities-demographics.csv |
| asian_population | asian population in the given city | us-cities-demographics.csv |
| black_population | black population in the given city | us-cities-demographics.csv |
| latino_population | latino population in the given city | us-cities-demographics.csv |
| white_population | white population in the given city | us-cities-demographics.csv |
 
##### Dimension Table: DimState

| column | Description | Source |
|------- | ----------- | ------ |
| code   | state code    | I94 immigration sas7dat files |
| name   | state name    | I94 immigration sas7dat files |

##### Dimension Table: DimTransportMode

| column | Description | Source |
| ------ | ----------- | ------ |
| code | transportation mode id | I94 immigration sas7dat files |
| mode | transporttation mode  | I94 immigration sas7dat files |

#### 6 Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.

  The aim of the project is to support queries of I94 immigration data. Apache Spark was used to extract data from various data source, clean up the data and load the data into redshift database.
  
  Spark was used, because it can be run in a cluster, and as data volume increase we can add more machines to handle the volume.
  The data was stored in redshift becasue of its easy of use, setup and performance. Also it support sql queries that most users is already familar with.
  
  
* Propose how often the data should be updated and why.

  Since the I94 data is monthly, it will be appropriate to incrementally udpate the data monthly.
  
  
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 
   If the data was to be increased by 100x fold, we can add more nodes to the spark cluster(EMR) to handle the etl portion. On top of that we should also increase the cluster size of the redshift database backend, employ more powerful machines with more memory and disk space, so it can handle the anticipated increase in data volume.
   
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 
   If the data needs to be updated daily by 7am every day, then we should use automated workflow solution like airflow. which will send out notification (via slack channel, email or other means) to notify if the job has been successful completed, or need attention by certain time so manual intervention can kick in to ensure the updated will meet the 7am deadline each day.
   
   
 * The database needed to be accessed by 100+ people.
 
   If the number of users accessing the data has to be accessed by 100+ people, we can upgrade the tier of the redshift database server. Using more powerful machines, more cpu's and memory to handle the increase in user population.