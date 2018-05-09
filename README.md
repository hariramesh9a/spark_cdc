##SCD Type 2 Change data capture using Apache Spark

This is a CDC (change data capture component). 
Module needs 5 arguments. 
- Table A (Yesterday's table)
-  Table B (Todays table)
-  ID columns (Columns used in joining to perform CDC - it can be composite keys)
-  CDC Columns (Columns on which change has to be detected)
- Path (Where the output result has to be saved)

CDC is performed and the results are saved with an additional column **CDC_status** which has values **I(new record inserted), U (Record updated), D (Record deleted)**


### SCD Type 2

#### Table A Sample

|  Id |Name   |Phone   |
| ------------ | ------------ | ------------ |
|  1 | Hari  |99999   |
|  2 | Dipu  |99997   |
|  3 | Doe  |99991   |



#### Table B Sample

|  Id |Name   |Phone   |
| ------------ | ------------ | ------------ |
|  1 | Hari  |99990   |
|  2 | Dipu  |99997   |
|  4 | Jane  |99992   |

#### Ouput Sample

|  Id |Name   |Phone   |  Id_1 |Name_1   |Phone_1   |cdc_status   |
| ------------ | ------------ | ------------ |
|  1 | Hari  |99999   |1 | Hari  |99990   |U|
|  2 | Dipu  |99997   |2 | Dipu  |99997   |N|
|  3 | Doe  |99991   |Null|Null|Null|D|
|  Null | Null  |Null   |  4 | Jane  |99992   |D|

> The results are saved as parquet file in the output path mentioned.