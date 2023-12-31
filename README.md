<img src="Logo.png" align="Center" alt="Hands On Data" style="height: 100px; width:115px;"/>

<p>  <br>
  </p>
  
# Useful PySpark commands for queries and learning 
<p>  <br>
  </p>

###### by [Richard Gomes de Araújo](https://github.com/RichardGomesDeAraujo) - 01/07/2023
[![Github Badge](https://img.shields.io/badge/-Github-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/RichardGomesDeAraujo)](https://github.com/RichardGomesDeAraujo)
[![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/richardaraujoanalistadedados/)](https://www.linkedin.com/in/richardaraujoanalistadedados/)
[![Youtube Badge](https://img.shields.io/badge/-YouTube-ff0000?style=flat-square&labelColor=ff0000&logo=youtube&logoColor=white&link=https://www.youtube.com/channel/UCc_jlqHut_GkXc8ahgQHOOw)](https://www.youtube.com/channel/UCc_jlqHut_GkXc8ahgQHOOw)
<p>  <br>
  </p>
  
# Index
- [**Table Visualization**](README.md#Table-Visualization)
- [**Creating a Dataframe**](README.md#Creating-a-Dataframe)
- [**Table Schema**](README.md#Table-Schema)
- [**Cache**](README.md#Cache)
- [**Variable Date**](README.md#Variable-Date)
- [**Creating a Little Table**](README.md#Creating-a-Little-Table)
- [**Special Commands in PySpark**](README.md#Special-Commands-in-PySpark)

---

### Table Visualization
Visualization using SQL commands:
```sql
%sql
select * from Table1
```

Visualization using spark.sql commnads:
```sql
spark.sql(“””
select
  *
from
  Table1 a
left join
  Table2 b
  on a.column = b.column
“””).createOrReplaceTempView(“TblUnion”)
```
```sql
spark.sql(“select * from TblUnion”).display()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Creating a Dataframe
Command to create and to visualize the Dataframe
```sql
df = spark.sql(“select * from TblUnion”)
display(df)
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Table Schema
Command to view the table schema from a Dataframe
```python
df.printSchema()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Cache
Command to create a table cache if necessary to view the table in a faster way
```sql
%sql
cache table TblUnion
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Variable Date
Command to create a variable date
  - Example 1:
```python
datavariavel = spark.sql(“select to_date(date_trunc(‘MM’, add_months(current_date(), -10)))”).collect() [0][0]
print(datavariavel)
```
  How to use in the spark.sql command:
    
  Write "f" before the quotation marks from the spark.sql
```sql
spark.sql(f"""
select
  *
from
  TblUnida
where
  DtContrato >= ‘{datavariavel}’
""").createOrReplaceTempView("TblFiltered")
```
  - Example 2:
    
    Some possibilities to find variable date
```python
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
#Search for the current date
  dt_day = datetime.now() - timedelta (days=1)
#Searche for the first day of the month
  first_day_month = datetime.strptime (dt_day.strftime (‘%Y-%m-01'), ‘%Y-%m-%d’)
#Search for the last day of the month
  last_day_month = (first_day_month + relativedelta (months = 1)) - timedelta (days = 1)
#Format date like: YYYY-mm-dd
  first_day_month = first_day_month.strftime(“%Y-%m-%d”)
```

###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Creating a Little Table
Command in Python to create a support table if necessary
```python
data = [
(1,"Park","Null"),
(2,"Square","Null"),
(3,"Block","Null"),
(4,"Street","Null"),
(5,"Mount","Null")
]
columns = ["IdType","Type","Qtd"]
spark.createDataFrame(data,columns).createOrReplaceTempView("TbStreetTypes")
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Special Commands in PySpark
Differents commands in PySpark compared to SQL Server

|SQL Server | PySpark |
|---| ---|
|getdate() | current_date()|
|convert() | cast() |
|++ | concat() |


###### [⏪](README.md#Index)
<p>  <br>
  </p>

---
