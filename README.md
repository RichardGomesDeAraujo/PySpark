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
- [**Variable Date**](README.md#Variables-Date)

---

### Table Visualization
Visualization using SQL commands:
```sql
%sql
select * from Tabela1
```

Visualization using spark.sql commnads:
```sql
spark.sql(“””
select
  *
from
  Tabela1 a
left join
  Tabela2 b
  on a.coluna = b.coluna
“””).createOrReplaceTempView(“TblUnida”)
```
```sql
spark.sql(“select * from TblUnida”).display()
```
###### [⏪](README.md#Index)
<p>  <br>
  </p>

---

### Creating a Dataframe
Command to create and to visualize the Dataframe
```sql
df = spark.sql(“select * from TblUnida”)
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
cache table TblUnida
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
#Buscar o dia atual
  dt_dia = datetime.now() - timedelta (days=1)
#Buscar o primeiro dia do mês
  primeiro_dia_mes = datetime.strptime (dt_dia.strftime (‘%Y-%m-01'), ‘%Y-%m-%d’)
#Buscar último dia do mês
  ultimo_dia_mes = (primeiro_dia_mes + relativedelta (months = 1)) - timedelta (days = 1)
#Formatar qualquer uma das variáveis para o formato: YYYY-mm-dd
  primeiro_dia_mês = primeiro_dia_mes.strftime(“%Y-%m-%d”)
```

###### [⏪](README.md#Index)
<p>  <br>
  </p>

---
