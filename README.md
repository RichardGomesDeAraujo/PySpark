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
  
# Índice
- [**Table Visualization**](README.md#Table-Visualization)
- [**Creating a Dataframe**](README.md#Creating-a-Dataframe)

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
###### [⏪](README.md#Índice)
<p>  <br>
  </p>
  
### Creating a Dataframe

###### [⏪](README.md#Índice)
