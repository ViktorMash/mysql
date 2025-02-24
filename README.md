### Шаги выполнения

1. Развернул MySQL контейнер в Docker и запустил его
2. Подключился к БД MySQL через DBeaver
3. Создал БД test_db и таблицу air_quality (файл 1_DDL.sql)
4. Разрешил загрузку из локальных источников allowLoadLocalInfile:true
5. Попытался загрузить данные нативным способом через запрос (файл 2_NativeLoad.sql), получил массу ошибок, т.к. данные неконсистентны и требуют предварительной очистки перед загрузкой. 
   Принято решение написать обработчик в Python который приведет данные к нужному типу, очистит от мусора и выполнит загрузку в MySQL
6. Скрипт находится в **/DataLoader/main.py** Для анализа данных использовал **pandas**, для подключения к mysql - **mysql-connector-python** и **sqlalchemy**



### Вопрос 1
Найдите все локации (Geo Place Name), где среднее значение PM2.5 (Name = 'Fine particles (PM 2.5)') превышает 12.0, 
и количество измерений больше 100. Сколько таких локаций?

**Ответ:** 23 локации

```sql
    select geo_place_name,
           count(*) as measures_cnt,
           avg(data_value) as avg_value
      from test_db.air_quality
     where lower(name) = 'fine particles (pm 2.5)'
  group by geo_place_name
    having count(*) > 100
       and avg(data_value) > 12.0
```

| № | Район | Кол-во измерений | Среднее значение |
| --: | :--- | --: | --: |
1 | Downtown - Heights - Slope | 218 | 725.564220 |
2 | Park Slope and Carroll Gardens (CD6) | 191 | 862.554974 |
3 | Lower East Side and Chinatown (CD3) | 187 | 844.048128 |
4 | Riverdale and Fieldston (CD8) | 201 | 778.835821 |
5 | Sunset Park | 116 | 75.120690 |
6 | South Ozone Park and Howard Beach (CD10) | 177 | 906.768362 |
7 | Queens Village (CD13) | 189 | 807.640212 |
8 | Greenpoint and Williamsburg (CD1) | 167 | 974.700599 |
9 | Mott Haven and Melrose (CD1) | 198 | 779.176768 |
10 | Morrisania and Crotona (CD3) | 189 | 859.338624 |
11 | Northeast Bronx | 223 | 679.049327 |
12 | Upper East Side-Gramercy | 203 | 757.817734 |
13 | Bayside and Little Neck (CD11) | 184 | 854.456522 |
14 | Bedford Stuyvesant - Crown Heights | 131 | 1226.083969 |
15 | Greenpoint | 134 | 1122.873134 |
16 | Gramercy Park - Murray Hill | 126 | 1155.126984 |
17 | Bedford Stuyvesant (CD3) | 129 | 1146.441860 |
18 | Washington Heights | 130 | 1221.546154 |
19 | Upper West Side | 125 | 1219.216000 |
20 | Upper West Side (CD7) | 124 | 1167.274194 |
21 | Pelham - Throgs Neck | 127 | 1202.149606 |
22 | Morris Park and Bronxdale (CD11) | 124 | 1204.370968 |
23 | Southeast Queens | 129 | 1196.813953 |



### Вопрос 2

Сопоставьте данные по PM2.5 (Name = 'Fine particles (PM 2.5)') и NO2 (Name = 'Nitrogen dioxide (NO2)') для одних и тех же локаций и дат. 
Сколько есть совпадающих измерений (когда в один день в одной локации измеряли оба показателя)?

**Ответ:**  2156 измерений

```sql
    with
    pm25 as (
        select distinct geo_place_name, start_date
        from test_db.air_quality
        where lower(name) = 'fine particles (pm 2.5)'
        and start_date is not null
    ),
    no2 as (
        select distinct geo_place_name, start_date
        from test_db.air_quality
        where lower(name) = 'nitrogen dioxide (no2)'
        and start_date is not null
    )
    select count(*)
    from pm25
    where exists ( select 1
                     from no2
                    where pm25.geo_place_name = no2.geo_place_name
                      and pm25.start_date = no2.start_date);    
```



### Вопрос 3

Найдите количество локаций, где среднее значение озона (Name = 'Ozone (O3)') выше, 
чем общее среднее значение озона по всем локациям

**Ответ:** 58 локаций

```sql
    with avg_ozone as (
        select avg(data_value) as overall_avg
        from test_db.air_quality
        where lower(name) = 'ozone (o3)'
    )
      select geo_place_name, avg(data_value) as loc_avg
        from test_db.air_quality
       where lower(name) = 'ozone (o3)'
    group by geo_place_name
      having avg(data_value) > (select overall_avg from avg_ozone);    
```

| № | Район | Среднее значение |
| --: | :--- | ---: |
1 | Greenwich Village - SoHo | 6.50 |
2 | Ridgewood - Forest Hills | 5.01 |
3 | St. George and Stapleton (CD1) | 5.72 |
4 | Highbridge and Concourse (CD4) | 5.98 |
5 | East New York | 4.77 |
6 | Long Island City and Astoria (CD1) | 5.25 |
7 | Flatlands and Canarsie (CD18) | 5.35 |
8 | Williamsbridge and Baychester (CD12) | 5.37 |
9 | Southeast Queens | 6.10 |
10 | Greenpoint | 5.41 |
11 | Long Island City - Astoria | 4.90 |
12 | Parkchester and Soundview (CD9) | 6.12 |
13 | Lower East Side and Chinatown (CD3) | 5.35 |
14 | Sunset Park (CD7) | 6.14 |
15 | Fresh Meadows | 5.88 |
16 | Willowbrook | 5.68 |
17 | Downtown - Heights - Slope | 6.41 |
18 | Midtown (CD5) | 5.24 |
19 | Manhattan | 5.95 |
20 | Hunts Point and Longwood (CD2) | 4.94 |
21 | Washington Heights | 5.95 |
22 | Mott Haven and Melrose (CD1) | 5.22 |
23 | Rego Park and Forest Hills (CD6) | 5.49 |
24 | Morris Park and Bronxdale (CD11) | 5.22 |
25 | Brooklyn | 6.11 |
26 | Queens Village (CD13) | 6.91 |
27 | South Beach - Tottenville | 5.48 |
28 | Kew Gardens and Woodhaven (CD9) | 5.12 |
29 | Chelsea-Village | 6.33 |
30 | Flatbush and Midwood (CD14) | 5.26 |
31 | Bensonhurst - Bay Ridge | 5.76 |
32 | Ridgewood and Maspeth (CD5) | 5.27 |
33 | Crown Heights and Prospect Heights (CD8) | 6.16 |
34 | Upper West Side (CD7) | 7.26 |
35 | Bensonhurst (CD11) | 6.13 |
36 | Elmhurst and Corona (CD4) | 4.84 |
37 | Upper East Side | 5.48 |
38 | Coney Island - Sheepshead Bay | 4.98 |
39 | South Bronx | 4.95 |
40 | Chelsea - Clinton | 5.34 |
41 | Port Richmond | 7.60 |
42 | Greenwich Village and Soho (CD2) | 6.48 |
43 | Jamaica and Hollis (CD12) | 5.38 |
44 | Riverdale and Fieldston (CD8) | 5.72 |
45 | Belmont and East Tremont (CD6) | 5.70 |
46 | Jamaica | 5.56 |
47 | Sunset Park | 5.04 |
48 | Upper East Side-Gramercy | 6.40 |
49 | Bay Ridge and Dyker Heights (CD10) | 5.25 |
50 | Stuyvesant Town and Turtle Bay (CD6) | 4.79 |
51 | Washington Heights and Inwood (CD12) | 5.03 |
52 | Park Slope and Carroll Gardens (CD6) | 6.72 |
53 | Bronx | 5.42 |
54 | Brownsville (CD16) | 6.08 |
55 | Bushwick (CD4) | 5.96 |
56 | Throgs Neck and Co-op City (CD10) | 5.79 |
57 | Central Harlem - Morningside Heights | 5.08 |
58 | South Beach and Willowbrook (CD2) | 4.89 |



### Вопрос 4

Для каждой локации найдите максимальное значение PM2.5 и его ранг (от наибольшего к наименьшему) среди всех локаций. 
Сколько локаций имеют ранг <=5 (учитывая возможные одинаковые значения)?

Ответ: 21 локация

```sql
    with 
    tbl as (
        select geo_place_name,
               max(data_value) as max_pm25,
               dense_rank() over (order by max(data_value) desc) as rnk
          from test_db.air_quality
         where lower(name) = 'fine particles (pm 2.5)'
         group by geo_place_name
    )
    select geo_place_name, max_pm25, rnk
      from tbl
     where rnk <= 5
    order by rnk, geo_place_name;    
```

| № | geo_place_name | max_pm25 | rnk |
|---|---|---|---|
| 1 | Greenpoint | 1499.00 | 1 |
| 2 | Greenpoint and Williamsburg (CD1) | 1499.00 | 1 |
| 3 | Morris Park and Bronxdale (CD11) | 1499.00 | 1 |
| 4 | Northeast Bronx | 1499.00 | 1 |
| 5 | Park Slope and Carroll Gardens (CD6) | 1499.00 | 1 |
| 6 | Southeast Queens | 1499.00 | 1 |
| 7 | Washington Heights | 1499.00 | 1 |
| 8 | Bayside and Little Neck (CD11) | 1498.00 | 2 |
| 9 | Bedford Stuyvesant - Crown Heights | 1498.00 | 2 |
| 10 | Bedford Stuyvesant (CD3) | 1498.00 | 2 |
| 11 | Gramercy Park - Murray Hill | 1498.00 | 2 |
| 12 | Morrisania and Crotona (CD3) | 1498.00 | 2 |
| 13 | Riverdale and Fieldston (CD8) | 1498.00 | 2 |
| 14 | Lower East Side and Chinatown (CD3) | 1497.00 | 3 |
| 15 | Upper West Side | 1497.00 | 3 |
| 16 | Mott Haven and Melrose (CD1) | 1495.00 | 4 |
| 17 | Pelham - Throgs Neck | 1495.00 | 4 |
| 18 | Upper East Side-Gramercy | 1495.00 | 4 |
| 19 | Upper West Side (CD7) | 1495.00 | 4 |
| 20 | Downtown - Heights - Slope | 1494.00 | 5 |
| 21 | South Ozone Park and Howard Beach (CD10) | 1494.00 | 5 |


### Вопрос 5

Сколько дней в 2023 году было зафиксировано превышение среднесуточной нормы PM2.5 (>35.5 мкг/м³) 
хотя бы в одной локации?

Ответ: 37 дней

```sql
    with t_exceed_dt as (
        select start_date, geo_place_name, avg(data_value) as avg_data
        from test_db.air_quality
        where lower(name) = 'fine particles (pm 2.5)'
        and year(start_date) = 2023
        group by start_date, geo_place_name
    )
    select distinct start_date
    from t_exceed_dt
    where avg_data > 35.5;  
```

| № | start_date |
|---|---|
| 1 | 2023-12-18 |
| 2 | 2023-11-28 |
| 3 | 2023-11-15 |
| 4 | 2023-11-10 |
| 5 | 2023-10-31 |
| 6 | 2023-10-29 |
| 7 | 2023-10-27 |
| 8 | 2023-10-15 |
| 9 | 2023-10-14 |
| 10 | 2023-09-13 |
| 11 | 2023-09-11 |
| 12 | 2023-09-08 |
| 13 | 2023-09-06 |
| 14 | 2023-09-01 |
| 15 | 2023-07-29 |
| 16 | 2023-07-28 |
| 17 | 2023-07-18 |
| 18 | 2023-07-05 |
| 19 | 2023-06-23 |
| 20 | 2023-06-20 |
| 21 | 2023-06-18 |
| 22 | 2023-05-25 |
| 23 | 2023-05-10 |
| 24 | 2023-04-29 |
| 25 | 2023-04-06 |
| 26 | 2023-03-24 |
| 27 | 2023-03-07 |
| 28 | 2023-03-06 |
| 29 | 2023-03-03 |
| 30 | 2023-02-28 |
| 31 | 2023-02-25 |
| 32 | 2023-02-18 |
| 33 | 2023-01-25 |
| 34 | 2023-01-16 |
| 35 | 2023-01-15 |
| 36 | 2023-01-14 |
| 37 | 2023-01-10 |