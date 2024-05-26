# Практическая работа №5. Работа с Clickhouse
Скаев Сармат, БИСО-02-20

## Цель работы

1.  Изучить возможности СУБД Clickhouse для обработки и анализ больших
    данных

## Ход работы

# Подключение к Clickhouse

``` r
library(tidyverse)
```

    Warning: пакет 'tidyverse' был собран под R версии 4.3.2

    Warning: пакет 'ggplot2' был собран под R версии 4.3.2

    Warning: пакет 'tidyr' был собран под R версии 4.3.2

    Warning: пакет 'readr' был собран под R версии 4.3.2

    Warning: пакет 'dplyr' был собран под R версии 4.3.2

    Warning: пакет 'forcats' был собран под R версии 4.3.2

    Warning: пакет 'lubridate' был собран под R версии 4.3.2

    ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
    ✔ dplyr     1.1.3     ✔ readr     2.1.4
    ✔ forcats   1.0.0     ✔ stringr   1.5.0
    ✔ ggplot2   3.4.4     ✔ tibble    3.2.1
    ✔ lubridate 1.9.3     ✔ tidyr     1.3.0
    ✔ purrr     1.0.2     
    ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ dplyr::filter() masks stats::filter()
    ✖ dplyr::lag()    masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors

``` r
library(dplyr)
library(ClickHouseHTTP)
```

    Warning: пакет 'ClickHouseHTTP' был собран под R версии 4.3.3

``` r
library(DBI)
```

    Warning: пакет 'DBI' был собран под R версии 4.3.2

``` r
library(lubridate)
con <- dbConnect(
   ClickHouseHTTP::ClickHouseHTTP(), 
   host="rc1d-sbdcf9jd6eaonra9.mdb.yandexcloud.net",
                      port=8443,
                      user="student24dwh",
                      password = "DiKhuiRIVVKdRt9XON",
                      db = "TMdata",
   https=TRUE, ssl_verifypeer=FALSE)
Clickhousedata <- dbReadTable(con, "data")
dff <- dbGetQuery(con, "SELECT * FROM data LIMIT 100")
```

## Важнейшие документы с результатами нашей исследовательской деятельности в области создания вакцин скачиваются в виде больших заархивированных дампов. Один из хостов в нашей сети используется для пересылки этой информации – он пересылает гораздо больше информации на внешние ресурсы в Интернете, чем остальные компьютеры нашей сети. Определите его IP-адрес.

``` r
leak <- dff  %>% select(src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
leak %>% head(1)
```

    # A tibble: 1 × 2
      src         bytes_amount
      <chr>              <int>
    1 12.46.91.98        60408

`13.37.84.125`

## Другой атакующий установил автоматическую задачу в системном планировщике cron для экспорта содержимого внутренней wiki системы. Эта система генерирует большое количество трафика в нерабочие часы, больше чем остальные хосты. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из предыдущей задачи.

``` r
df_normaltime_by_traffic_size <- dff %>% select(timestamp, src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% group_by(timestamp) %>% summarize(traffic_size = sum(bytes)) %>% arrange(desc(traffic_size))
df_normaltime_by_traffic_size %>% collect() %>% print(n = Inf)
```

    # A tibble: 1 × 2
      timestamp traffic_size
          <int>        <int>
    1        16       442208

Найдём Ip

``` r
df_traffic_no_worktime_anomaly <- dff %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
df_traffic_no_worktime_anomaly %>% filter(src != '13.37.84.125') %>% head(1)
```

    # A tibble: 0 × 2
    # ℹ 2 variables: src <chr>, bytes_amount <int>

## Еще один нарушитель собирает содержимое электронной почты и отправляет в Интернет используя порт, который обычно используется для другого типа трафика. Атакующий пересылает большое количество информации используя этот порт, которое нехарактерно для других хостов, использующих этот номер порта. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей из предыдущих задач.

``` r
average_ports_traffic <- dff |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% group_by(port) %>% summarise(average_port_traffic = mean(bytes_ip_port)) %>% arrange(desc(average_port_traffic)) |> collect()
```

    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.

``` r
max_ips_ports_traffic <- dff |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% collect() %>% group_by(port) %>% top_n(1, bytes_ip_port) %>% arrange(desc(bytes_ip_port))
```

    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.

``` r
merged_df <- merge(max_ips_ports_traffic, average_ports_traffic, by = "port")

anomaly_ip_port_traffic <- merged_df %>% mutate(average_anomaly = bytes_ip_port/average_port_traffic) %>% arrange(desc(average_anomaly)) %>% head(1)
anomaly_ip_port_traffic
```

      port          src bytes_ip_port average_port_traffic average_anomaly
    1  115 14.33.30.103         20979                14408        1.456066

`12.30.96.87`

## Зачастую в корпоротивных сетях находятся ранее зараженные системы, компрометация которых осталась незамеченной. Такие системы генерируют небольшое количество трафика для связи с панелью управления бот-сети, но с одинаковыми параметрами – в данном случае с одинаковым номером порта. Какой номер порта используется бот-панелью для управления ботами?

``` r
df2 <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes), avg(bytes), port,count(port) FROM data group by port having avg(bytes) - min(bytes) < 10 and min(bytes) != max(bytes)")
df2 %>% select(port)
```

      port
    1  124

## Иногда компрометация сети проявляется в нехарактерном трафике между хостами в локальной сети, который свидетельствует о горизонтальном перемещении (lateral movement). В нашей сети замечена система, которая ретранслирует по локальной сети полученные от панели управления бот-сети команды, создав таким образом внутреннюю пиринговую сеть. Какой уникальный порт используется этой бот сетью для внутреннего общения между собой?

``` r
df2 <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes) as anomaly, avg(bytes), port,count(port) FROM data where (src LIKE '12.%' or src LIKE '13.%' or src LIKE '14.%') and (dst LIKE '12.%' or dst LIKE '13.%' or dst LIKE '14.%') group by port order by anomaly desc limit 1")
df2 %>% select(port)
```

      port
    1  115

## В нашем трафике есть еще одна бот-сеть, которая использует очень большой интервал подключения к панели управления. Хосты этой продвинутой бот-сети не входят в уже обнаруженную нами бот-сеть. Какой порт используется продвинутой бот-сетью для коммуникации?

``` r
df2 <- dbGetQuery(con, "SELECT port, timestamp FROM data where timestamp == (select max(timestamp) from data)")
df2
```

      port    timestamp
    1   83 1.578784e+12
