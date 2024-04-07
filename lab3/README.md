# Практическая работа №3. Знакомство с пакетом arrow
Скаев Сармат, БИСО-02-20

## Ход работы

`Перед началом работы подключим пакет arrow`

``` r
library(dplyr)
```

    Warning: пакет 'dplyr' был собран под R версии 4.3.2


    Присоединяю пакет: 'dplyr'

    Следующие объекты скрыты от 'package:stats':

        filter, lag

    Следующие объекты скрыты от 'package:base':

        intersect, setdiff, setequal, union

``` r
library(tidyverse)
```

    Warning: пакет 'tidyverse' был собран под R версии 4.3.2

    Warning: пакет 'ggplot2' был собран под R версии 4.3.2

    Warning: пакет 'tidyr' был собран под R версии 4.3.2

    Warning: пакет 'readr' был собран под R версии 4.3.2

    Warning: пакет 'forcats' был собран под R версии 4.3.2

    Warning: пакет 'lubridate' был собран под R версии 4.3.2

    ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
    ✔ forcats   1.0.0     ✔ readr     2.1.4
    ✔ ggplot2   3.4.4     ✔ stringr   1.5.0
    ✔ lubridate 1.9.3     ✔ tibble    3.2.1
    ✔ purrr     1.0.2     ✔ tidyr     1.3.0

    ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ dplyr::filter() masks stats::filter()
    ✖ dplyr::lag()    masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors

``` r
library(arrow)
```

    Warning: пакет 'arrow' был собран под R версии 4.3.3


    Присоединяю пакет: 'arrow'

    Следующий объект скрыт от 'package:lubridate':

        duration

    Следующий объект скрыт от 'package:utils':

        timestamp

``` r
library(lubridate)
df <- arrow::open_dataset("./tm_data.pqt")
```

## Задание 1

## Важнейшие документы с результатами нашей исследовательской деятельности в области создания вакцин скачиваются в виде больших заархивированных дампов. Один из хостов в нашей сети используется для пересылки этой информации – он пересылает гораздо больше информации на внешние ресурсы в Интернете, чем остальные компьютеры нашей сети. Определите его IP-адрес.

``` r
leaked <- df %>%
    select(src, dst, bytes) %>%
    filter(!str_detect(dst, "1[2-4].*")) %>%
    group_by(src) %>%
    summarise(bytes_amount = sum(bytes)) %>%
    arrange(desc(bytes_amount)) %>%
    collect()
leaked %>% head(1)
```

    # A tibble: 1 × 2
      src          bytes_amount
      <chr>             <int64>
    1 13.37.84.125   5765792351

## Задание 2

## Другой атакующий установил автоматическую задачу в системном планировщике cron для экспорта содержимого внутренней wiki системы. Эта система генерирует большое количество трафика в нерабочие часы, больше чем остальные хосты. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из предыдущей задачи.

``` r
traficByTime  <- df %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
traficByTime  %>% filter(src != '13.37.84.125') %>% head(1)
```

    # A tibble: 1 × 2
      src         bytes_amount
      <chr>              <int>
    1 12.55.77.96    194447613

``` r
notNormalTraffic  <- df %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
notNormalTraffic  %>% filter(src != '13.37.84.125') %>% head(1)
```

    # A tibble: 1 × 2
      src         bytes_amount
      <chr>              <int>
    1 12.55.77.96    194447613

## Задание 3

## Еще один нарушитель собирает содержимое электронной почты и отправляет в Интернет используя порт, который обычно используется для другого типа трафика. Атакующий пересылает большое количество информации используя этот порт, которое нехарактерно для других хостов, использующих этот номер порта. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей из предыдущих задач.

``` r
portsTraffic  <- df |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% group_by(port) %>% summarise(average_port_traffic = mean(bytes_ip_port)) %>% arrange(desc(average_port_traffic)) |> collect()

maxPortsTraffic  <- df |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% collect() %>% group_by(port) %>% top_n(1, bytes_ip_port) %>% arrange(desc(bytes_ip_port))

merged_df <- merge(maxPortsTraffic , portsTraffic , by = "port")

notNormalIpTraffic  <- merged_df %>% mutate(average_anomaly = bytes_ip_port/average_port_traffic) %>% arrange(desc(average_anomaly)) %>% head(1)
notNormalIpTraffic 
```

      port         src bytes_ip_port average_port_traffic average_anomaly
    1  124 12.30.96.87        281993             15641.06        18.02902
