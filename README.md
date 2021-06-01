# etl-vouchers
Vouchers ETL Service


```
poetry run invoke etl --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv

```


```
poetry run invoke top-customers --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv

```

```
poetry run invoke unused-barcodes --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv

```