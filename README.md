# etl-vouchers
Vouchers ETL Service


### Project Structure

```
.
├── datasets
│   ├── barcodes.1622544683.csv
│   └── orders.1622544686.csv
├── etl_vouchers
│   ├── etl.py - the main file that does the ETL piece.
│   ├── exceptions.py
│   ├── __init__.py
│   ├── statistic.py - this file is responsbile for extra analysis of the input data.
│   ├── utils.py - some utils, you know...
│   └── validator.py - validations for input are here
├── LICENSE
├── poetry.lock
├── pyproject.toml
├── README.md
├── ERD.pdf - entity relationship model for orders, customers and barcodes
├── tasks.py - here stored all the cli tasks, that's the entry point.
└── tests - tests are here

```

### Installation

#### [Option 1] If you have/want/use poetry.

1. Install poetry - https://python-poetry.org/docs/
2. Navigate into the root of the project
3. `poetry env use 3.7.2`
4. `poetry install`
5. `poetry run pytest`

#### [Option 2] If don't want to mess with poetry. 
Note, I haven't really tested it, and for a reason it fails on my laptop. Something related to setuptools.

1. `pip install -e .`
2. `poetry run pytest`
3. Done.

### How to Run

- Run ETL
    ```
    poetry run invoke etl --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv
    
    ```
    If you want to generate vouchers that does not contain any barcodes (which really does not make sense to me)
    ```
    poetry run invoke etl --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv --allow-useless
    
    ```

- Show top 5 customers
  ```
  poetry run invoke top-customers --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv
  
  ```

- Show unused barcodes
  ```
  poetry run invoke unused-barcodes --orders=./datasets/orders.1622544686.csv --barcodes=./datasets/barcodes.1622544683.csv
  
  ```
