class SqlQueries:
    creating_pre_staging_table = ("""
        DROP TABLE IF EXISTS pre_stage;
        CREATE TABLE IF NOT EXISTS pre_stage (
            "Invoice/Item Number" varchar(256),
            "Date" varchar(256),
            "Store Number" varchar(256),
            "Store Name" varchar(256),
            "Address" varchar(256),
            "City" varchar(256),
            "Zip Code" varchar(256),
            "Store Location" varchar(256),
            "County Number" varchar(256),
            "County" varchar(256),
            "Category" varchar(256),
            "Category Name" varchar(256),
            "Vendor Number" varchar(256),
            "Vendor Name" varchar(256),
            "Item Number" varchar(256),
            "Item Description" varchar(256),
            "Pack" varchar(256),
            "Bottle Volume (ml)" varchar(256),
            "State Bottle Cost" varchar(256),
            "State Bottle Retail" varchar(256),
            "Bottles Sold" varchar(256),
            "Sale (Dollars)" varchar(256),
            "Volume Sold (Liters)" varchar(256),
            "Volume Sold (Gallons)" varchar(256),
            PRIMARY KEY("Invoice/Item Number")
        );
    """)

    dropping_pre_stage_table = ("""
         DROP TABLE IF EXISTS pre_stage;
    """)

    creating_stage_table = ("""
        DROP TABLE IF EXISTS staging_csv;
        CREATE TABLE IF NOT EXISTS staging_csv (
            id                      VARCHAR(12) NOT NULL,
            date                    TIMESTAMP NOT NULL,
            store_number            INTEGER,
            store_name              VARCHAR(256),
            zip_code                INTEGER,
            county_number           INTEGER,
            county                  VARCHAR(256),
            category                INTEGER,
            category_name           VARCHAR(256),
            vendor_number           INTEGER,
            vendor_name             VARCHAR(256),
            item_number             INTEGER,
            item_description        VARCHAR(256),
            pack                    INTEGER,
            bottle_volume_ml        INTEGER,
            state_bottle_cost       INTEGER,
            state_bottle_retail     INTEGER,
            bottles_sold            INTEGER,
            sale_dollars            INTEGER,
            volume_sold             INTEGER,
            PRIMARY KEY(id)
        );
    """)

    creating_fact_sales_table = ("""
        DROP TABLE IF EXISTS sales_fact;
        CREATE TABLE IF NOT EXISTS sales_fact (
            id                      VARCHAR(12) NOT NULL,
            date                    TIMESTAMP NOT NULL,
            product_id              INTEGER,
            store_id                INTEGER,
            county_id               INTEGER,
            category_id             INTEGER,
            vendor_id               INTEGER,
            bottles_sold            INTEGER,
            sale_amount             INTEGER,
            volume_sold             INTEGER,
            PRIMARY KEY(id)
        );
    """)

    inserting_fact_sales = ("""
        SELECT id,
            date,
            item_number,
            store_number,
            county_number,
            category,
            vendor_number,
            bottles_sold,
            sale_dollars,
            volume_sold
        FROM staging_csv
    """)

    creating_products_table = ("""
        DROP TABLE IF EXISTS products_dim;
        CREATE TABLE IF NOT EXISTS products_dim (
            product_id              INTEGER NOT NULL,
            description             VARCHAR(256),
            pack                    INTEGER,
            state_bottle_cost       INTEGER,
            state_bottle_retail     INTEGER,
            bottle_volume_ml        INTEGER,
            PRIMARY KEY(product_id)
        );
    """)

    inserting_products = ("""
        SELECT item_number,
            item_description ,
            pack,
            state_bottle_cost,
            state_bottle_retail,
            bottle_volume_ml
        FROM staging_csv
    """)

    creating_categories_table = ("""
        DROP TABLE IF EXISTS categories_dim;
        CREATE TABLE IF NOT EXISTS categories_dim (
            category_id             INTEGER NOT NULL,
            name                    VARCHAR(256),
            PRIMARY KEY(category_id)
        );
    """)

    inserting_categories = ("""
        SELECT item_number,
            item_description ,
            pack,
            state_bottle_cost,
            state_bottle_retail,
            bottle_volume_ml
        FROM staging_csv
    """)

    creating_vendors_table = ("""
        DROP TABLE IF EXISTS vendors_dim;
        CREATE TABLE IF NOT EXISTS vendors_dim (
            vendor_id               INTEGER NOT NULL,
            name                    VARCHAR(256),
            PRIMARY KEY(vendor_id)
        );
    """)

    inserting_vendors = ("""
        SELECT vendor_number,
            vendor_name
        FROM staging_csv
    """)

    creating_counties_table = ("""
        DROP TABLE IF EXISTS counties_dim;
        CREATE TABLE IF NOT EXISTS counties_dim (
            county_id               INTEGER NOT NULL,
            name                    VARCHAR(256),
            PRIMARY KEY(county_id)
        );
    """)

    inserting_vendors = ("""
        SELECT vendor_number,
            vendor_name
        FROM staging_csv
    """)

    creating_stores_table = ("""
        DROP TABLE IF EXISTS stores_dim;
        CREATE TABLE IF NOT EXISTS stores_dim (
            store_id                INTEGER NOT NULL,
            name                    VARCHAR(256),
            address                 VARCHAR(256),
            city                    VARCHAR(256),
            zip_code                INTEGER,
            lon                     FLOAT,
            lat                     FLOAT,
            PRIMARY KEY(store_id)
        );
    """)

    creating_calendar_table = ("""
        DROP TABLE IF EXISTS calendar_dim;
        CREATE TABLE IF NOT EXISTS calendar_dim (
            date                    TIMESTAMP NOT NULL,
            hour                    INTEGER NOT NULL ,
            day                     INTEGER NOT NULL,
            week                    INTEGER NOT NULL ,
            month                   INTEGER NOT NULL,
            year                    INTEGER NOT NULL,
            weekday                 INTEGER NOT NULL ,
            PRIMARY KEY(date, day, month, year)
        );
    """)
