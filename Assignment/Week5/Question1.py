import mysql.connector
import pandas as pd

# ---------------- MySQL Config ----------------
MYSQL_CONFIG = {
    "user": "root",          # change this
    "password": "AAbb@@11", # change this
    "host": "localhost",
    "database": "online_retail"
}

# ---------------- Connect MySQL ----------------
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

# ---------------- Create Tables (2NF schema) ----------------
def create_mysql_tables():
    conn = get_mysql_connection()
    cur = conn.cursor()

    # Drop old tables if exist
    cur.execute("DROP TABLE IF EXISTS Transactions;")
    cur.execute("DROP TABLE IF EXISTS Products;")
    cur.execute("DROP TABLE IF EXISTS Customers;")

    # Customers Table
    cur.execute("""
        CREATE TABLE Customers (
            CustomerID INT PRIMARY KEY,
            Country VARCHAR(100)
        );
    """)

    # Products Table
    cur.execute("""
        CREATE TABLE Products (
            StockCode VARCHAR(20) PRIMARY KEY,
            Description VARCHAR(255),
            UnitPrice DECIMAL(10,2)
        );
    """)

    # Transactions Table
    cur.execute("""
        CREATE TABLE Transactions (
            InvoiceNo VARCHAR(20),
            InvoiceDate DATETIME,
            CustomerID INT,
            StockCode VARCHAR(20),
            Quantity INT,
            PRIMARY KEY (InvoiceNo, StockCode),
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
            FOREIGN KEY (StockCode) REFERENCES Products(StockCode)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ MySQL tables created.")

# ---------------- Insert Data ----------------
def insert_mysql_data(csv_path, limit=1000):
    conn = get_mysql_connection()
    cur = conn.cursor()

    df = pd.read_csv(csv_path)  # Dataset is Excel file
    df = df.dropna(subset=["CustomerID"])  # Remove missing customers
    df = df.head(limit)  # Take first N records

    # Insert Customers
    for cid, country in df[["CustomerID", "Country"]].drop_duplicates().values:
        cur.execute("INSERT IGNORE INTO Customers VALUES (%s, %s)", (int(cid), country))

    # Insert Products
    for sc, desc, price in df[["StockCode", "Description", "UnitPrice"]].drop_duplicates().values:
        cur.execute("INSERT IGNORE INTO Products VALUES (%s, %s, %s)", (sc, str(desc), float(price)))

    # Insert Transactions
    for inv, date, cid, sc, qty in df[["InvoiceNo", "InvoiceDate", "CustomerID", "StockCode", "Quantity"]].values:
        cur.execute("INSERT IGNORE INTO Transactions VALUES (%s, %s, %s, %s, %s)",
                    (str(inv), pd.to_datetime(date), int(cid), str(sc), int(qty)))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Inserted", len(df), "records into MySQL.")

# ---------------- Run Part 1 ----------------
if __name__ == "__main__":
    create_mysql_tables()
    insert_mysql_data('C:/Users/HP/Downloads/Online_retail.csv', limit=1000)

