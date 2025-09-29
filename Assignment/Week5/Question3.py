import time
import mysql.connector
from pymongo import MongoClient

# ---------------- MySQL Config ----------------
MYSQL_CONFIG = {
    "user": "root",
    "password": "AAbb@@11,
    "host": "localhost",
    "database": "online_retail"
}

# ---------------- MongoDB Config ----------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "online_retail"

# ---------------- Connections ----------------
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_mongo_connection():
    return MongoClient(MONGO_URI, maxPoolSize=50, wTimeoutMS=2500)

# ---------------- CRUD in MySQL ----------------
def mysql_crud():
    conn = get_mysql_connection()
    cur = conn.cursor()

    timings = {}

    # CREATE
    start = time.time()
    cur.execute("INSERT INTO Customers VALUES (%s, %s)", (99999, "TestCountry"))
    conn.commit()
    timings["MySQL Create"] = time.time() - start

    # READ
    start = time.time()
    cur.execute("SELECT * FROM Customers WHERE CustomerID = %s", (99999,))
    _ = cur.fetchone()
    timings["MySQL Read"] = time.time() - start

    # UPDATE
    start = time.time()
    cur.execute("UPDATE Customers SET Country = %s WHERE CustomerID = %s", ("UpdatedCountry", 99999))
    conn.commit()
    timings["MySQL Update"] = time.time() - start

    # DELETE
    start = time.time()
    cur.execute("DELETE FROM Customers WHERE CustomerID = %s", (99999,))
    conn.commit()
    timings["MySQL Delete"] = time.time() - start

    cur.close()
    conn.close()
    return timings

# ---------------- CRUD in MongoDB ----------------
def mongo_crud():
    client = get_mongo_connection()
    db = client[DB_NAME]
    coll = db["customers"]  # Using customer-centric for demo

    timings = {}

    # CREATE
    start = time.time()
    coll.insert_one({"CustomerID": 99999, "Country": "TestCountry", "Transactions": []})
    timings["Mongo Create"] = time.time() - start

    # READ
    start = time.time()
    _ = coll.find_one({"CustomerID": 99999})
    timings["Mongo Read"] = time.time() - start

    # UPDATE
    start = time.time()
    coll.update_one({"CustomerID": 99999}, {"$set": {"Country": "UpdatedCountry"}})
    timings["Mongo Update"] = time.time() - start

    # DELETE
    start = time.time()
    coll.delete_one({"CustomerID": 99999})
    timings["Mongo Delete"] = time.time() - start

    client.close()
    return timings

# ---------------- Run CRUD + Compare ----------------
if __name__ == "__main__":
    mysql_times = mysql_crud()
    mongo_times = mongo_crud()

    print("\n--- CRUD Performance Comparison ---")
    for k, v in mysql_times.items():
        print(f"{k}: {v:.6f} sec")
    for k, v in mongo_times.items():
        print(f"{k}: {v:.6f} sec")
