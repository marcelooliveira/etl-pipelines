import sqlite3

conn = sqlite3.connect("data/crypto.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM cryptocurrencies LIMIT 5")
print(cursor.fetchall())
