import kodb
import secrets
try:
	database = kodb.KoDB.new("testTable")
except:
	database = kodb.KoDB.open("testTable")

table = database.get_table("default")
table.auto_commit = False

for i in range(0, 50000):
	table.insert(i, {secrets.token_hex(): secrets.token_hex()})

table.commit()