import tasho
import secrets
try:
	database = tasho.Database.new("testTable")
except:
	database = tasho.Database.open("testTable")

table = database.table.Default
table.auto_commit = False

for i in range(0, 50000):
	table.insert(i, {secrets.token_hex(): secrets.token_hex()})

table.commit()