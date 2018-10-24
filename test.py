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


# mov = test.database.table.Movies
# from tasho.query_engine import QueryEngine
# engine = QueryEngine(5)
# x = engine.query(lambda id, data: "World" in data['title'], mov.chunks)

#import test
#songs = test.database.table.Songs
#from tasho.query_engine import QueryEngine
#engine = QueryEngine(5)
#x = engine.query(lambda id, data: "Michael" in data['artist'], songs.chunks)