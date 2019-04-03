# TashoDB 

A fast and portable python NoSQL database. Sucessor to KoDB.
```
> pip install tasho
```


#### Using the DB

To initalize or open a database, it's as straightforward as calling `tasho.Database.new` or `tasho.Database.open`.
```python
>>> import tasho
>>> database = tasho.Database.new("AnimeDatabase")  # Creates a new database.
>>> database = tasho.Database.new("AnimeDatabase", open_instead=True)  # Creates a new database or opens if it already exists.
>>> database = tasho.Database.open("AnimeDatabase") # Opens a database.
```

Tables can be called through `tasho.Database.get_table(table_name)` or through `tasho.Database.table.table_name` 
```python
>>> tbl_anime = database.table.Anime 			# These all return 
>>> tbl_anime = database.get_table("Anime")		# the same Table
>>> tbl_anime = database.table['Anime']			# object.
>>> tbl_anime
<TashoDBTable>: Anime | Chunks: 1
```

~~***Note:  Tables are set to auto commit by default. When doing bulk inserts, make sure to set `Table.auto_commit` to `False` and running `Table.commit()` manually afterwards.***~~ You can now do bulk inserts through `Table.bulk_insert`.


#### Data Storage
```python
>>> tbl_anime.insert('001', {'title': 'Nichijou', 'episodes': 24, 'rating': 99})
'Shows-d545998bc3485346'
>>> tbl_anime.insert(tasho.AutoGenerateId, {'title': 'Nichijou', 'episodes': 24, 'rating': 99})
'Shows-485399846d545bc3'
>>> tbl_anime.bulk_insert(
    {
        001: {'title': 'Nichijou', 'episodes': 24, 'rating': 99},
        002: {'title': 'Danshi Koukousei Wa Nichijou', 'episodes': 24, 'rating': 80},
    }
)
True
>>>
```

This stores the data with `001` as the Document ID. Document IDs can either be String or Int or you can specify `tasho.AutoGenerateId` to let the database generate an ID. Since `Table.auto_commit` has been set to true, running `Table.commit()` is no longer needed.


#### Retrieval
There are multiple ways of accessing data.
```python
# The document can be accessed through a regular get method
>>> show = tbl_shows.get('001')
>>> show
<TashoDBDocument:001> Origin: Shows
>>> show.dict
{'title': 'Nichijou', 'episodes': 24, 'rating': 99, '_id': '001'}
>>>
# Table.query allows you to pass a callable to filter the data.
>>> tbl_shows.query(lambda id, data: data['rating'] > 50)
[<TashoDBDocument:001> Origin: Shows]
>>>
# Table.query_one works the same as Table.query but stops at the first match.
>>> tbl_shows.query_one(lambda id, data: data['rating'] > 50)
<TashoDBDocument:001> Origin: Shows
```
`Table.get(id)` returns a Document object that contains the data.


#### Manipulating Data

Manipulating data is as easy as changing the values in the Document object.
```python
>>> show
<TashoDBDocument:001> Origin: Shows
>>> show.dict
{'title': 'Nichijou', 'episodes': 24, 'rating': 99, '_id': '001'}
>>> show.rating = 98
>>> show['title'] = 'Nichibros'
>>> show.save()
'Shows-d545998bc3485346'
>>> show.dict
{'title': 'Nichibros', 'episodes': 24, 'rating': 98, '_id': '001'}
```


Document deletion can also be done with `Document.delete()`.
```python
>>> list(tbl_shows.items())
[('001', {'title': 'Nichijou', 'episodes': 24, 'rating': 98})]
>>> show.delete()
True
>>> list(tbl_shows.items())
[]
```

***Note: Document objects behaves almost the same way as dictionaries. `Document.pop`, `Document.update` and `Document.get` works the same way.***

_See: test.py for more use cases._
