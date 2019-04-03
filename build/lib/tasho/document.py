
class Document():

    def __init__(self, data, table):
        super(Document, self).__setattr__('_id', data[0])
        super(Document, self).__setattr__('_data', data[1])
        super(Document, self).__setattr__('_table', table)

    def __repr__(self):
        return "<TashoDBDocument:{} Origin: {}>".format(self._id, self._table.name)

    @property
    def dict(self):
        data = {x: y for x, y in self._data.items()}
        data['_id'] = self._id
        return data

    def __getattr__(self, attribute):
        if attribute in self._data:
            return self._data[attribute]

    def __setattr__(self, attribute, data):
        if attribute in self._data:
            self._data[attribute] = data

    def __getitem__(self, attribute):
        return self._data[attribute]
    
    def __setitem__(self, attribute, data):
        self._data[attribute] = data   


    def save(self):
        """
        Document.save()

        Saves the document to the table. Might have to call Table.commit()
        """
        return self._table.insert(self._id, self._data)

    def update(self, data):
        """
        Document.update(Dict:data)

        Updates the document. Works the same as Dict.update()
        """
        self._data.update(data)

    def pop(self, data):
        """
        Document.pop() returns Something

        Works the same way as Dict.pop()
        """
        return self._data.pop()

    def get(self, data, default=None):
        """
        Document.get(Object:data, Object:default)

        Works the same way as Dict.get()
        """
        return self._data.get(data, default)

    def delete(self):
        """
        Document.delete()

        Deletes the document.
        """
        return self._table.delete(self._id)