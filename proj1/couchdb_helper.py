import couchdb

class CouchDB:

    db: couchdb.Database

    def __init__(self) -> None:
        pass

    def connect_to_db(self, db_name: str):

        couch = couchdb.Server('http://admin:2021_Copia_ML@127.0.0.1:5984/')

        try:
            self.db = couch.create(db_name)
        except:
            self.db = couch[db_name]

    def create_kv_pair(self, key: str, value: str):
        
        doc = {key: value}
        self.db.save(doc)