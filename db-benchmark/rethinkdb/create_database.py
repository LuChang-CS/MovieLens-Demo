import rethinkdb as r


database = 'MovieLens'

host = 'localhost'
port = 28015
username = 'admin'
password = ''

conn = r.connect(host=host, port=port, db=database, user=username, password=password)

try:
    r.db_drop(database).run(conn)
except:
    pass
finally:
    r.db_create(database).run(conn)
