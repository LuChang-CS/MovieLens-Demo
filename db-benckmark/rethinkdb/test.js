var db = r.db('MovieLens');

// select *
db.table('movies');
db.table('genres');
db.table('ratings');
db.table('tags');

// where
db.table('ratings').filter();
