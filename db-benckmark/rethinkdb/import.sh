#!/bin/bash
#
# file: import.sh
# 
# change to directory movielens csv files stored
# and execute './import.sh' in commandline
# note the executable priviledge of this shell

# database name
db="MovieLens"

# movies table name and movies filename
movies_table="movies"
movies_filename="${movies_table}.csv"
movies_pkey="movieId"

# genres table name and genres filename
genres_table="genres"
genres_filename="${genres_table}.csv"
genres_pkey="id"

# ratings table name and ratings filename
ratings_table="ratings"
ratings_filename="${ratings_table}.csv"
ratings_pkey="id"

# tags table and tags filename
tags_table="tags"
tages_filename="${tags_table}.csv"
tags_pkey="id"


rethinkdb import -f ${movies_filename}  --table ${db}.${movies_table}  --pkey ${movies_pkey}
rethinkdb import -f ${genres_filename}  --table ${db}.${genres_table}  --pkey ${genres_pkey}
rethinkdb import -f ${ratings_filename} --table ${db}.${ratings_table} --pkey ${ratings_pkey}
rethinkdb import -f ${tages_filename}   --table ${db}.${tags_table}    --pkey ${tags_pkey}
