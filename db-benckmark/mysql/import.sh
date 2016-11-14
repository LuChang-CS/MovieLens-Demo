#!/bin/bash
#
# file: import.sh
# 
# execute './import.sh' in commandline
# note the executable priviledge of this shell

# MovieLens csv path
movielens_path="~/Downloads/movielens/ml-20m"

# database name
db="MovieLens"

# movies table name and movies filename
movies_table="movies"
movies_filename="${movielens_path}/${movies_table}.csv"

# genres table name and genres filename
genres_table="genres"
genres_filename="${movielens_path}/${genres_table}.csv"

# ratings table name and ratings filename
ratings_table="ratings"
ratings_filename="${movielens_path}/${ratings_table}.csv"

# tags table and tags filename
tags_table="tags"
tages_filename="${movielens_path}/${tags_table}.csv"


user="root"
password="root"

mysqlcc="mysqlimport \
    --local \
    --fields-optionally-enclosed-by='\"' \
    --fields-terminated-by=',' \
    --lines-terminated-by='\n' \
    --ignore-lines=1 \
    --user=${user} \
    --password=${password} \
    ${db}"

$mysqlcc $movies_filename
$mysqlcc $genres_filename
$mysqlcc $ratings_filename
$mysqlcc $tages_filename
