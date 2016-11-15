/**
 * create database and table in mysql
 * usage: mysql -uUser -pPassword < sql_path
 *     or 1. mysql -uUser -pPassword
 *        2. mysql> source sql_path
 */

create database if not exists MovieLens;

drop table if exists movies;
drop table if exists genres;
drop table if exists ratings;
drop table if exists tags;

use MovieLens;

create table movies (
    movieId int not null auto_increment,
    title varchar(200) not null,
    imdbId char(7),
    tmdbId int,
    primary key(movieId)
);

create table genres (
    id int not null auto_increment,
    movieId int not null,
    genres varchar(11) not null,
    primary key(id)
);

create table ratings (
    id int not null auto_increment,
    userId int not null,
    movieId int not null,
    ratings float not null,
    timestamp bigint not null,
    primary key(id)
);

create table tags (
    id int not null auto_increment,
    userId int not null,
    movieId int not null,
    tags varchar(200) not null,
    timestamp bigint not null,
    primary key(id)
);