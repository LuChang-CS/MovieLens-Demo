-- select *
select * from movies;
select * from genres;
select * from ratings;
select * from tags;

-- where
select * from ratings where rating > 2;
select * from genres where genres = 'Action';
select * from ratings where timestamp > 1000000000 and rating > 3.5
select * from genres where genres = 'Adventure' or genres = 'Sci-Fi'

-- aggregation function
select max(rating) from ratings;
select count(rating) from ratings;
select distinct(userId) from ratings;
select count(distinct(userId)) from ratings;

-- group by
select userId, max(rating) from ratings group by userId;
select movieId, count(genres) from genres group by movieId;

-- order by
select * from ratings order by rating;
select * from ratings order by rating, movieId;
select avg(rating) as avg_rating from ratings group by movieId order by avg_rating DESC;

-- join
select title, rating 
    from ratings join movies 
    on ratings.movieId = movies.movieId;

select rating 
    from ratings join genres 
    on ratings.movieId = genres.movieId 
    where genres = 'Documentary';

select title, rating 
    from ratings join movies 
    on ratings.movieId = movies.movieId join genres 
    on ratings.movieId =genres.movieId 
    where genres = 'Documentary';

-- subquery
select user, ratings.movie, rating 
    from ratings join ((select movie, max(rating) as r from ratings group by movie) as RR) 
    on ratings.movie = RR.movie and ratings.rating = RR.r;


-- insert
insert into ratings values(10005, 15, 1200, 5.0, 949779173);
insert into ratings values(10006, 15, 1200, 5.0, 949779173);
insert into genres values(54407, 131262, 'Horror');
insert into genres values(54408, 131262, 'Horror');
insert into tags values(1296, 663, 260, 'Syfy', 1438398050);
insert into tags values(1296, 663, 260, 'Syft', 1438398050);
insert into movies values(9999999, 'qweqwe', 123123123, 123123123);

-- update
update ratings set rating = 5.0 where id = 3456;
update ratings set rating = 5.0 where userId = 4 and movieId = 185;
update ratings set rating = 5.0 where userId = 4 and movieId = 260;

-- delete
delete from ratings where id = 3456;
delete from ratings where userId = 4 and movieId = 185;
delete from ratings where userId = 4 and movieId = 260;

create index MoviesMovieId  on movies(movieId);
create index MoviesTitle    on movies(title);

create index GenresMovieId  on genres(movieId);
create index GenresGenres   on genres(genres);

create index RatingsUserId  on ratings(userId);
create index RatingsMovieId on ratings(movieId);
create index RatingsRating  on ratings(rating);

create index TagsMovieId    on tags(movieId);
