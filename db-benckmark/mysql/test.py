import timeit
import random

import pymysql


class MySQLTest:

    def __init__(self, host, username, password, database, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port

        self.db = pymysql.connect(host, username, password, database, port)

    def __del__(self):
        self.db.close()

    def test_select(self, repeat_times=100):
        sql1 = 'select * from movies;'
        sql2 = 'select * from genres;'
        sql3 = 'select * from ratings;'
        sql4 = 'select * from tags;'

        return self._test_select([sql1, sql2, sql3, sql4], repeat_times)

    def test_where(self, repeat_times=100):
        sql1 = 'select * from ratings where rating > 2;'
        sql2 = 'select * from genres where genres = \'Action\';'
        sql3 = 'select * from ratings where timestamp > 1000000000 and rating > 3.5;'
        sql4 = 'select * from genres where genres = \'Adventure\' or genres = \'Sci-Fi\';'

        return self._test_select([sql1, sql2, sql3, sql4], repeat_times)

    def test_aggregation(self, repeat_times=100):
        sql1 = 'select max(rating) from ratings;'
        sql2 = 'select count(rating) from ratings;'
        sql3 = 'select distinct(userId) from ratings;'
        sql4 = 'select count(distinct(userId)) from ratings;'

        return self._test_select([sql1, sql2, sql3, sql4], repeat_times)

    def test_group_by(self, repeat_times=100):
        sql1 = 'select userId, max(rating) from ratings group by userId;'
        sql2 = 'select movieId, count(genres) from genres group by movieId;'

        return self._test_select([sql1, sql2], repeat_times)

    def test_order_by(self, repeat_times=100):
        sql1 = 'select * from ratings order by rating;'
        sql2 = 'select * from ratings order by rating, movieId;'
        sql3 = 'select avg(rating) as avg_rating from ratings group by movieId order by avg_rating DESC;'

        return self._test_select([sql1, sql2, sql3], repeat_times)

    def test_join(self, repeat_times=100):
        sql1 = 'select title, rating \
            from ratings join movies \
            on ratings.movieId = movies.movieId;'
        sql2 = 'select rating \
            from ratings join genres \
            on ratings.movieId = genres.movieId \
            where genres = \'Documentary\';'
        sql3 = 'select title, rating \
            from ratings join movies \
            on ratings.movieId = movies.movieId \
            join genres \
            on ratings.movieId = genres.movieId \
            where genres = \'Documentary\';'

        return self._test_select([sql1, sql2, sql3], repeat_times)

    def test_subquery(self, repeat_times):
        sql1 = 'select userId, ratings.movieId, rating \
            from ratings join ((select movieId, max(rating) as r from ratings group by movieId) as RR) \
            on ratings.movieId = RR.movieId and ratings.rating = RR.r;'

        return self._test_select([sql1], repeat_times)

    def _test_select(self, sqls, repeat_times):
        t = []
        for sql in sqls:
            cost = self._timeit(sql, False, repeat_times)
            t += [cost]
        return t

    def test_insert(self, table='ratings', column='id', update_times=1000):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])
        values = [repr(v) for v in max_line[1:]]
        sql = 'insert into {table} values(%d, {value});'.format(table=table, value=','.join(values))

        sqls = []
        for id_ in range(max_id + 1, max_id + update_times):
            sqls.append(sql % id_)

        return self._test_update(sqls)

    def test_delete(self, table='ratings', column='id', update_times=1000):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])

        ids_to_delete = random.sample(range(1, max_id + 1), update_times)
        sql = 'delete from {table} where id = %d;'

        sqls = []
        for id_ in ids_to_delete:
            sqls.append(sql % id_)

        return self._test_update(sqls)

    def test_update(self, table='ratings', column='id', update_column='rating', update_column_index=3, update_times=1000):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])

        ids_to_delete = random.sample(range(1, max_id + 1), update_times)
        sql = 'update {table} set {update_column} = {value} where id = %d;'\
            .format(
                table=table,
                update_column=update_column,
                value=repr(max_line[update_column_index])
            )

        sqls = []
        for id_ in ids_to_delete:
            sqls.append(sql % id_)

        return self._test_update(sqls)

    def test_create_index(self):
        sql1 = 'create index MoviesMovieId  on movies(movieId);'
        sql2 = 'create index MoviesTitle    on movies(title);'
        sql3 = 'create index GenresMovieId  on genres(movieId);'
        sql4 = 'create index GenresGenres   on genres(genres);'
        sql5 = 'create index RatingsUserId  on ratings(userId);'
        sql6 = 'create index RatingsMovieId on ratings(movieId);'
        sql7 = 'create index RatingsRating  on ratings(rating);'
        sql8 = 'create index TagsMovieId    on tags(movieId);'

        sqls = [sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8]

        return self._test_update(sqls)

    def _select_max(self, table, column):
        sql = 'select * from {table} where {column} = (select max(column) from {table})'.format(table=table, column=column)
        cursor = self.db.cursor()
        cursor.execute(sql)
        return cursor.fetchone()

    def _test_update(self, sqls):
        return self._timeit(sqls, True)

    def _timeit(self, sqls, update, repeat_times=100):
        assert isinstance(repeat_times, int) and repeat_times > 0

        cursor = self.db.cursor()

        if update:
            start = timeit.default_timer()
            try:
                for sql in sqls:
                    cursor.execute(sql)
                self.db.commit()
            except:
                self.db.rollback()
            end = timeit.default_timer()
        else:
            start = timeit.default_timer()
            for _ in range(0, repeat_times):
                cursor.execute(sqls)
            end = timeit.default_timer()

        return end - start


if __name__ == '__main__':
    mst = MySQLTest('localhost', 'root', 'root', 'MovieLens')
    t = mst.test_select()
    print(t)
    print([i / 100 for i in t])
