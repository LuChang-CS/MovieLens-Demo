import timeit
import random

import pymysql


class MySQLTest:

    def __init__(self, host='localhost', port=3306, username='root', password='root', database='test'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.conn = pymysql.connect(host, username, password, database, port)

    def __del__(self):
        self.conn.close()

    def test_select(self, repeat_times=1, limit=10000):
        sql1 = 'select * from movies limit %d;' % limit
        sql2 = 'select * from genres limit %d;' % limit
        sql3 = 'select * from ratings limit %d;' % limit
        sql4 = 'select * from tags limit %d;' % limit

        sqls = [sql1, sql2, sql3, sql4]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_where(self, repeat_times=1, limit=10000):
        sql1 = 'select * from ratings where rating > 2 limit %d;' % limit
        sql2 = 'select * from genres where genres = \'Documentary\' limit %d;' % limit
        sql3 = 'select * from ratings where timestamp > 1000000000 and rating > 3.5 limit %d;' % limit
        sql4 = 'select * from genres where genres = \'Adventure\' or genres = \'Sci-Fi\' limit %d;' % limit

        sqls = [sql1, sql2, sql3, sql4]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_aggregation(self, repeat_times=1, limit=10000):
        sql1 = 'select max(rating) from ratings;'
        sql2 = 'select count(rating) from ratings;'
        sql3 = 'select distinct(userId) from ratings;'
        sql4 = 'select count(distinct(userId)) from ratings;'

        sqls = [sql1, sql2, sql3, sql4]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_group_by(self, repeat_times=1, limit=10000):
        sql1 = 'select userId, max(rating) from ratings group by userId limit %d;' % limit
        sql2 = 'select movieId, count(genres) from genres group by movieId limit %d;' % limit

        sqls = [sql1, sql2]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_order_by(self, repeat_times=1, limit=10000):
        sql1 = 'select * from ratings order by rating limit %d;' % limit
        sql2 = 'select * from ratings order by rating, movieId limit %d;' % limit
        sql3 = 'select avg(rating) as avg_rating from ratings group by movieId order by avg_rating DESC limit %d;' % limit

        sqls = [sql1, sql2, sql3]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_join(self, repeat_times=1, limit=10000):
        sql1 = 'select title, rating \
            from ratings join movies \
            on ratings.movieId = movies.movieId limit %d;' % limit
        sql2 = 'select rating \
            from ratings join genres \
            on ratings.movieId = genres.movieId \
            where genres = \'Documentary\' limit %d;' % limit
        sql3 = 'select title, rating \
            from ratings join movies \
            on ratings.movieId = movies.movieId \
            join genres \
            on ratings.movieId = genres.movieId \
            where genres = \'Documentary\' limit %d;' % limit

        sqls = [sql1, sql2, sql3]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def test_subquery(self, repeat_times=1, limit=10000):
        sql1 = 'select userId, ratings.movieId, rating \
            from ratings join ((select movieId, max(rating) as r from ratings group by movieId) as RR) \
            on ratings.movieId = RR.movieId and ratings.rating = RR.r limit %d;' % limit

        sqls = [sql1]
        time_costs = self._test_select(sqls, repeat_times)

        return self._test_select_time_cost(sqls, time_costs, repeat_times)

    def _test_select(self, sqls, repeat_times):
        t = []
        for sql in sqls:
            cost = self._timeit(sql, False, repeat_times)
            t += [cost]
        return t

    def _test_select_time_cost(self, sqls, time_costs, repeat_times):
        return [{
            'sql': sql.replace('\n', ' '),
            'total_time_cost': time_cost,
            'average_time_cost': time_cost / repeat_times
        } for (sql, time_cost) in zip(sqls, time_costs)]

    def test_insert(self, table='ratings', column='id', update_times=1):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])
        values = [repr(v) for v in max_line[1:]]
        sql = 'insert into {table} values(%d, {value});'.format(table=table, value=','.join(values))

        sqls = []
        for id_ in range(max_id + 1, max_id + update_times + 1):
            sqls.append(sql % id_)

        time_cost = self._test_update(sqls)

        return self._test_update_time_cost(time_cost, update_times)

    def test_delete(self, table='ratings', column='id', update_times=1):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])

        ids_to_delete = random.sample(range(1, max_id + 1), update_times)
        sql = 'delete from {table} where {column} = %d;'.format(table=table, column=column)

        sqls = []
        for id_ in ids_to_delete:
            sqls.append(sql % id_)

        time_cost = self._test_update(sqls)

        return self._test_update_time_cost(time_cost, update_times)

    def test_update(self, table='ratings', column='id', update_column='rating', update_column_index=3, update_times=1):
        max_line = self._select_max(table, column)
        max_id = int(max_line[0])

        ids_to_update = random.sample(range(1, max_id + 1), update_times)
        sql = 'update {table} set {update_column} = {value} where {column} = %d;'\
            .format(
                table=table,
                update_column=update_column,
                value=repr(max_line[update_column_index]),
                column=column
            )

        sqls = []
        for id_ in ids_to_update:
            sqls.append(sql % id_)

        time_cost = self._test_update(sqls)

        return self._test_update_time_cost(time_cost, update_times)

    def test_create_index(self):
        sql1 = 'create unique index MoviesMovieId  on movies(movieId);'
        sql2 = 'create unique index MoviesTitle    on movies(title);'
        sql3 = 'create index GenresMovieId  on genres(movieId);'
        sql4 = 'create index GenresGenres   on genres(genres);'
        sql5 = 'create index RatingsUserId  on ratings(userId);'
        sql6 = 'create index RatingsMovieId on ratings(movieId);'
        sql7 = 'create index RatingsRating  on ratings(rating);'
        sql8 = 'create index TagsMovieId    on tags(movieId);'

        sqls = [sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8]

        time_cost = self._test_update(sqls)

        return self._test_update_time_cost(time_cost, 1)

    def _select_max(self, table, column):
        sql = 'select * from {table} where {column} = (select max({column}) from {table})'.format(table=table, column=column)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone()

    def _test_update(self, sqls):
        return self._timeit(sqls, True)

    def _test_update_time_cost(self, time_cost, update_times):
        return {
            'total_time_cost': time_cost,
            'average_time_cost': time_cost / update_times
        }

    def _timeit(self, sqls, update, repeat_times=1):
        assert isinstance(repeat_times, int) and repeat_times > 0

        cursor = self.conn.cursor()

        if update:
            start = timeit.default_timer()
            try:
                for sql in sqls:
                    cursor.execute(sql)
                self.conn.commit()
            except:
                self.conn.rollback()
            end = timeit.default_timer()
        else:
            start = timeit.default_timer()
            for _ in range(0, repeat_times):
                cursor.execute(sqls)
            end = timeit.default_timer()

        return end - start


if __name__ == '__main__':
    mst = MySQLTest(database='MovieLens')
    repeat_t = 5
    update_t = 10000
    limit = 10000

#    print('testing select without index')
#    select_time_without_index = dict()
#    print('    testing select')
#    select_time_without_index['select'] = mst.test_select(repeat_times=repeat_t)
#    print('    testing where')
#    select_time_without_index['where'] = mst.test_where(repeat_times=repeat_t)
#    print('    testing aggregation')
#    select_time_without_index['aggregation'] = mst.test_aggregation(repeat_times=repeat_t)
#    print('    testing group by')
#    select_time_without_index['group by'] = mst.test_group_by(repeat_times=repeat_t)
#    print('    testing order by')
#    select_time_without_index['order by'] = mst.test_order_by(repeat_times=repeat_t)
#    print('    testing join')
#    select_time_without_index['join'] = mst.test_join(repeat_times=repeat_t)
#    print('    testing subquery')
#    select_time_without_index['subquery'] = mst.test_subquery(repeat_times=repeat_t)

#    print('testing update without index')
#    update_time_without_index = dict()
#    print('    testing insert')
#    update_time_without_index['insert'] = mst.test_insert(update_times=update_t)
#    print('    testing delete')
#    update_time_without_index['delete'] = mst.test_delete(update_times=update_t)
#    print('testing update')
#    update_time_without_index['update'] = mst.test_update(update_times=update_t)
#    print('testing create index')
#    update_time_without_index['create index'] = mst.test_create_index()

    print('testing select with index')
    select_time_with_index = dict()
    print('    testing select')
    select_time_with_index['select'] = mst.test_select(repeat_times=repeat_t, limit=limit)
    print('    testing where')
    select_time_with_index['where'] = mst.test_where(repeat_times=repeat_t, limit=limit)
    print('    testing aggregation')
    select_time_with_index['aggregation'] = mst.test_aggregation(repeat_times=repeat_t, limit=limit)
    print('    testing group by')
    select_time_with_index['group by'] = mst.test_group_by(repeat_times=repeat_t, limit=limit)
    print('    testing order by')
    select_time_with_index['order by'] = mst.test_order_by(repeat_times=repeat_t, limit=limit)
    print('    testing join')
    select_time_with_index['join'] = mst.test_join(repeat_times=repeat_t, limit=limit)
    print('    testing subquery')
    select_time_with_index['subquery'] = mst.test_subquery(repeat_times=repeat_t, limit=limit)

    print('testing update with index')
    update_time_with_index = dict()
    print('    testing insert')
    update_time_with_index['insert'] = mst.test_insert(update_times=update_t)
    print('    testing delete')
    update_time_with_index['delete'] = mst.test_delete(update_times=update_t)
    print('testing update')
    update_time_with_index['update'] = mst.test_update(update_times=update_t)

    time_cost_info = {
        'select_time_with_index': select_time_with_index,
        'update_time_with_index': update_time_with_index
    }

    info = open('info.txt', 'w', encoding='UTF-8')
    info.write('for select: repeat_times = %d\n' % repeat_t)
    info.write('for select: limit = %d\n' % limit)
    info.write('for update: update_times = %d\n' % update_t)
    info.write('result:\n')

    import json

    json.dump(time_cost_info, info, indent=4)
    info.close()
