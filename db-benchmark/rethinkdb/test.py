import timeit
import random

import rethinkdb as r


class RethinkDBTest:

    def __init__(self, host='localhost', port=28015, username='admin', password='', database='MovieLens'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.conn = r.connect(host=host, port=port, db=database, user=username, password=password)

    def __del__(self):
        self.conn.close()

    def test_select(self, repeat_times=1):
        reql1 = r.table('movies')
        reql2 = r.table('genres')
        reql3 = r.table('ratings')
        reql4 = r.table('tags')

        reqls = [reql1, reql2, reql3, reql4]
        time_costs = self._test_select([reql1, reql2, reql3, reql4], repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_where(self, repeat_times=1):
        reql1 = r.table('ratings').between(2, r.maxval, left_bound='open', index='rating')
        reql2 = r.table('genres').get_all('Action', index='genres')
        reql3 = r.table('ratings').filter(r.row['timestamp'].gt(1000000000).and_(r.row['rating'].gt(3.5)))
        reql4 = r.table('genres').get_all('Adventure', 'Sci-Fi', index='genres')

        reqls = [reql1, reql2, reql3, reql4]
        time_costs = self._test_select([reql1, reql2, reql3, reql4], repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_aggregation(self, repeat_times=1):
        reql1 = r.table('ratings').max(index='rating')
        reql2 = r.table('ratings').count('rating')
        reql3 = r.table('ratings').distinct(index='userId')
        reql4 = r.table('ratings').distinct(index='userId').count()

        reqls = [reql1, reql2, reql3, reql4]
        time_costs = self._test_select([reql1, reql2, reql3, reql4], repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_group_by(self, repeat_times=1):
        reql1 = r.table('ratings').group('userId', index='userId').max('rating')
        reql2 = r.table('genres').group('movieId', index='movieId').count('genres')

        reqls = [reql1, reql2]
        time_costs = self._test_select(reqls, repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_order_by(self, repeat_times=1):
        reql1 = r.table('ratings').order_by(index='rating')
        reql2 = r.table('ratings').order_by(index='rating_and_movieId')
        reql3 = r.table('ratings').group('movieId', index='movieId').avg('rating').ungroup().order_by('reduction')

        reqls = [reql1, reql2, reql3]
        time_costs = self._test_select(reqls, repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_join(self, repeat_times=1):
        reql1 = r.table('ratings').eq_join('movieId', r.table('movies'), index='movieId').zip().pluck('title', 'rating')
        reql2 = r.table('genres').filter(r.row['genres'].eq('Documentary'))\
            .eq_join('movieId', r.table('ratings'), index='movieId')\
            .zip()
        reql3 = r.table('genres').filter(r.row['genres'].eq('Documentary'))\
            .eq_join('movieId', r.table('movies')).zip()\
            .eq_join('movieId', r.table('ratings'), index='movieId')\
            .zip().pluck('title', 'rating')

        reqls = [reql1, reql2, reql3]
        time_costs = self._test_select(reqls, repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def test_subquery(self, repeat_times=1):
        reql1 = r.table('ratings')\
            .group('movieId', index='movieId').max('rating')\
            .ungroup().get_field('reduction')\
            .eq_join(
                lambda row: [row['rating'], row['movieId']],
                r.table('ratings'),
                index='rating_and_movieId'
            ).zip().pluck('userId', 'movieId', 'rating')

        reqls = [reql1]
        time_costs = self._test_select(reqls, repeat_times)

        return self._test_select_time_cost(reqls, time_costs, repeat_times)

    def _test_select(self, reqls, repeat_times):
        t = []
        for reql in reqls:
            cost = self._timeit(reql, False, repeat_times)
            t += [cost]
        return t

    def _test_select_time_cost(self, reqls, time_costs, repeat_times):
        return [{
            'reql': str(reql),
            'total_time_cost': time_cost,
            'average_time_cost': time_cost / repeat_times
        } for (reql, time_cost) in zip(reqls, time_costs)]

    def test_insert(self, table='ratings', column='id', update_times=1):
        max_line = r.table(table).max(index=column).run(self.conn)
        max_id = int(max_line[column])

        reqls = []
        for id_ in range(max_id + 1, max_id + update_times + 1):
            obj = max_line.copy()
            obj[column] = id_
            reqls.append(r.table(table).insert(obj))

        time_cost = self._test_update(reqls)
        return self._test_update_time_cost(time_cost, update_times)

    def test_delete(self, table='ratings', column='id', update_times=1):
        max_line = r.table(table).max(index=column).run(self.conn)
        max_id = int(max_line[column])

        ids_to_delete = random.sample(range(1, max_id + 1), update_times)
        reqls = []
        for id_ in ids_to_delete:
            reqls.append(r.table(table).get_all(id_, index=column).delete())

        time_cost = self._test_update(reqls)
        return self._test_update_time_cost(time_cost, update_times)

    def test_update(self, table='ratings', column='id', update_column='rating', update_times=1):
        max_line = r.table(table).max(index=column).run(self.conn)
        max_id = int(max_line['id'])

        ids_to_update = random.sample(range(1, max_id + 1), update_times)
        reqls = []
        for id_ in ids_to_update:
            reqls.append(r.table(table).get_all(id_, index=column).update({
                update_column: max_line[update_column]
            }))

        time_cost = self._test_update(reqls)
        return self._test_update_time_cost(time_cost, update_times)

    def test_create_index(self):
        reql1 = r.table('ratings').index_create('userId')
        reql2 = r.table('ratings').index_create('movieId')
        reql3 = r.table('ratings').index_create('rating')
        reql4 = r.table('ratings').index_create('timestamp')
        reql5 = r.table('ratings').index_create('rating_and_movieId', [r.row('rating'), r.row('movieId')])
        reql6 = r.table('genres').index_create('genres')
        reql7 = r.table('genres').index_create('movieId')

        reqls = [reql1, reql2, reql3, reql4, reql5, reql6, reql7]

        time_cost = self._test_update(reqls)

        return self._test_update_time_cost(time_cost, 1)

    def _test_update(self, reqls):
        return self._timeit(reqls, True)

    def _test_update_time_cost(self, time_cost, update_times):
        return {
            'total_time_cost': time_cost,
            'average_time_cost': time_cost / update_times
        }

    def _timeit(self, reqls, update, repeat_times=1):
        assert isinstance(repeat_times, int) and repeat_times > 0

        if update:
            start = timeit.default_timer()
            for reql in reqls:
                reql.run(self.conn, array_limit=100000000)
            end = timeit.default_timer()
        else:
            start = timeit.default_timer()
            for _ in range(0, repeat_times):
                reqls.run(self.conn, array_limit=100000000)
            end = timeit.default_timer()

        return end - start


if __name__ == '__main__':
    rt = RethinkDBTest('10.132.141.101')
    repeat_t = 2
    update_t = 10

    print('testing select with index')
    select_time_with_index = dict()
    select_time_with_index['select'] = rt.test_select(repeat_times=repeat_t)
    select_time_with_index['where'] = rt.test_where(repeat_times=repeat_t)
    select_time_with_index['aggregation'] = rt.test_aggregation(repeat_times=repeat_t)
    select_time_with_index['group by'] = rt.test_group_by(repeat_times=repeat_t)
    select_time_with_index['order by'] = rt.test_order_by(repeat_times=repeat_t)
    select_time_with_index['join'] = rt.test_join(repeat_times=repeat_t)
    select_time_with_index['subquery'] = rt.test_subquery(repeat_times=repeat_t)

    print('testing update with index')
    update_time_with_index = dict()
    update_time_with_index['insert'] = rt.test_insert(update_times=update_t)
    update_time_with_index['delete'] = rt.test_delete(update_times=update_t)
    update_time_with_index['update'] = rt.test_update(update_times=update_t)

    time_cost_info = {
        'select_time_with_index': select_time_with_index,
        'update_time_with_index': update_time_with_index
    }

    info = open('info.txt', 'w', encoding='UTF-8')
    info.write('for select: repeat_times = %d\n' % repeat_t)
    info.write('for update: update_times = %d\n' % update_t)
    info.write('result:\n')

    import json

    json.dump(time_cost_info, info, indent=4)
    info.close()
