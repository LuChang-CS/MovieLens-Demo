import sys
import os
import csv
import json


class PreProcessData:

    LINKS = 'links.csv'

    MOVIES = 'movies.csv'

    RATINGS = 'ratings.csv'

    TAGS = 'tags.csv'

    def __init__(self, movielens_path):
        if not movielens_path.endswith(os.sep):
            movielens_path += os.sep

        self.movielens_path = movielens_path
        self.links_path = movielens_path + PreProcessData.LINKS
        self.movies_path = movielens_path + PreProcessData.MOVIES
        self.ratings_path = movielens_path + PreProcessData.RATINGS
        self.tags_path = movielens_path + PreProcessData.TAGS

        self.movies_new_path = movielens_path + 'movies_new.csv'
        self.genres_new_path = movielens_path + 'genres.csv'
        self.ratings_new_path = movielens_path + 'ratings_new.csv'
        self.tags_new_path = movielens_path + 'tags_new.csv'

    def handle_movies_links(self, movies_out='movies_new.csv', genres_out='genres.csv'):
        self.movies_new_path = self.movielens_path + movies_out
        self.genres_new_path = self.movielens_path + genres_out

        movies_out_writer = csv.writer(open(self.movies_new_path, 'w', newline='', encoding='UTF-8'))
        genres_out_writer = csv.writer(open(self.genres_new_path, 'w', newline='', encoding='UTF-8'))

        links_csv = csv.reader(open(self.links_path, encoding='UTF-8'))
        movies_csv = csv.reader(open(self.movies_path, encoding='UTF-8'))

        movies_header = movies_csv.__next__()
        links_header = links_csv.__next__()
        movies_out_writer.writerow(movies_header[0:2] + links_header[1:])
        genres_out_writer.writerow(['id'] + movies_header[0:1] + movies_header[2:3])

        id_ = 1
        for link_items, movie_items in zip(links_csv, movies_csv):

            movies_out_writer.writerow(movie_items[0:2] + link_items[1:])

            movie_id = movie_items[0:1]
            for genre in movie_items[2].split('|'):
                genres_out_writer.writerow([id_] + movie_id + [genre])
                id_ += 1

    def handle_ratings(self, ratings_out='ratings_new.csv'):
        self.ratings_new_path = self.movielens_path + ratings_out
        self._handle_ids(self.ratings_path, self.ratings_new_path)

    def handle_tags(self, tags_out='tags_new.csv'):
        self.tags_new_path = self.movielens_path + tags_out
        self._handle_ids(self.tags_path, self.movielens_path + tags_out)

    def _handle_ids(self, in_path, out_path):
        f = open(in_path, encoding='UTF-8')
        o = open(out_path, 'w', encoding='UTF-8')

        header = f.readline()
        header = 'id,' + header

        content = [header]
        for id_, l in enumerate(f.readlines(), 1):
            content.append(str(id_) + ',' + l)

        o.writelines(content)
        f.close()
        o.close()

    def movies_to_json(self, movies_out='movies.json'):
        self._csv_to_json(self.movies_new_path, self.movielens_path + movies_out, [int, str, str, int])

    def genres_to_json(self, genres_out='genres.json'):
        self._csv_to_json(self.genres_new_path, self.movielens_path + genres_out, [int, int, str])

    def ratings_to_json(self, ratings_out='ratings.json'):
        self._csv_to_json(self.ratings_new_path, self.movielens_path + ratings_out, [int, int, int, float, int])

    def tags_to_json(self, tags_out='tags.json'):
        self._csv_to_json(self.tags_new_path, self.movielens_path + tags_out, [int, int, int, str, int])

    def _csv_to_json(self, in_path, out_path, type_list, line_cache=1):
        f_csv = csv.reader(open(in_path, encoding='UTF-8'))
        o = open(out_path, 'w', encoding='UTF-8')

        header = f_csv.__next__()

        res = []
        for item in f_csv:
            item_d = {}
            for i, type_ in enumerate(type_list):
                if item[i] == '':
                    continue
                item_d[header[i]] = type_(item[i])
            res += [item_d]

        json.dump(res, o, indent=4)
        o.close()


if __name__ == '__main__':
    ml_path = sys.argv[1]
    pre_process_data = PreProcessData(ml_path)

    pre_process_data.handle_movies_links()
    pre_process_data.handle_ratings()
    pre_process_data.handle_tags()

    pre_process_data.movies_to_json()
    pre_process_data.genres_to_json()
    pre_process_data.ratings_to_json()
    pre_process_data.tags_to_json()
