import sys
import os
import csv


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

    def handle_movies_links(self, movies_out='movies_new.csv', genres_out='genres.csv'):
        movies_out_writer = csv.writer(open(self.movielens_path + movies_out, 'w', newline='', encoding='UTF-8'))
        genres_out_writer = csv.writer(open(self.movielens_path + genres_out, 'w', newline='', encoding='UTF-8'))

        links_csv = csv.reader(open(self.links_path, encoding='UTF-8'))
        movies_csv = csv.reader(open(self.movies_path, encoding='UTF-8'))

        movies_header = movies_csv.__next__()
        links_header = links_csv.__next__()
        movies_out_writer.writerow(movies_header[0:2] + links_header[1:])
        genres_out_writer.writerow(movies_header[0:1] + movies_header[2:3])

        for link_items, movie_items in zip(links_csv, movies_csv):

            movies_out_writer.writerow(movie_items[0:2] + link_items[1:])

            movie_id = movie_items[0:1]
            for genre in movie_items[2].split('|'):
                genres_out_writer.writerow(movie_id + [genre])

    def handle_ratings(self, ratings_out='ratings_new.csv'):
        self._handle_ids(self.ratings_path, self.movielens_path + ratings_out)

    def handle_tags(self, tags_out='tags_new.csv'):
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


if __name__ == '__main__':
    ml_path = sys.argv[1]
    pre_process_data = PreProcessData(ml_path)
    pre_process_data.handle_movies_links()
    pre_process_data.handle_tags()
    pre_process_data.handle_ratings()
