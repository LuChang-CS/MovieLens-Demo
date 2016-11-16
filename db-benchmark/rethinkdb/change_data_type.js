r.db('MovieLens').table('movies').update(function(doc){
    return doc.merge({
        movieId: doc('movieId').coerceTo('number'),
        tmdbid: doc('tmdbId').coerceTo('number')
    });
});

r.db('MovieLens').table('genres').update(function(doc){
    return doc.merge({
        id: doc('id').coerceTo('number'),
        movieId: doc('movieId').coerceTo('number'),
    });
});

r.db('MovieLens').table('ratings').update(function(doc){
    return doc.merge({
        id: doc('id').coerceTo('number'),
        userId: doc('userId').coerceTo('number'),
        movieId: doc('movieId').coerceTo('number'),
        rating: doc('rating').coerceTo('number'),
        timestamp: doc('timestamp').coerceTo('number')
    });
});

r.db('MovieLens').table('tags').update(function(doc){
    return doc.merge({
        id: doc('id').coerceTo('number'),
        userId: doc('userId').coerceTo('number'),
        movieId: doc('movieId').coerceTo('number'),
        timestamp: doc('timestamp').coerceTo('number')
    });
});
