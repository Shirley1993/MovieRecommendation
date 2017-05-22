load ('top7002Matrix.mat');
load ('rawRatingMatrix');
load ('movie.mat');
load ('user.mat');
load ('userAverage.mat');
load ('movieAverage.mat');

user_total_average = mean(user_average);

for i = 1:1821
    if(me(i) > 0)
        continue;
    end
    
    user_ratings = me;
    user_rated_index = find(user_ratings>0);
    
    movie_index = i;
    movie_top7002 = top7002Matrix(movie_index,:);
    movie_top7002_index = find(movie_top7002 > 0);
    
    overlapped_index = intersect(user_rated_index , movie_top7002_index);
    overlapped_similarity = movie_top7002(overlapped_index);
    overlapped_ratings = user_ratings(overlapped_index);
    
    predict_rating(i) = overlapped_similarity * overlapped_ratings / sum(overlapped_similarity);
end
[index,b]=sort(predict_rating,'descend');
movie_id_index = movie(b);