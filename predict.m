load ('top7002Matrix.mat');
load ('rawRatingMatrix');
load ('movie.mat');
load ('user.mat');
load ('userAverage.mat');
load ('movieAverage.mat');

user_total_average = mean(user_average);

a = load ('TestingRatings.txt','-ascii');
%a = load ('sampleTest.txt','-ascii');
[d,n] = size(a);

for i=1:1:d
    testsample = a(i,:);

    user_index = find(user == testsample(2));
    user_ratings = matrix(:,user_index);
    user_rated_index = find(user_ratings>0);

    movie_index = find(movie == testsample(1));
    movie_top7002 = top7002Matrix(movie_index,:);
    movie_top7002_index = find(movie_top7002 > 0);

    overlapped_index = intersect(user_rated_index , movie_top7002_index);
    overlapped_similarity = movie_top7002(overlapped_index);
    overlapped_ratings = user_ratings(overlapped_index);
    
    %if (isempty(overlapped_index))
        %predict_rating(i) = movie_average(movie_index) + user_average(user_index) - user_total_average;
    %else
        predict_rating(i) = overlapped_similarity * overlapped_ratings / sum(overlapped_similarity);
        %predict_rating(i) =  sum(overlapped_ratings) / length(overlapped_index);
    %end
end
p1 = predict_rating';
p2 = a(:,3);