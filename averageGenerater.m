load ('rawRatingMatrix');
[m,n] = size(matrix);
for i = 1:n
    user_average(i) = sum(matrix(:,i))/ length(matrix(matrix(:,i)~=0));
end

for i = 1:m
    movie_average(i) = sum(matrix(i,:))/ length(matrix(matrix(i,:)~=0));
end