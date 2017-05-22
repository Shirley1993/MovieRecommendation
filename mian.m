%  1821movie  28978 user
a = load ('TrainingRatings.txt','-ascii');
c=int32(a);
matrix=zeros(1821,28978); 
movie=unique(c(:,1));
user= unique(c(:,2));
%index=1821:1;
%index1=index(movie);
for i=1:1:3255352
    matrix(find(movie==c(i,1)),find(user==c(i,2)))=c(i,3); 
end