load ('movie.mat');

a = load ('top3002.txt','-ascii');
top3002Matrix=zeros(1821,1821);
[d,n] = size(a);
for i=1:1:d
    top3002Matrix(find(movie==a(i,1)),find(movie==a(i,2)))=a(i,3); 
end