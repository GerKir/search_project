**Create docker image:**

docker build -t postgres-search .

**Run container:**

docker run --name postgres-search-container -d -p 5555:5432 postgres-search