
```
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /workspaces/DE-Zoomcamp-2024/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

# How to run the docker container for the postgres database --> this just creates a Postgres database.
# In order to do things with it in a GUI we need PG Admin (another tool), or we can use a CLI tool called PG CLI
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

# This connects to the database with PG CLI (Postgres command line interface)
`pgcli -h localhost -p 5432 -u root -d ny_taxi`

# This is the file as a parquet file
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

# This is the file as a csv
`wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz`

# How to unzip a .gz file
`gunzip yellow_tripdata_2021-01.csv.gz`

# Reading the first 100 rows of the dataset
`head -n 100 yellow_tripdata_2021-01.csv`

# Data dictionary
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# The docker image for PG admin
https://hub.docker.com/r/dpage/pgadmin4/

# Command to pull it
`docker pull dpage/pgadmin4`

# The docker commands to run PG admin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```
# The problem now is that PG Admin can't connect to the existing Postgres database because they are in 2 separate, disconnected containers
Solution: create a network and connect them to the same network
`docker network create pg-network`

Now we need to re-run the postgres container with the network configuration
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

And then also re-run the PG Admin container with the network configuration

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

# Now we'll run our Python script to ingest the data from the source
This is the "E" (Extract) in ETL (Extract, Transform, Load)
```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

# !! The Dockerfile has been updated so now we run it again to build the image
`docker build -t taxi_ingest:v001 .`

Now the docker image is built, we can run it using the same command as above:

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```

## TODO 24/01/2024
- Add the `zones` table to the database
- Add the green taxi data to the database


# 25/01/2021: GCP VM instance setup
I can also just do `ssh de-zoomcamp` as I have added this to my ssh config file already