import requests
import pandas as pd
from sqlalchemy import create_engine

#   EXTRACT the red wines data from the given api

print("EXTRACT")
url = "https://api.sampleapis.com/wines/reds"
data = requests.get(url).json()
df = pd.DataFrame(data)

#   TRANSFORM the data as per your needs
#   in this case we are performing some code cleaning and picking certain columns from the data
print("TRANSFORM")
df['country'] = df['location'].apply(lambda x: x.split("\n·\n")[0].strip())
df['city'] = df['location'].apply(lambda x: x.split("\n·\n")[1].strip() if "\n·\n" in x else None)

ratings =[]
reviews =[]
for rating in data:
    ratings.append(float(rating["rating"]["average"]))
    reviews.append(int(rating["rating"]["reviews"].split()[0]))

df["avg_rating"] = ratings
df["total_reviews"] = reviews

df.drop(columns ="rating", inplace=True)

# LOAD the updated data into the Database which in this case is SQLlite

print("LOAD")
disk_engine = create_engine("sqlite:///my_data-pipeline.db")
df.to_sql('wine_table',disk_engine, if_exists='replace')