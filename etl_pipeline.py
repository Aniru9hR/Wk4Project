import requests
import pandas as pd
import numpy as np

connection = 'postgresql://plyquqmz:0ddQrmMaG14JHE65LcNoCvCgfFFmZnMf@peanut.db.elephantsql.com/plyquqmz'


def get_disney_data():
    base_url = 'https://api.disneyapi.dev/character?page=1&pageSize=7500'
    response = requests.get(base_url)
    print(response)
    if response.ok:
        resp = response.json()
        d = resp.get("data")
        total_list = []
        for x in d:
            character = {"name": x.get("name"),
                         "films": x.get("films"),
                         "tv_shows": x.get("tvShows"),
                         "short_films": x.get("shortFilms"),
                         "allies": x.get("allies"),
                         "enemies": x.get("enemies"),
                         "park_attractions": x.get("parkAttractions")
                         }
            df = pd.DataFrame.from_dict(character, orient='index').transpose()
            total_list.append(df)
        final_df = pd.concat(total_list)
        return final_df
    else:
        return response
    

def wrangle_data(disney_df):
    disney_df['films_count'] = disney_df.films.apply(len)
    disney_df['tv_shows_count'] = disney_df.tv_shows.apply(len)
    disney_df['short_films_count'] = disney_df.short_films.apply(len)
    disney_df['park_attractions_count'] = disney_df.park_attractions.apply(len)
    disney_df['enemies_count'] = disney_df.enemies.apply(len)
    disney_df['allies_count'] = disney_df.allies.apply(len)
    print(disney_df)
    disney_df.loc[disney_df.park_attractions.str.len() == 0, "park_attractions"] = np.nan
    disney_df.loc[disney_df.enemies.str.len() == 0, "enemies"] = np.nan
    disney_df.loc[disney_df.allies.str.len() == 0, "allies"] = np.nan
    disney_df.loc[disney_df.films.str.len() == 0, "films"] = np.nan
    disney_df.loc[disney_df.tv_shows.str.len() == 0, "tv_shows"] = np.nan
    disney_df.loc[disney_df.short_films.str.len() == 0, "short_films"] = np.nan
    disney = disney_df.reset_index(drop=True)
    return disney


disney_df = get_disney_data()
disney = wrangle_data(disney_df)
disney.to_sql('disney_char', con=connection, schema=None, if_exists='fail',
              index=True, index_label=None, chunksize=None,
              dtype=None, method=None)
print(disney)
disney.to_csv('disney1.csv')
