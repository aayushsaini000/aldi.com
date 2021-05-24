import pandas as pd
import requests

df = pd.read_csv('GroceryoutletStores.csv')

# print(df.columns)

for index, row in df.iterrows():
    try:
        print(index)
        url = row['Latitude ']
        if 'http' not in url:
            raise Exception('Common')

        response = requests.get(url, allow_redirects=True)

        this = (
            str(response.content).split('meta content="https://')[1].split('"')[0].split('markers=')[1].split('&')[0])

        df.loc[index, 'Latitude '] = this.split('%2C')[0]
        df.loc[index, 'Longitude'] = this.split('%2C')[-1]
    except Exception as e:
        print(e)

df.to_csv('GroceryoutletStoresOutput.csv', index=False)
