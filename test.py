import requests
          
dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
offset = 200
url = "https://data.gov.sg/api/action/datastore_search?resource_id=" + dataset_id + '&offset=' + str(offset)
        
response = requests.get(url)
with open(f'./data/hdb_listings_offset_{offset}.json', 'w') as f:
    f.write(response.content.decode())