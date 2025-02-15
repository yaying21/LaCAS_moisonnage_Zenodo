from datetime import datetime
from pathlib import Path
from rdflib import Dataset, Namespace, URIRef, RDFS, Literal,RDF
from zenodo_helper import get_zenodo_collections, detecte_one_valid_url, get_metadataMedia, get_metadataCollection, get_collection, remove_collection_data, get_media, remove_media_data, convert_metadata_to_triplet
from okapi_api import okapi_login, okapi_logout, sparql_search, sparql_admin_internal, get_corpus, set_corpus, set_individual, get_individual, sparql_construct, compute_fulltext_index


login = input("Votre compte : ")
passwd = input("Mot de pass : ")
okapi_url = "http://okapi-api:3010"
opener = okapi_login(okapi_url, login, passwd)
kb = Dataset()
data_list = get_zenodo_collections(okapi_url, opener)

if Path("last_zenodo_harvesting.txt").is_file():
    with open("last_zenodo_harvesting.txt", "r", encoding="utf8") as f:
        last_harvest_date = datetime.strptime(f.readline().strip(), '%Y-%m-%d') # 
else:
    last_harvest_date = datetime.strptime("2000-01-01", '%Y-%m-%d')

for idData, json_data in data_list :
    if detecte_one_valid_url(json_data) :
    #### detecter collection et ses medias et supprimer les cas qui doient être mettre à jour
        data_collection_list = get_metadataCollection(json_data) # récupérer les metadata de la collection
        print(data_collection_list)
        for doi, doi_url, collection_uri, time_update_zenodo in data_collection_list :
            if time_update_zenodo > last_harvest_date :  # vérifier la date de mise en jour de collection
                if get_collection(collection_uri, okapi_url, opener, kb, okapi_url): 
                    kb = remove_collection_data(data_collection_list, kb)
                        
            data_media_list = get_metadataMedia(json_data, collection_uri)
            for title, identifier, media_url, media_uri, media_segment_uri in data_media_list:
                if time_update_zenodo > last_harvest_date : 
                    if get_media(media_segment_uri, okapi_url, opener, media_uri,kb, okapi_url):
                        kb = remove_media_data(data_media_list, kb)
                        
        
    ###### partie de récupérer les metadonnées et transfrer en triplets
        convert_metadata_to_triplet(json_data, okapi_url, opener, idData, doi, doi_url)
 ## 
 # céer une nouvelle date de mise en jour pour le fichier txt   
if Path("last_zenodo_harvesting.txt").is_file():
    with open("last_zenodo_harvesting.txt", "w", encoding="utf8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))
print("computing full text index...")
compute_fulltext_index(okapi_url, "http://www.campus-AAR.fr/resource_339623745", opener) # 
print("done")
