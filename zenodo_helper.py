import requests
from datetime import datetime
import urllib
import unidecode
import json
from dateutil import parser
from bs4 import BeautifulSoup
from rdflib import Dataset, Namespace, URIRef, RDFS, Literal,RDF
from urllib.parse import quote
import regex as re
from io import BytesIO
import io
from okapi_api import okapi_login, okapi_logout, sparql_search, sparql_admin_internal, get_corpus, set_corpus, set_individual, get_individual, sparql_construct


def get_zenodo_collections(okapi_url, opener):
    """
    Récupérer les metadata de données par l'api Zenodo
    1. lire le fichier qui contient les id de données 
    2. avec requête query_lisatData connecte l'api de Zenodo
    3. récupérer : idData : DOI de collection, json_data : metadonnée de la collection
    
    """
    answer = sparql_search(okapi_url, """
            PREFIX core: <http://www.ina.fr/core#>
            PREFIX lacas: <http://lacas.inalco.fr/>
            SELECT distinct ?id
            WHERE {
              ?uri a lacas:CorpusZenodo .
              BIND (STRAFTER(str(?uri), "https://zenodo.org/records/") AS ?id)
            }""", opener)
    data_list = []
    for item in answer:
        idData = item['id']['value']
        print(idData)
        query_listData = f"https://zenodo.org/api/records?q=doi:10.5281/zenodo.{idData}"
        try:
            response = requests.get(query_listData)
            response.raise_for_status()
            json_data = response.json()
            data_list.append((idData, json_data))
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for DOI:{idData}, error: {e}")
    return data_list



# def fetch_data_from_file(fileList):
#     data_list = []
#     with open(fileList, 'r', encoding='utf-8') as file:
#         for line in file:
#             idData = line.strip()
#             print(idData)
#             query_listData = f"https://zenodo.org/api/records?q=doi:10.5281/zenodo.{idData}"
#             try:
#                 response = requests.get(query_listData)
#                 response.raise_for_status()
#                 json_data = response.json()
#                 data_list.append((idData, json_data))
#             except requests.exceptions.RequestException as e:
#                 print(f"Failed to fetch data for DOI:{idData}, error: {e}")
                
#     return data_list
    

def detecte_one_valid_url(json_data): 
    """
    Vérifier si la collection contient au moins une URL valide
    listFormat contient les types de données que Okapi ne peut pas traiter
    Boolen : 
    Trouevr une URL valide Return True
    Itérer sur toutes les URLs de média dans la collection, aucune url valide Return False
    """
    listFormat = ["txt", "zip", "csv", "json", "xlsx", "doc", "docx", "owl", "sql", "gz", "xls", "html"]
    if "hits" in json_data and "hits" in json_data["hits"] and len(json_data["hits"]["hits"]) > 0:
        for item in json_data["hits"]["hits"]:
            for file in item.get("files", []):
                url = file['links'].get('self',[])
                if ' ' not in url and all(item not in url for item in listFormat):
                    return True 
                    
    return False

                

def get_metadataCollection(json_data):
    """
    Récupérer les metadatas de donnée au niveau de collection
    """
    metadataCollection_list = []
    if detecte_one_valid_url(json_data):
        for item in json_data["hits"]["hits"]:
            doi_url = item.get("doi_url", "")
            doi = item.get("doi", "")
            doi_collection = doi.split('/') # doi = 10.5281/zenodo.10547022
            doi_string= doi_collection[1] # [1] = zenodo.10547022
            doi_string_record = doi_string.split('.')[1] # [1] = 10547022
            collection_uri = "https://zenodo.org/records/" + doi_string_record # https://zenodo.org/records/10547022
            time_update_zenodo = item.get("modified", None)
            if time_update_zenodo:
                try: 
                    time_update_zenodo = datetime.fromisoformat(time_update_zenodo)
                except ValueError:
                    print("Not information updateTime")
                    time_update_zenodo = None
                    
            metadataCollection_list.append((doi, doi_url, collection_uri, time_update_zenodo))

    return metadataCollection_list
            

def get_metadataMedia(json_data, collection_uri):
    """
    Récupérer les metadatas de donnée au niveau de média
    """
    metadataMedia_list = []
    if detecte_one_valid_url(json_data):
        for item in json_data["hits"]["hits"]:
            for file in item.get("files", []):
                title = file.get("key", "")
                identifier = file.get("id", "")
                media_url = file["links"].get("self","")
                media_uri = collection_uri +"/"+ identifier if identifier else ""
                media_segment_uri = media_uri + "/segment" if media_uri else ""
                
                metadataMedia_list.append((title, identifier, media_url, media_uri, media_segment_uri))
                
    return metadataMedia_list

# def updateTime_collection(collection_uri, okapi_url, opener, kb, time_update_zenodo, base_url):
#     if okapi_exists(collection_uri, okapi_url, opener):
#         result_media = get_corpus(base_url, URIRef(collection_uri), kb=kb, opener=opener, write="true")
#         for (_,p,o,g) in kb.quads((URIRef(collection_uri), None, None, None)) :               
#             if str(p) == "http://www.ina.fr/core#lastUpdateDate":
#                 time_update_lacas = str(o)
#                 time_update_lacas = datetime.fromisoformat(time_update_lacas)
#                 if time_update_zenodo > time_update_lacas :
#                     return True                   

#     return False



def get_collection(collection_uri, okapi_url, opener, kb,  base_url):
    """
    Vérifier si la collection exist dans la base de donnée kb
    get_courpus : fonction qui récupérer les triplets de collection 

    """
    if okapi_exists(collection_uri, okapi_url, opener):
        if get_corpus(base_url, URIRef(collection_uri), kb=kb, opener=opener, write="true") :
            return True                   

    return False
                 
                    

# def updateTime_media(media_segment_uri, okapi_url, opener, media_uri, kb, time_update_zenodo, base_url):  
#     if okapi_exists(media_segment_uri, okapi_url, opener):
#         result_media = get_media(base_url, URIRef(media_uri), kb=kb, opener=opener, write="true")
#         for (_,p,o,g) in kb.quads((URIRef(media_segment_uri), None, None, None)) :               
#             if str(p) == "http://www.ina.fr/core#lastUpdateDate":
#                 time_update_lacas = str(o)
#                 time_update_lacas = datetime.fromisoformat(time_update_lacas)
#                 if time_update_zenodo > time_update_lacas :
#                     return True                    
#     return False                

def get_media(media_uri, okapi_url, opener, kb,  base_url):
    """
    Vérifier si les exist dans la base de donnée kb
    get_courpus : fonction qui récupérer les triplets de média
    
    """
    if okapi_exists(media_uri, okapi_url, opener):
        if get_media(base_url, URIRef(media_uri), kb=kb, opener=opener, write="true") :
            return True                   

    return False

def remove_collection_data(data_collection_list, collection_uri, kb):
    """
    Supprimmer toutes les triplets au niveau de la collection
    collection_properties_to_delete : liste des propriétés de donnée au niveau de collection

    """
    collection_properties_to_delete = ["http://purl.org/dc/elements/1.1/subject","http://purl.org/dc/elements/1.1/rights","http://www.ina.fr/core#lastUser","http://www.ina.fr/core#fulltextSearchLabel",
                   "http://www.ina.fr/core#affiliation","http://www.campus-AAR.fr/resource_575227804","http://purl.org/dc/elements/1.1/title","http://www.ina.fr/core#creationDate",
                   "http://campus-aar.fr/asa#description","http://www.ina.fr/core#owner","http://www.ina.fr/core#lastUpdateDate","http://www.campus-AAR.fr/resource_876211265",
                   "http://www.campus-AAR.fr/resource_1338016320","http://purl.org/dc/elements/1.1/language","http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                   'http://www.ina.fr/core#thumbnailUrl',"http://campus-aar.fr/collection#AAI", "http://www.ina.fr/core#creationDate"]
    for (_,p,o,g) in kb.quads((URIRef(collection_uri), None, None, None)) :
        if str(p) in collection_properties_to_delete :
            print((URIRef(collection_uri), p, o, URIRef(collection_uri)))
            kb.remove((URIRef(collection_uri), p, o, URIRef(collection_uri)))
    return kb
    

def remove_media_data(data_media_list, kb):
    """
    Supprimmer toutes les triplets au niveau de la média
    media_properties_to_delete : liste des propriétés de donnée au niveau de média

    """
    media_properties_to_delete = ["http://campus-aar.fr/collection#AAI","http://www.ina.fr/core#thumbnailUrl","http://www.ina.fr/core#fulltextSearchLabel","http://www.ina.fr/core#element",
                                               "http://www.campus-AAR.fr/resource_575227804","http://www.campus-AAR.fr/resource_876211265","http://purl.org/dc/elements/1.1/rights","http://www.ina.fr/core#creationDate",
                                               "http://campus-aar.fr/asa#description","http://www.ina.fr/core#owner"]
    for media_url, media_uri, media_segment_uri in data_media_list:
        for (_,p,o,g) in kb.quads((URIRef(media_segment_uri), None, None, None)) :
            if str(p) in media_properties_to_delete :
                kb.remove((URIRef(media_segment_uri), p, o, URIRef(media_segment_uri)))
    return kb
    
def add_uri_creator(okapi_url, opener, name) :
    """
    Trouver ou créer l'uri pour un auteur 
    1. Récupérer le nom de famille et le prénom d'un auteur
    2. Chercher l'uri selon la requête find_uri_creator
    3. S'il n'existe pas, créer une uri 
    """
    firstName, lastName = parse_name(name)
    uriExi_creator = find_uri_creator(okapi_url, opener, lastName, firstName)
    
    if uriExi_creator is not None :
        uri_creator = uriExi_creator
    else:
        first_name = unidecode.unidecode(firstName).lower().strip().replace(' ', '_')
        last_name = unidecode.unidecode(lastName).lower().strip().replace(' ', '_')
        
        uri_creator = "http://lacas.inalco.fr/resource/" + first_name + "_" + last_name
    
    return uri_creator, firstName, lastName
    
def compute_fulltext_index(base_url, portalUri, opener, targetProperty = 'http://www.ina.fr/core#fulltextSearchLabel'):
    url = base_url + "/api/saphir/compute_fulltext_property?targetProperty=" + urllib.parse.quote(targetProperty) + "&portalUri=" + urllib.parse.quote(portalUri)
    response = opener.open(url).read().decode()
    return response

def okapi_exists(uri, okapi_url,opener):
    answer = sparql_construct(okapi_url, """
                        PREFIX lacas: <http://lacas.inalco.fr/>
                        ASK {
                          graph <""" + uri + """> {<""" + uri + """> a ?t}
                        }""", opener)
    return bool(json.loads(answer)["boolean"])

def get_corpus(base_url, individual_ref, kb, opener, write="true", labels="false", format="rdf"):
    url = base_url + "/api/saphir/get_individual?uri="\
          + urllib.parse.quote(URIRef(individual_ref).toPython())\
          + "&labels=" + labels \
          + "&lock=" + write \
          + "&format=" + format
    response = opener.open(url).read().decode()
    #print("check the result of response :",response)
    if response != 'error_unknown_uri':
        if format == "rdf":
            kb.parse(format='trig', data=response, publicID="http://toDelete/")
            # update the fake uris with toDelete base by the initial empty one
            for (s, p, o, g) in kb.quads((None, URIRef("http://www.ina.fr/core#hasType"), None, None)):
                if o.toPython().startswith("http://toDelete/"):
                    kb.add((s, p, URIRef(o.toPython().replace("http://toDelete/", "")), g))
                    kb.remove((s, p, o, g))
        else:
            # todo MUST BE completed to handle typed literals !!!
            parsed_json = json.loads(response)
            for result in parsed_json[0]["results"]["bindings"]:
                kb.add((URIRef(result["s"]["value"]) if result["s"]["type"] == 'uri' else Literal(result["s"]["value"]),
                        URIRef(result["p"]["value"]) if result["p"]["type"] == 'uri' else Literal(result["p"]["value"]),
                        URIRef(result["o"]["value"]) if result["o"]["type"] == 'uri' else Literal(result["o"]["value"]),
                        URIRef(result["g"]["value"]) if result["g"]["type"] == 'uri' else Literal(result["g"]["value"])))

        # remove technical triples added by the server to specify editable layers
        for (s, p, o, g) in kb.quads((None, URIRef("http://www.ina.fr/core#is_editable"), None, None)):
                kb.remove((s, p, o, g))

 
        return kb

def get_media(base_url, media_ref, kb, opener, write="true", format="rdf",  withComputed="false"):
    url = base_url + "/api/saphir/get_media?media="\
          + urllib.parse.quote(media_ref.toPython())\
          + "&lock=" + write \
          + "&withComputed=" + withComputed \
          + "&format=" + format
    response = opener.open(url).read().decode()
    if response == "already_locked" :
        print("Data is locked")
        return
    
    if response != 'error_unknown_uri':
        if format == "rdf":
            kb.parse(format='trig', data=response, publicID="http://toDelete/")
            # update the fake uris with toDelete base by the initial empty one
            for (s, p, o, g) in kb.quads((None, URIRef("http://www.ina.fr/core#hasType"), None, None)):
                if o.toPython().startswith("http://toDelete/"):
                    kb.add((s, p, URIRef(o.toPython().replace("http://toDelete/", "")), g))
                    kb.remove((s, p, o, g))
        else:
            # todo MUST BE completed to handle typed literals !!!
            parsed_json = json.loads(response)
            for result in parsed_json[0]["results"]["bindings"]:
                kb.add((URIRef(result["s"]["value"]) if result["s"]["type"] == 'uri' else Literal(result["s"]["value"]),
                        URIRef(result["p"]["value"]) if result["p"]["type"] == 'uri' else Literal(result["p"]["value"]),
                        URIRef(result["o"]["value"]) if result["o"]["type"] == 'uri' else Literal(result["o"]["value"]),
                        URIRef(result["g"]["value"]) if result["g"]["type"] == 'uri' else Literal(result["g"]["value"])))

        # remove technical triples added by the server to specify editable layers
        for (s, p, o, g) in kb.quads((None, URIRef("http://www.ina.fr/core#is_editable"), None, None)):
                kb.remove((s, p, o, g))
        return True
    else:
        return False
    
def set_media(base_url, media_ref, media_url, identifier, mimeType, segmentType, threshold, media_segment_ref,kb, opener,
              unlock="true"):
    url = base_url + "/api/saphir/set_media?uri=" + urllib.parse.quote(media_ref.toPython()) +\
          "&url=" + urllib.parse.quote(media_url) +\
          "&identifier=" + identifier +\
          "&mimetype=" + mimeType +\
          "&segmenttype=" + urllib.parse.quote(segmentType) +\
          "&threshold=" + threshold +\
          "&unlock=" + unlock
    #print("url: ", url)
    trig_string = kb.graph(media_segment_ref.toPython()).serialize(format='trig', base='.', encoding='utf-8')
    req = urllib.request.Request(url, trig_string, {'Content-Type': 'application/trig; charset: UTF-8'})
    op = urllib.request.urlopen(req)
    return op.read().decode()

# deux fonctions check_datatype à vérifier le format de données 
# parce que les images pouvent afficher leurs contenus directemnt sur lacas
# les videos, radios ,pdfs sont affichés avec des icons

def check_datatype(url):
    """
    Vérifier le format de la média
    Attribuer la valeur à ces deux varaibles : mimetype, media_segment_type
    """
    imagette_url = None
    data_type = url.split('.')[-1].split('/')[0].lower()
    if data_type in ['mp4', 'mov', 'avi', 'wmv', 'flv', 'avchd']:
        mimetype = "video/mp4"
        media_segment_type = "http://campus-aar.fr/asa#ASAIngestionVideo"
        imagette_url = "https://cdn.pixabay.com/photo/2023/01/12/04/36/cinema-7713265_1280.png"
    elif data_type == "mp3":
        mimetype = "audio/mp3"
        media_segment_type = "http://campus-aar.fr/asa#ASAIngestionSound"
        imagette_url = "https://cdn.pixabay.com/photo/2014/03/25/15/23/speaker-296661_1280.png"
    elif data_type == "pdf":
        mimetype = "application/pdf"
        media_segment_type = "http://campus-aar.fr/asa#ASAIngestionText"
        imagette_url = "https://cdn.pixabay.com/photo/2012/04/02/16/55/adobe-24943_1280.png"
    else:
        mimetype = None
        media_segment_type = None
    return mimetype, media_segment_type, imagette_url


def check_datatype_image(idData,url):
    """
    Vérifier le type de média

    """
    imagette_url = None
    data_type = url.split('.')[-1].split('/')[0].lower()
    #print(data_type)
    if data_type in ['jpg', 'jpeg', 'png', 'gif', 'tiff', 'heif', 'raw']:
        query_listData = f"https://zenodo.org/api/records?q=doi:10.5281/zenodo.{idData}"
        response = requests.get(query_listData)
        json_data = response.json()
        hits_data = json_data["hits"]
        for item in json_data["hits"]["hits"]:
            for file in item["files"] : 
                imagette_url = file['links']["self"]
                mimetype = "image/jpeg"
                media_segment_type = "http://campus-aar.fr/asa#ASAIngestionImage"
    else:
        mimetype = None
        media_segment_type = None
            
    return mimetype, media_segment_type,imagette_url



def parse_name(name):
    """
    Formaliser la fomre de nom d'auteur : Nome de famille, prénom 
    Exemple : Stockinger, Peter
    """
    name = name.strip()
    if ',' in name :
        parts = name.split(',')
        lastName = parts[0].strip()
        firstName = parts[1].strip() if len(parts) > 1 else ''
    else :
        parts = name.split()
        if len(parts)  == 1:
            lastName = parts[0]
            firstName = ''
        else:
            firstName = parts[0]
            lastName = ' '.join(parts[1:])

    return firstName,lastName


def find_uri_creator(okapi_url, opener, lastname, firstname): 
    """
    Récupérer l'uri d'auteur
    """
    query = """?lab bif:contains "'""" + lastname + """' AND '"""+firstname+"""'".""" if len(lastname) > 0 and len(firstname) > 0 \
        else """?lab bif:contains "'""" + lastname + """'".""" if len(lastname) > 0 else """?lab bif:contains "'""" + firstname + """'"."""
    answer = sparql_search(okapi_url, """
	      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	      PREFIX owl: <http://www.w3.org/2002/07/owl#>
	      PREFIX core: <http://www.ina.fr/core#>
	      SELECT distinct ?individual 
          WHERE {
		    ?individual a core:PhysicalPerson OPTION (inference "http://campus-aar/owl") .
		    ?individual a core:CommonKnowledge .
		    ?individual rdfs:label ?lab ."""
           + query + """}""", opener)
    if len(answer) == 0: 
        return None
    elif len(answer) > 1: 
        return "duplicate"
    else :    
        return answer[0]['individual']['value']


def find_uri_affiliation(okapi_url, opener, organisation): 
    """
    Récupérer l'uri d'affiliation
    """
    answer = sparql_search(okapi_url, """
	      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
	      PREFIX owl: <http://www.w3.org/2002/07/owl#>
	      PREFIX core: <http://www.ina.fr/core#>
	      SELECT distinct ?individual 
          WHERE {
		    ?individual a core:affiliation OPTION (inference "http://campus-aar/owl") .
		    ?individual a core:CommonKnowledge .
		    ?individual rdfs:label ?lab .

            ?lab bif:contains "'""" +organisation+""" '" .
    }	      
    """, opener)
   
    if len(answer) == 0:  # 0 means this person does not exist on site lacas
        return None
    else :
        return answer[0]['individual']['value'] 
    

def add_uri_affiliation(okapi_url, opener, affiliation) :
    """
    Trouver ou créer l'uri pour une affiliation 
    1. Chercher l'uri d'affiliation selon la requête find_uri_affiliation
    3. S'il n'existe pas, créer une uri 
    """
    uriExi_affiliation = find_uri_affiliation(okapi_url, opener, affiliation)
    if uriExi_affiliation is not None :
        uri_affiliation = uriExi_affiliation
    else :
        affiliation_uni = unidecode.unidecode(affiliation.lower().strip())
        affiliation_uni = re.sub(' ', '_', affiliation)
        uri_affiliation = "http://lacas.inalco.fr/resource/" + affiliation_uni

    return uri_affiliation

def convert_metadata_to_triplet(json_data, kb, collection_uri, okapi_url, opener, idData, doi, doi_url):
    """
    Convertir les métadata de JSON en RDF et intégrer dans la base de données kb
    1. Récupèrer les metadata au niveau de la collection et convertir en RDF
    2. Vérifier le type de donnée : image ou pas
    3. Récupèrer les metadonnée au niveau de la média et convertir en RDF
    """
    core = Namespace("http://www.ina.fr/core#")
    asa= Namespace("http://campus-aar.fr/asa#")
    dc= Namespace("http://purl.org/dc/elements/1.1/")
    
    for item in json_data["hits"]["hits"]:
        #record_id = item.get("id")
        name_collection = item.get("title", "")
        
        description = item["metadata"].get("description", None)
        soup = BeautifulSoup(description, 'html.parser')
        description = soup.get_text()
        kb.add((URIRef(collection_uri), URIRef('http://campus-aar.fr/asa#description'), 
                                        Literal((str(description)).strip()), URIRef(collection_uri)))
        
        publication_date = item["metadata"].get("publication_date", None)
        kb.add((URIRef(collection_uri), asa.HALModificationDate ,
                                            Literal((str(publication_date)).strip()), URIRef(collection_uri)))
                                            
        language = item["metadata"].get("language", None)
        kb.add((URIRef(collection_uri), URIRef('http://purl.org/dc/elements/1.1/language'), 
                                        Literal((str(language)).strip()), URIRef(collection_uri)))
         
        licence = item["metadata"]["license"].get("id", None)
        kb.add((URIRef(collection_uri), URIRef('http://purl.org/dc/elements/1.1/rights'), 
                                        Literal((str(licence)).strip()), URIRef(collection_uri)))
                                        
        for word in item["metadata"].get("keywords", []):   
            kb.add((URIRef(collection_uri), URIRef('http://purl.org/dc/elements/1.1/subject'), Literal((str(word)).strip()), URIRef(collection_uri)))
        
        for name_community in item["metadata"].get("communities", []):
            kb.add((URIRef(collection_uri), URIRef('http://www.campus-AAR.fr/resource_1338016320'), 
                                            Literal((str(name_community)).strip()), URIRef(collection_uri)))
                                            
        for i in item["metadata"]["creators"] :
            name = i["name"]
            if name:
                uri_creator, lastName, firstName = add_uri_creator(okapi_url, opener, name)
                if uri_creator : 
                    data_media_list = get_metadataMedia(json_data, collection_uri)
                    for title, identifier, media_url, media_uri, media_segment_uri in data_media_list:
                        kb.add((URIRef(uri_creator), RDF.type, URIRef('http://www.campus-AAR.fr/resource_1271554455'),
                                                        URIRef(uri_creator)))
                        kb.add((URIRef(uri_creator), RDF.type, URIRef('http://campus-aar.fr/asa#693279f0-2e16-45f6-aea5-98c48840a2da'),
                                                        URIRef(uri_creator)))
                        kb.add((URIRef(uri_creator), RDF.type, URIRef('http://www.ina.fr/core#CommonKnowledge'),
                                                        URIRef(uri_creator)))
                        kb.add((URIRef(uri_creator), RDFS.label, Literal(str(firstName).strip() + ", " + str(lastName).strip()), URIRef(uri_creator)))
                        
                        kb.add((URIRef(uri_creator), URIRef('http://campus-aar.fr/asa#lastname'), Literal(str(firstName).strip()), URIRef(uri_creator)))
                        kb.add((URIRef(uri_creator), URIRef('http://campus-aar.fr/asa#forname'),
                                                        Literal(str(lastName).strip()), URIRef(uri_creator)))
                        
                        kb.add((URIRef(uri_creator), URIRef('http://campus-aar.fr/asa#lastname'), Literal(str(firstName).strip()), URIRef(uri_creator)))
                        kb.add((URIRef(uri_creator), URIRef('http://campus-aar.fr/asa#forname'), Literal(str(lastName).strip()), URIRef(uri_creator)))
                        
                        kb.add((URIRef(collection_uri), URIRef('http://www.campus-AAR.fr/resource_1592967362'), URIRef(uri_creator), URIRef(collection_uri)))
                        kb.add((URIRef(media_segment_uri), URIRef('http://www.campus-AAR.fr/resource_1592967362'), URIRef(uri_creator),URIRef(media_segment_uri)))
                        set_individual(okapi_url, URIRef(uri_creator), kb, opener)
                
                else :
                    kb.add((URIRef(collection_uri), URIRef('http://www.campus-AAR.fr/resource_1592967362'), URIRef(uri_creator), URIRef(collection_uri)))
                               
            affiliation = i["affiliation"]
            if affiliation :
                uri_affiliation = add_uri_affiliation (okapi_url, opener, affiliation)
                if uri_affiliation :
                    kb.add((URIRef(uri_affiliation), RDF.type, URIRef('http://www.campus-AAR.fr/resource_1271554455'),
                            URIRef(uri_affiliation)))
                    kb.add((URIRef(uri_affiliation), RDF.type,
                            URIRef('http://campus-aar.fr/asa#693279f0-2e16-45f6-aea5-98c48840a2da'),
                            URIRef(uri_affiliation)))
                    kb.add((URIRef(uri_affiliation), RDF.type, URIRef('http://www.ina.fr/core#CommonKnowledge'),
                            URIRef(uri_affiliation)))
                    kb.add((URIRef(uri_affiliation), RDFS.label,
                            Literal(str(affiliation).strip()), URIRef(uri_affiliation)))
                    
                    kb.add((URIRef(uri_creator), URIRef('http://www.ina.fr/core#affiliation'),URIRef(uri_affiliation), URIRef(media_segment_uri)))# ajouter le triplet d'affiliation d'auteur : la structrure hiérarchique
                    
                    kb.add((URIRef(uri_creator), URIRef('http://www.ina.fr/core#affiliation'), URIRef(uri_affiliation), URIRef(collection_uri)))
   
                    set_individual(okapi_url, URIRef(uri_affiliation), kb, opener)
                else:
                    kb.add((URIRef(uri_creator), URIRef('http://www.ina.fr/core#affiliation'), 
                        URIRef(uri_affiliation), URIRef(collection_uri)))
                        
        kb.add((URIRef(collection_uri), RDF.type, URIRef('http://www.ina.fr/core#CommonKnowledge'), URIRef(collection_uri)))                        
        kb.add((URIRef(collection_uri), RDFS.label, Literal(name_collection), URIRef(collection_uri)))                        
        kb.add((URIRef(collection_uri), RDF.type, URIRef('http://lacas.inalco.fr/CorpusZenodo'), URIRef(collection_uri)))                       
        kb.add((URIRef(collection_uri), URIRef('http://www.campus-AAR.fr/resource_575227804'), Literal((str(doi)).strip()), URIRef(collection_uri)))
        kb.add((URIRef(collection_uri), URIRef('http://www.campus-AAR.fr/resource_876211265'), Literal((str(doi_url)).strip()), URIRef(collection_uri)))
        kb.add((URIRef(collection_uri), URIRef('http://purl.org/dc/elements/1.1/title'), Literal((str(name_collection)).strip()), URIRef(collection_uri)))

        data_media_list = get_metadataMedia(json_data, collection_uri)
        print(data_media_list)
        #######
        ####### vérifier le type de donnée est image ou pas 
        ###### attribuer la dofférente valeur pour mimetype et media_segment_type
        for title, identifier, media_url, media_uri, media_segment_uri in data_media_list:
            #print('MEDIA URI :', media_segment_uri)
            if not okapi_exists(media_segment_uri, okapi_url, opener): # si la collection existe sur okapi, on n'a pas besion de faire les étapes suivantes
                mimetype, media_segment_type, imagette_url = check_datatype(media_url)
                if mimetype is None or media_segment_type is None :
                    mimetype, media_segment_type, imagette_url = check_datatype_image(idData,media_url)
                    #print('MIMETYPE :*****', mimetype, 'MEDIA_SEGEMENT :*******', media_segment_type)
                    if mimetype is None or media_segment_type is None :
                        continue
                
                threshold = 0.0

                kb.add((URIRef(media_segment_uri), RDF.type, URIRef(media_segment_type), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), core.collection, URIRef("http://campus-aar.fr/collection#AAI"),
                        URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://www.ina.fr/core#thumbnailUrl'), URIRef(imagette_url), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), RDFS.label, Literal(name_collection), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef("http://www.ina.fr/core#fulltextSearchLabel"), Literal(title), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://www.campus-AAR.fr/resource_575227804'), Literal((str(doi)).strip()), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://www.campus-AAR.fr/resource_876211265'), Literal((str(doi_url)).strip()), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://purl.org/dc/elements/1.1/rights'), Literal((str(license)).strip()), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://www.ina.fr/core#creationDate'), Literal((str(publication_date)).strip()), URIRef(media_segment_uri)))
                kb.add((URIRef(media_segment_uri), URIRef('http://campus-aar.fr/asa#description'), Literal((str(description)).strip()), URIRef(media_segment_uri)))
                
                kb.add((URIRef(collection_uri), URIRef('http://www.ina.fr/core#element'), URIRef(media_segment_uri), URIRef(collection_uri)))

                set_media(okapi_url, URIRef(media_uri), media_url, identifier, mimetype, media_segment_type,
                            str(threshold), URIRef(media_segment_uri), kb, opener, unlock="true")
        
        print(set_corpus(okapi_url, URIRef(collection_uri), kb, opener))