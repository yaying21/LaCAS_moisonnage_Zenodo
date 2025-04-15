Moissonnage de données Zenodo pour le domaine SHS

**Mots-clés** : API REST ; Transformation RDF; Python; SPARQL, ontologie

**###### Objectifs #######**
- Automatiser la collecte de métadonnées depuis Zenodo via son API
- Mettre à jour les données dans la BDD
- Importer le graphe de connaissances dans la plateforme

**Structure du Code**

1. Connecter API de Zenodo et vérifier le conteus de métadonnée de chaque collection
   
   def get_zenodo_collections(okapi_url, opener):
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

3. Mis à jour les BDD (vérifier les métadonnées de la collection et les médias)
   def get_metadataCollection(json_data)
   
   def get_collection(collection_uri, okapi_url, opener, kb, okapi_url)
   
   def remove_collection_data(data_collection_list, kb)

4. Récupèrer les méradonnées de chaque collection

5. Récupèrer ou créer des URIs avec des requêtes SPARQL
   
   Chercher l'uri pour les personnes et les affliation avec les requêts SPARQL. S'il n'exist pas, on le crée.

   def add_uri_affiliation(okapi_url, opener, affiliation) :
    uriExi_affiliation = find_uri_affiliation(okapi_url, opener, affiliation)
    if uriExi_affiliation is not None :
        uri_affiliation = uriExi_affiliation
    else :
        affiliation_uni = unidecode.unidecode(affiliation.lower().strip())
        affiliation_uni = re.sub(' ', '_', affiliation)
        uri_affiliation = "http://lacas.inalco.fr/resource/" + affiliation_uni

    return uri_affiliation

6. Transformer les données en triplets en RDF

7. Ajouter les triplets générés dans le graphe

