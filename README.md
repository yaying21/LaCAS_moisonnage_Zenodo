Moissonnage de données Zenodo pour le domaine SHS

**Mots-clés** : API REST ; Transformation RDF; Python; SPARQL

###### Objectifs #######
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

2. Mis à jour les BDD (vérifier les métadonnées de la collection et les médias)
   def get_metadataCollection(json_data)
   def get_collection(collection_uri, okapi_url, opener, kb, okapi_url)
   def remove_collection_data(data_collection_list, kb)

3. Récupèrer les méradonnées de chaque collection

4. Récupèrer ou créer des URIs avec des requêtes SPARQL
   
   Chercher l'uri pour les personnes et les affliation avec les requêts SPARQL. S'il n'exist pas, on le crée.

7. Transformer les données en triplets en RDF

Ajouter les triplets générés dans le graphe

