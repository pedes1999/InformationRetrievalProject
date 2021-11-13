from elasticsearch import Elasticsearch, helpers
import csv
# Create Client
es = Elasticsearch(host = "localhost", port = 9200)
# Open csv file , upload
with open('BX-Books.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='books')