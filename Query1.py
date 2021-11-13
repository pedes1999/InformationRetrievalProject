from elasticsearch import Elasticsearch

def queryInput(user_input):
    results=es.search(index="books" , query = {"match" :{"book_title":"{}".format(user_input)}} , size = 50)
    hits = results['hits']['total']['value']
    print("Got {} Hits:".format(hits))
    try:
        for x in range(hits):
            print(x+1,')Book Title: ',results['hits']['hits'][x]['_source']['book_title'],"\n   Similarity Score: " ,results['hits']['hits'][x]['_score'])
    except:
        for x in range(50):
            print(x+1,')Book Title: ',results['hits']['hits'][x]['_source']['book_title'],"\n   Similarity Score: " ,results['hits']['hits'][x]['_score'])
    return

es = Elasticsearch()

print('Input "/stop" if you wanna stop the script')
user_input = input("Please input Book to search for: ")

while(user_input != '/stop'):
    queryInput(user_input)
    print('Type "/stop" if you wanna stop the search')
    user_input = input("Please input Book to search for: ")
