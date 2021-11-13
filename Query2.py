
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch

def createResultsTable(userInput):
    results=es.search(index="books" , query = {"match" :{"book_title":"{}".format(userInput)}} , size = 50) #default 10 , to kanw 50
    hits = results['hits']['total']['value']
    print("Got {} Hits:".format(hits))

    #dimiourgw main dataframe gia na valw ta apotelesmata
    results_data = pd.DataFrame(columns=['isbn' , 'Book Title' , 'Similarity Score'])
    # error handling me try,except na apofugw (list index out of range) , dimiourgw temp df kai metaferw ta dedomena sto main df
    try:
        for x in range(hits):
            temporary_df = pd.DataFrame([[(results['hits']['hits'][x]['_source']['isbn']),results['hits']['hits'][x]['_source']['book_title'],results['hits']['hits'][x]['_score'] ] ],columns=['isbn', 'Book Title','Similarity Score'])
            results_data = results_data.append(temporary_df , ignore_index= True)
    except:
        for x in range(50):
            temporary_df = pd.DataFrame([[(results['hits']['hits'][x]['_source']['isbn']),results['hits']['hits'][x]['_source']['book_title'],results['hits']['hits'][x]['_score'] ] ],columns=['isbn', 'Book Title','Similarity Score'])
            results_data = results_data.append(temporary_df , ignore_index= True)
    return results_data


#ftiaxnw 2 dataframes.1 gia tin avg vathomologia kai ena gia tin user.
def bookRatings():
    ratings_data = pd.read_csv('BX-Book-Ratings.csv')
    userBookRating = ratings_data[['uid','isbn','rating']]
    avgBookRating = userBookRating.groupby(by='isbn').mean()
    avgBookRating = avgBookRating.drop('uid', axis=1).reset_index() #gia na parw tin avg vathomologia den xreiazomai tin stili 'uid' opote tin petaw
    return avgBookRating , userBookRating
    
def startMain(avgBookRating,userBookRating):

    print("Please type '/stop' if you want to stop searching")
    inputBook = input("Please input the book you want to search for: ")
    inputUser = input("Which user do you want to search for? ")

    query_search= createResultsTable(inputBook)

    final_merged =finalSort(query_search,avgBookRating,userBookRating,inputUser)

    print(final_merged)

    print("Please type '/stop' to stop searching")
    inputBook = ("Please enter a new book to search")

    return

def finalSort (query_search,avgBookRating,userBookRating,inputUser):
    final_merged=query_search.copy(deep=True)

    #kanw merge to final_merged me to avgBookRating
    final_merged=final_merged.merge(avgBookRating , on = 'isbn' , how = 'left')
    final_merged.rename(columns={'rating':'average_rating'} , inplace=True)

    #kanw merge to final_merged me to userBookRating,xrisimopoiw temp gia na min allaksw to arxiko
    temp = userBookRating.copy(deep=True)
    temp.drop(temp[temp['uid'] != int(inputUser)].index,inplace= True)
    final_merged=final_merged.merge(temp , on='isbn' ,how='left')
    final_merged.rename(columns= {'rating' : 'user_rating'},inplace=True)
    

    #xrisimopoiw grammiko sundiasmo twn 3
    final_merged['Score'] = np.nan
    #an exw kai ta 3 dosmena
    final_merged['Score'] = final_merged['Similarity Score'] + final_merged['average_rating'] + final_merged['user_rating']
    #an exw mono avg kai similarity
    final_merged['Score'].fillna(final_merged['Similarity Score'] + final_merged['average_rating'] , inplace=True)
    #an exw mono similarity
    final_merged['Score'].fillna(final_merged['Similarity Score'],inplace=True)

    #telos sortarw analoga me to Score
    final_merged=final_merged.sort_values(by='Score' , ascending = False).reset_index()
    final_merged.drop(['uid' , 'index' ] , axis= 1 )
    final_merged=final_merged.drop_duplicates()

    return final_merged


es=Elasticsearch()
avgBookRating , userBookRating = bookRatings()
startMain(avgBookRating,userBookRating)

