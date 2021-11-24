from nltk.corpus.reader.bnc import BNCWordView
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from nltk.corpus import stopwords
import string
from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer, TfidfVectorizer
from progress.bar import FillingSquaresBar

books = pd.read_csv('BX-Books.csv')
ratings = pd.read_csv('BX-Book-Ratings.csv')
columns_to_drop = ['book_title','book_author','year_of_publication','category','publisher']
books.drop(columns_to_drop,axis=1,inplace=True)
books_dropped = books[books['summary'].notna()]
print(books.isna().sum())
def text_process(text):
    nopunc = [char for char in text if char not in string.punctuation]
    nopunc = ''.join(nopunc)
    return [word for word in nopunc.split() if word.lower() not in stopwords.words('english')]


books_dropped = books_dropped[books_dropped['summary'].notna()]


bow_transformer = CountVectorizer(analyzer=text_process)
summary_bow = bow_transformer.fit_transform(books_dropped['summary'])

print(summary_bow.shape)

tfidf_transformer = TfidfTransformer().fit(summary_bow)
tfidf = tfidf_transformer.transform(summary_bow)

