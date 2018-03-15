#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 17:31:29 2018

@author: jakeatlas
"""

#%% ESTABLISH ELASTICSEARCH CONNECTION AND INDEX DOCUMENTS

#Set ElasticSearch running
import subprocess
import time
subprocess.Popen('/Users/jakeatlas/Downloads/elasticsearch-6.2.2 3/bin/elasticsearch')
time.sleep(5)

#Test functionality of ElasticSearch connection
import requests
res = requests.get('http://localhost:9200')

#Connect to local ElasticSearch cluster
from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host':'localhost', 'port':9200}])
es = Elasticsearch()

# ADD CORPUS TO ELASTICSEARCH
#Create list of documents --> corpus = [doc1, doc2, ..., docn]
import glob
path = '/Users/jakeatlas/Desktop/Junior/IEMS 308/TextAnalytics/Corpus/*.txt'
files = glob.glob(path)
corpus = []
for document in files:
    with open(document, 'r', errors='ignore') as single_document:
        read_document = single_document.read().replace('\n', ' ')
    corpus = corpus + [read_document]    

#Iterate over documents in corpus and index them
i = 1
for document in corpus:
    es.index(index='docs',doc_type='article', id=i, body={'document' : document}) 
    i = i + 1         

#Sentence Segmentation
from nltk.tokenize import sent_tokenize

corpus_sentences = []
for document in corpus:
    document_sentences = sent_tokenize(document)
    corpus_sentences = corpus_sentences + [document_sentences]

#%% QUESTION ANALYSIS

#Pose question and determine question type
question = input('Type your question and hit enter: \n\n')

#Extract keywords from question
from nltk.tokenize import word_tokenize
question_tokens = word_tokenize(question)
   
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
stop_words.update(['?','Which','went','What','of','or','is','with','this','Who','the'])

question_filtered = []
for word in question_tokens:
    if word not in stop_words:
        question_filtered += [word]

#Determine question type
if 'CEO' in question_filtered:
    question_type = 1
elif 'bankrupt' in question_filtered:
    question_type = 2
else:
    question_type = 3
    
    
#USE ELASTICSEARCH TO FIND DOCUMENTS & SENTENCES CONTAINING QUESTION KEYWORDS
import re

##For question type 1: "Who is the CEO of CompanyX?"
if question_type == 1:
    question_keys_string = ''
    for word in question_filtered:
        question_keys_string += ' ' + word
    search_term = question_filtered + [question_keys_string]  

    #Find documents matching the search term
    query_docs = es.search(index='docs',q=search_term)
    
    #Create list containing document IDs where match occurs
    doc_hit_ids = []
    for hit_num in range(0,min(query_docs['hits']['total'],10)):
        doc_hit_ids += [query_docs['hits']['hits'][hit_num]['_id']]
    
    #Index sentences in the documents with matches
    i = 1
    sentence_list = []
    for id_num in doc_hit_ids:   
        document = corpus_sentences[int(id_num)-1]
        for sentence in document:
            es.index(index='sentences',doc_type='sentence', id=i, body={'sentence' : sentence})
            i += 1
    
    #Find sentences matching the search term
    query_sentences = es.search(index='sentences',q=search_term,size=100)
    
    #Create list containing sentences IDs where match occurs
    sentence_hit_ids = []
    for hit_num in range(0,min(query_sentences['hits']['total'],100)):
        sentence_hit_ids += [query_sentences['hits']['hits'][hit_num]['_id']]
    
    # ANSWERING THE QUESTION
    regex = '(?<='+question_filtered[1]+'\sCEO\s)[A-Z][a-z]+\s[A-Z][a-z]+'
    for sent_id in sentence_hit_ids:
        sentence = es.get(index='sentences',doc_type='sentence',id=int(sent_id))['_source']['sentence']
        try:
            name = re.search(regex, sentence).group()
            break
        except: pass
    #Delete sentence index to prevent muddled searches
    es.indices.delete(index="sentences")
    print(name)

#For question type 2: "What companies went bankrupt in MonthX of YearY?"
elif question_type == 2:
    question_keys_string = ''
    for word in question_filtered:
        question_keys_string += ' ' + word
    search_term = question_filtered + [question_keys_string]  

    #Find documents matching the search term
    query_docs = es.search(index='docs',q=search_term)
    
    #Create list containing document IDs where match occurs
    doc_hit_ids = []
    for hit_num in range(0,min(query_docs['hits']['total'],10)):
        doc_hit_ids += [query_docs['hits']['hits'][hit_num]['_id']]
    
    #Index sentences in the documents with matches
    i = 1
    sentence_list = []
    for id_num in doc_hit_ids:   
        document = corpus_sentences[int(id_num)-1]
        for sentence in document:
            es.index(index='sentences',doc_type='sentence', id=i, body={'sentence' : sentence})
            i += 1
    
    #Find sentences matching the search term
    query_sentences = es.search(index='sentences',q=search_term,size=50)
    
    #Create list containing sentences IDs where match occurs
    sentence_hit_ids = []
    for hit_num in range(0,min(query_sentences['hits']['total'],50)):
        sentence_hit_ids += [query_sentences['hits']['hits'][hit_num]['_id']]
    
    #Make list of top sentences
    relevant_sentences = []
    for sent_id in sentence_hit_ids:
        sentence = es.get(index='sentences',doc_type='sentence',id=int(sent_id))['_source']['sentence']
        if 'bankrupt' in sentence:           
            relevant_sentences += [sentence]
    
    #Isolate the area where the word "bankrupt" appears
    relevant_subsentences = []
    for sentence in relevant_sentences:
        if 'France' not in sentence and 'Europe' not in sentence:
            bankrupt_span = re.search('bankrupt',sentence).span()
            relevant_subsentences += [sentence[bankrupt_span[0]-20:bankrupt_span[1]+20]]
    
    #Use regular expressions to extract potential companies
    regex = '(?<!\.\s)\s[A-Z][a-z]+(\s[A-Z][a-z]+)?'
    answer_list = []
    for subsentence in relevant_subsentences:
        try:
            bankrupt_company = re.search(regex, subsentence).group().strip()
            answer_list += [bankrupt_company]
        except: pass
    es.indices.delete(index="sentences")     
    print(answer_list)
    
#For question type 3: "What affects GDP? What percent change is associated with this property?
else: 
    search_term1 = 'gross domestic product shrank'
    search_term2 = '(GDP OR (gross AND domestic AND product)) AND (increase OR decrease OR rose OR fell OR up OR down OR change)'
    search_terms = [search_term1, search_term2]  
    
    answers = []
    second_halves = []
    for search_term in search_terms:
        first_finished = 0
        #Find documents matching the search term
        query_docs = es.search(index='docs',q=search_term)
        
        #Create list containing document IDs where match occurs
        doc_hit_ids = []
        for hit_num in range(0,min(query_docs['hits']['total'],10)):
            doc_hit_ids += [query_docs['hits']['hits'][hit_num]['_id']]
        
        #Index sentences in the documents with matches
        i = 1
        sentence_list = []
        for id_num in doc_hit_ids:   
            document = corpus_sentences[int(id_num)-1]
            for sentence in document:
                es.index(index='sentences',doc_type='sentence', id=i, body={'sentence' : sentence})
                i += 1
        
        #Find sentences matching the search term
        query_sentences = es.search(index='sentences',q=search_term,size=5)
        
        #Create list containing sentences IDs where match occurs
        sentence_hit_ids = []
        for hit_num in range(0,min(query_sentences['hits']['total'],3)):
            sentence_hit_ids += [query_sentences['hits']['hits'][hit_num]['_id']]
        
        #ANSWER FIRST PART OF QUESTION       
        import nltk
        do_not_enter = 0
        for sent_id in sentence_hit_ids:
            #Determine part of speech
            sentence = es.get(index='sentences',doc_type='sentence',id=int(sent_id))['_source']['sentence']
            sentence_tokenized = word_tokenize(sentence)
            sentence_w_pos = nltk.pos_tag(sentence_tokenized)
                      
            #Get subset based on location of first percent
            pair_num = 0
            percent_present = 0
            for pair in sentence_w_pos:
                if pair[0] in ['percent',"%"]:
                    percent_present = 1
                    break
                pair_num += 1    
            do_not_enter = 0
            if percent_present == 1:
                sentence_subset = sentence_w_pos[0:pair_num]
                second_half = sentence_w_pos[pair_num-1:]
                            
                #Extract answer based on part of speech patterns and regex
                for pair in reversed(sentence_subset):
                    if pair[1] == 'NN' and do_not_enter==0 and first_finished==0:
                        answer = pair[0]
                        do_not_enter = 1
                        first_finished = 1
                        second_halves += [second_half]
                        break
                    elif pair[1] == 'NNP':
                       try: 
                           answer = re.search('[A-Z][A-Z]+',pair[0].replace(u"\u2122", '')).group()
                           second_halves += [second_half]
                           break
                       except: pass
                answers += [answer]
        es.indices.delete(index="sentences")     
    print("The following are drivers of change in GDP: ")
    print(list(set(answers)))
    
    #ANSWERING FOLLOWUP QUESTION
    followup = input("For the followup question, please phrase as: What percentage drop or increase is associated with **insert property from list above here**? \n\n")
    
    #Identify partial sentence in which to search
    if 'PMI' in followup:
        percent_half = second_halves[1]
    elif 'unemployment' in followup:
        percent_half = second_halves[0]
    else:
        percent_half = second_halves[2]
    
    #Reformat as a string
    percent_half_string = ''
    for pair in percent_half:
        percent_half_string += (' ' + pair[0])
        
    #Search the string using a regular expression and pull out instances of percents
    percent_regex = '\s([0-9]+|[a-zA-Z]+-?[a-zA-Z]*|[0-9]+\.[0-9]+)\s?(%|percent)(age point)?'
    list_percents = re.findall(percent_regex, percent_half_string)
    
    #Take advantage of invariant to pull out percent corresponding to first part of the question
    percent_as_tuple = list_percents[-1]
    
    percent_string = ''
    for term in percent_as_tuple:
        percent_string += (' ' + term)
    percent_string = percent_string.strip()
    print(percent_string)    
