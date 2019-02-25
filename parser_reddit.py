# -*- coding: utf-8 -*-
"""
Parses a json file where every entry represents a reddit comment.
Store it in a mysql database with three tables: threads, users, posts.
@author: Alberto Lumbreras
"""

import os
import dbmanager as dbmanager
from os.path import join
import json
from glob import glob
from multiprocessing import Pool

def process_file(fname):
    input_file = fname
    output_name = "{}.db".format(fname.split("/")[-1])
    db = dbmanager.DBmanager(join('output', output_name))
    nposts = sum(1 for line in open(input_file, 'r'))
    with open(input_file, 'r') as f:
        
        # Every line is a post        
        for i, line in enumerate(f):
            
            d = json.loads(line)

            try:
                try:
                    idstr = d["name"] # t1_cnas8zv
                except:
                    idstr = d["id"]

                created = d["created_utc"]
                author = d["author"]
                parent = d["parent_id"] # t3_ if first level comment; else t1_... 
                submission = d["link_id"] # t3_... (id of root link)
                body = d['body']
                score = d['score']
                upvotes = d['ups']
                subreddit = d['subreddit']
        
                db.insert_comment(idstr, created, author, parent, submission, body, score, upvotes, subreddit)  
            except Exception as e:
                print(str(e))  
                                                    
            if i%100000 == 0:
                print("Processed", i, "/", nposts , 100.0*i/nposts)
            if i%20000000 == 0:
                print("Processed", i, "/", nposts , 100.0*i/nposts)
                db.dbcommit()
                print(".....................................")
        
        db.dbcommit()

    os.remove(fname)

def parse_reddit():
    '''
    Parse reddit from json files and store them in a SQLite database
    '''
    all_comments = glob('input/comments/*')
    max_thread = 8
    p = Pool(max_thread)

    for i in range(max_thread, len(all_comments), max_thread):
        current_comments = all_comments[:len(all_comments)]
        all_comments=all_comments[len(all_comments):]
        p.map(process_file, current_comments)

    p.map(process_file, all_comments)
        
parse_reddit()
