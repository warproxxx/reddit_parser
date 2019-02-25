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

def parse_reddit():
    '''
    Parse reddit from json files and store them in a SQLite database
    '''
    all_comments = glob('input/comments/*')

    for comment_file in all_comments:
        db = dbmanager.DBmanager(join('output', 'reddit.db'))
        input_file = comment_file
        

        nposts = sum(1 for line in open(input_file, 'r'))
        with open(input_file, 'r') as f:
            
            # Every line is a post        

            for i, line in enumerate(f):
                
                d = json.loads(line)
                idstr = d["name"] # t1_cnas8zv
                created = d["created_utc"]
                author = d["author"]
                parent = d["parent_id"] # t3_ if first level comment; else t1_... 
                submission = d["link_id"] # t3_... (id of root link)
                body = d['body']
                score = d['score']
                upvotes = d['ups']
                subreddit = d['subreddit']
        
                db.insert_comment(idstr, created, author, parent, submission, body, score, upvotes, subreddit)    
                                                        
                if i%100000 == 0:
                    print("Processed", i, "/", nposts , 100.0*i/nposts)
                if i%20000000 == 0:
                    print("Processed", i, "/", nposts , 100.0*i/nposts)
                    db.dbcommit()
                    print(".....................................")
            
            db.dbcommit()
        
parse_reddit()
