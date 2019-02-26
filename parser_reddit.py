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
import subprocess

def process_file(fname, file_type="comment"):
    '''
    Parameters:
    ___________

    file_type (string):
    comment or post
    '''

    input_file = fname
    output_name = "{}.db".format(fname.split("/")[-1])
    full_output = join('output', output_name)
    try:
        os.remove(full_output)
    except:
        pass
    
    try:
        os.remove(full_output + "-journal")
    except:
        pass

    db = dbmanager.DBmanager(full_output)
    nposts = sum(1 for line in open(input_file, 'r'))
    with open(input_file, 'r') as f:
        
        # Every line is a post        
        for i, line in enumerate(f):
            try:
                d = json.loads(line)

                if file_type == "comment":
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
            
                    db.insert_comment(postid=idstr, created=created, author=author, parent=parent, submission=submission, body=body, score=score, upvotes=upvotes, subreddit=subreddit)  
                else:
                    try:
                        idstr = d["name"] # t1_cnas8zv
                    except:
                        idstr = d["id"]
                    
                    created = d["created_utc"]
                    is_self = d["is_self"]
                    nsfw = d["over_18"]
                    author = d["author"] 
                    title = d["title"] 
                    url = d["url"]
                    selftext = d["selftext"] 
                    score = d["score"]
                    upvotes = d["ups"] 
                    subreddit = d["subreddit"] 
                    num_comments = d["num_comments"] 
                    flair_text = d["link_flair_text"]

                    db.insert_submissions(postid=idstr, created=created, self=is_self, nsfw=nsfw, author=author, title=title, url=url, selftext=selftext, score=score, upvotes=upvotes, subreddit=subreddit, num_comments=num_comments, flair_text=flair_text)
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
    all_submissions = glob('input/submissions/*')

    for comment in all_comments:
        print("Now doing: {}".format(comment))
        if "xz" in comment:
            command = "lbzip2 -d {}".format(comment)
            subprocess.check_call(command.split(" "))
            comment = comment.replace(".xz", "")
        elif "bz2" in comment:
            command = "lbzip2 -d {}".format(comment)
            subprocess.check_call(command.split(" "))
            comment = comment.replace(".bz2", "")

        process_file(comment)

    for submission in all_submissions:
        print("Now doing: {}".format(submission))
        if "xz" in submission:
            command = "lbzip2 -d {}".format(submission)
            subprocess.check_call(command.split(" "))
            submission = submission.replace(".xz", "")
        elif "bz2" in comment:
            command = "lbzip2 -d {}".format(submission)
            subprocess.check_call(command.split(" "))
            submission = submission.replace(".bz2", "")

        process_file(submission, file_type="submission")   
        
parse_reddit()
