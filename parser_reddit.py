# -*- coding: utf-8 -*-
"""
Parses a json file where every entry represents a reddit comment/sumbission.
@author: Daniel Sapkota
"""

import os
import dbmanager as dbmanager
from os.path import join
import json
from glob import glob
from multiprocessing import Pool
import subprocess
import lzma

def process_file(fname, file_type):
    '''
    Parameters:
    ___________

    file_type (string):
    comment or post
    '''        
    if "xz" in fname:
        extractedFile = fname.replace(".xz", "")
        with lzma.open(fname) as f, open(extractedFile, 'wb') as fout:
            file_content = f.read()
            fout.write(file_content)
        
        os.remove(fname)
        fname = extractedFile
    elif "bz2" in fname:
        command = "lbzip2 -d {}".format(fname)
        subprocess.check_call(command.split(" "))
        fname = fname.replace(".bz2", "")

    input_file = fname
    output_name = "{}.db".format(fname.split("/")[-1])
    full_output = join('output', output_name)

    if file_type == "comment":
        if os.path.isfile(full_output + "-journal"):
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
                    all_error = True
                    try:
                        idstr = d["name"] # t1_cnas8zv
                        all_error = False
                    except:
                        idstr = d["id"]

                    created = 0
                    try:
                        created = d["created_utc"]
                        all_error = False
                    except:
                        pass

                    author = ""
                    try:
                        author = d["author"]
                        all_error = False
                    except:
                        pass

                    parent = ""
                    try:
                        parent = d["parent_id"] # t3_ if first level comment; else t1_...
                        all_error = False 
                    except:
                        pass

                    submission = ""
                    try:
                        submission = d["link_id"] # t3_... (id of root link)
                        all_error = False
                    except:
                        pass

                    body = ""
                    try:
                        body = d['body']
                        all_error = False
                    except:
                        pass

                    score = 0
                    try:
                        score = d['score']
                        all_error = False
                    except:
                        pass

                    upvotes = 0
                    try:
                        upvotes = d['ups']
                        all_error = False
                    except:
                        pass
                    
                    subreddit = ""
                    try:
                        subreddit = d['subreddit']
                        all_error = False
                    except:
                        pass

                    if all_error == False:
                        db.insert_comment(postid=idstr, created=created, author=author, parent=parent, submission=submission, body=body, score=score, upvotes=upvotes, subreddit=subreddit)  
                else:

                    all_error = True

                    try:
                        idstr = d["name"] # t1_cnas8zv
                        all_error = False
                    except:
                        idstr = d["id"]
                    
                    created = 0
                    try:
                        created = d["created_utc"]
                        all_error = False
                    except:
                        pass

                    is_self = ""
                    try:
                        is_self = d["is_self"]
                        all_error = False
                    except:
                        pass

                    nsfw = ""
                    try:
                        nsfw = d["over_18"]
                        all_error = False
                    except:
                        pass
                    
                    author = ""
                    try:
                        author = d["author"] 
                        all_error = False
                    except:
                        pass

                    title = ""
                    try:
                        title = d["title"] 
                        all_error = False
                    except:
                        pass

                    url = ""
                    try:
                        url = d["url"]
                        all_error = False
                    except:
                        pass

                    selftext = ""
                    try:
                        selftext = d["selftext"] 
                        all_error = False
                    except:
                        pass
                    
                    score = 0
                    try:
                        score = d["score"]
                        all_error = False
                    except:
                        pass

                    upvotes = 0
                    try:
                        upvotes = d["ups"] 
                        all_error = False
                    except:
                        pass

                    subreddit = ""
                    try:
                        subreddit = d["subreddit"] 
                        all_error = False
                    except:
                        pass

                    num_comments = 0
                    try:
                        num_comments = d["num_comments"] 
                        all_error = False
                    except:
                        pass

                    flair_text = ""

                    try:
                        flair_text = d["link_flair_text"]
                        all_error = False
                    except:
                        pass

                    if all_error == False:
                        db.insert_submissions(postid=idstr, created=created, is_self=is_self, nsfw=nsfw, author=author, title=title, url=url, selftext=selftext, score=score, upvotes=upvotes, subreddit=subreddit, num_comments=num_comments, flair_text=flair_text)
            except Exception as e:
                print(str(e))
                print(d)

            if i%1000000 == 0:
                print("Processed", i, "/", nposts , 100.0*i/nposts)
            if i%200000000 == 0:
                print("Processed", i, "/", nposts , 100.0*i/nposts)
                db.dbcommit()
                print(".....................................")
        
        db.dbcommit()

    os.remove(fname)


def clear_files():
    all_files = glob('output/*')
    
    for file in all_files:
        if os.path.isfile(file + "-journal"):
            try:
                os.remove(file)
            except:
                pass
            
            try:
                os.remove(file + "-journal")
            except:
                pass

def start_running(files, threads, file_type):
    p = Pool(threads)

    for i in range(threads, len(files), threads):
        current_files = files[:len(files)]
        files=files[len(files):]

        file_types = [file_type] * len(current_files)
        p.starmap(process_file, zip(current_files, file_types))

    file_types = [file_type] * len(files)
    p.starmap(process_file, zip(files, file_types))


def parse_reddit():
    '''
    Parse reddit from json files and store them in a SQLite database
    '''

    all_comments = glob('input/comments/*')
    all_submissions = glob('input/submissions/*')
    max_thread = 6

    start_running(all_comments, max_thread, "comment")
    start_running(all_submissions, max_thread, "submission")

clear_files()       
parse_reddit()