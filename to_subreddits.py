import os
import sqlite3 as sqlite
from glob import glob
import dbmanager as dbmanager
import lzma
from multiprocessing import Pool
from os.path import join

def process(input_file, file_type):
    '''
    Parameters:
    ___________
    input_file (string):
    Location of input
    
    file_type (string):
    comments or submissions. Anything else is invalid
    '''
    if (file_type == "comments" or file_type == "submissions"):    
        con=sqlite.connect(input_file)
        cursor = con.cursor()

        cursor.execute("SELECT DISTINCT subreddit FROM {}".format(file_type))
        subreddits = cursor.fetchall()

        for subreddit in subreddits:
            cursor.execute("SELECT * FROM {} WHERE subreddit='{}'".format(file_type, subreddit[0]))
            rows = cursor.fetchall()
            filename = "subreddits/{}.db".format(subreddit[0])
            db = dbmanager.DBmanager(filename)
            
            i = 0
            for row in rows:

                if file_type == "submissions":
                    db.insert_submissions(postid=row[0], created=row[1], is_self=row[2], nsfw=row[3], author=row[4], title=row[5], url=row[6], selftext=row[7], score=row[8], upvotes=row[9], subreddit=row[10], num_comments=row[11], flair_text=row[12])
                else:
                    db.insert_comment(postid=row[0], created=row[1], author=row[2], parent=row[3], submission=row[4], body=row[5], score=row[6], upvotes=row[7], subreddit=row[8])
                
                i = i+1
                
                if i%200000000 == 0:
                    db.dbcommit()
                    
            
            db.dbcommit()
        os.remove(input_file)
    else:
        return 0

def start_running(files, threads, file_type):
    p = Pool(threads)

    for i in range(threads, len(files), threads):
        current_files = files[:len(files)]
        files=files[len(files):]

        file_types = [file_type] * len(current_files)
        p.starmap(process, zip(current_files, file_types))

    file_types = [file_type] * len(files)
    p.starmap(process, zip(files, file_types))

all_files = glob('output/*')
submissions = []
comments = []

for file in all_files:
    if "RC" in file:
        comments.append(file)
    elif "RS" in file:
        submissions.append(file)

threads = 3

start_running(submissions, threads, "submissions")
start_running(comments, threads, "comments")
