'''
Set up the tables of the database
'''
import os
import sqlite3 as sqlite


class DBmanager():
    def __init__(self, dbname):
        thisfilepath = os.path.dirname(os.path.realpath(__file__))
        dbrelpath = ''
        self.filepath = os.path.join(thisfilepath, dbrelpath, dbname)

        # Create tables if do not exist
        if not os.path.exists(self.filepath):
            self.con=sqlite.connect(self.filepath)
            self.con.row_factory = sqlite.Row
            self.cursor = self.con.cursor()
            self.create_tables()
            self.dbcommit()
            
        # Else, connect to the db
        else:
            self.con=sqlite.connect(self.filepath)
            self.con.row_factory = sqlite.Row
            self.cursor = self.con.cursor()
            
            
    def __del__(self):
        self.con.close()
        
    def dbcommit(self):
        self.con.commit()
        
    def dbdelete(self):
        if os.path.exists(self.filepath):
            os.remove(self.filepath)

    def create_tables(self):
        '''Init empty tables to store comments and submissions''' 
        
        self.cursor.execute("""create table comments(
                            postid TEXT, 
                            created INT,
                            author TEXT,
                            parent TEXT,
                            submission TEXT,
                            body TEXT,
                            score INT,
                            upvotes INT,
                            subreddit TEXT
                            )""")
        
        self.cursor.execute("""create table submissions(
                            postid TEXT, 
                            created INT,
                            self INT,
                            nsfw INT,
                            author TEXT,
                            title TEXT,
                            url TEXT,
                            selftext TEXT,
                            score INT,
                            upvotes INT,
                            subreddit TEXT,
                            num_comments INT,
                            flair_text TEXT
                            )""")
        
        
    def insert_comment(self, postid, created, author, parent, submission, body, score, upvotes, subreddit):
        self.cursor.execute("""INSERT INTO 
                            comments(
                            postid, 
                            created, 
                            author,
                            parent,
                            submission, 
                            body, 
                            score,
                            upvotes,
                            subreddit
                            ) 
                            VALUES 
                            (?,?,?,?,?,?,?,?,?)"""
                            , (postid, created, author, parent, 
                               submission, body, score, upvotes, subreddit))
    
    def insert_submissions(self, postid, created, is_self, nsfw, author, title, url, selftext, score, upvotes, subreddit, num_comments, flair_text):
        try:
            self.cursor.execute("""INSERT INTO 
                                submissions(
                                postid, 
                                created, 
                                self, 
                                nsfw, 
                                author, 
                                title, 
                                url, 
                                selftext, 
                                score, 
                                upvotes, 
                                subreddit, 
                                num_comments, 
                                flair_text
                                ) 
                                VALUES 
                                (?,?,?,?,?,?,?,?,?,?,?,?,?)"""
                                , (postid, created, is_self, nsfw, author, title, url, selftext, score, upvotes, subreddit, num_comments, flair_text))
        except:
            self.cursor.execute("""create table submissions(
                            postid TEXT, 
                            created INT,
                            self INT,
                            nsfw INT,
                            author TEXT,
                            title TEXT,
                            url TEXT,
                            selftext TEXT,
                            score INT,
                            upvotes INT,
                            subreddit TEXT,
                            num_comments INT,
                            flair_text TEXT
                            )""")