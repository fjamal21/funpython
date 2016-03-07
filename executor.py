#!/usr/bin/python
import fileinput
import csv
import os
import sys
import time
import MySQLdb

from collections import defaultdict, Counter

domain_counts = defaultdict(Counter)

# ======================== Defined Functions ======================
def get_file_path(filename):
        currentdirpath = os.getcwd()
        # get current working directory path
        filepath = os.path.join(currentdirpath, filename)
        return filepath
# ===========================================================
def read_CSV(filepath):

        with open('emails.csv') as f:
                reader = csv.reader(f)
                for row in reader:
                        domain_counts[row[0].split('@')[1].strip()][row[1]] += 1

        db = MySQLdb.connect(host="localhost", # your host, usually localhost
                                                 user="root", # your username
                                                 passwd="abcdef1234", # your password
                                                 db="test") # name of the data base
        cur = db.cursor()

        q = """INSERT INTO domains(domain_name, count, date_entry) VALUES(%s, %s, STR_TO_DATE(%s, '%%d-%%m-%%Y'))"""


        for domain, data in domain_counts.iteritems():
                for email_date, email_count in data.iteritems():
                         cur.execute(q, (domain, email_count, email_date))

        db.commit()

        cur1 = db.cursor()
        sql = "SELECT  domain_name, pct_growth FROM (  SELECT t1.domain_name, (Sum(CASE WHEN t1.date_entry >= (CURRENT_DATE - INTERVAL 30 DAY) THEN t1.count ELSE 0 END) - Sum(CASE WHEN t1.date_entry < (CURRENT_DATE - INTERVAL 30 DAY) THEN t1.count ELSE 0 END) ) / (SELECT SUM(t2.count) FROM domains t2 WHERE t2.date_entry < (CURRENT_DATE - INTERVAL 30 DAY)) As pct_growth  FROM domains t1 GROUP BY t1.domain_name ) As derivedTable  ORDER BY pct_growth DESC LIMIT 50;"

        cur1.execute(sql)

        for row in cur1.fetchall():
           print(row)

# ======================= main program =======================================
path = get_file_path('emails.csv')
read_CSV(path) # read the input file
