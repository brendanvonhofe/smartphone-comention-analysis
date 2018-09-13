import psycopg2
from datetime import date, timedelta, datetime
import time
import itertools
import re
import pandas as pd
import numpy as np

def recordMentions(df, phones, regexes, comment, time):
    combs = itertools.combinations(phones, 2)
    r_combs = [(re.compile(x[0], re.IGNORECASE), re.compile(x[1], re.IGNORECASE)) for x in itertools.combinations(phones, 2)]
    for phone, reg in zip(phones, regexes):
        if(reg.search(comment)):
            df[phone].loc[time] += 1
            
            # Delete false positives
            if(phone == 'lg' 
               and not re.compile("\Wlg\W", re.IGNORECASE).search(comment) 
               and not re.compile("lg\W", re.IGNORECASE).search(comment) 
               and not re.compile("\Wlg", re.IGNORECASE).search(comment)):
                df[phone].loc[time] -= 1
            if(phone == 'htc' 
               and not re.compile("\Whtc\W", re.IGNORECASE).search(comment)
               and not re.compile("htc\W", re.IGNORECASE).search(comment)
               and not re.compile("\Whtc", re.IGNORECASE).search(comment)):
                df[phone].loc[time] -= 1
            
    for comb, r_comb in zip(combs, r_combs):
        if(r_comb[0].search(comment) and r_comb[1].search(comment)):
            flag = 0
            df[' '.join(comb)].loc[time] += 1
                        
            # Delete false positives
            if(comb[0] == 'lg' or comb[1] == 'lg'):
                if(not re.compile("\Wlg\W", re.IGNORECASE).search(comment) 
                   and not re.compile("lg\W", re.IGNORECASE).search(comment) 
                   and not re.compile("\Wlg", re.IGNORECASE).search(comment)):
                    print("~~~ LG LIVE ONE ({}) ~~~".format(str(comb)))
                    print(comment)
                    df[' '.join(comb)].loc[time] -= 1
                    flag = 1
                
            if(comb[0] == 'htc' or comb[1] == 'htc'):
                if(not re.compile("\Whtc\W", re.IGNORECASE).search(comment) 
                   and not re.compile("htc\W", re.IGNORECASE).search(comment) 
                   and not re.compile("\Whtc", re.IGNORECASE).search(comment)):
                    print(" &&& HTC LIVE ONE ({}) &&& ".format(str(comb)))
                    print(comment)
                    df[' '.join(comb)].loc[time] -= 1
                    if(flag == 1):
                        df[' '.join(comb)].loc[time] += 1



d1 = date(2016, 9, 4)  # start date
d2 = date(2017, 12, 31)  # end date
delta = d2 - d1
date_range = [] # create list of all dates to search
for i in range(70):
    date_range.append(((d1 + timedelta(weeks=i)).strftime("%Y-%m-%d")))
time_range = date_range[:-1] # beginning of each week

phones=['iphone', 'galaxy', 'htc', 'lg', 'pixel']
regs=[re.compile(phone, re.IGNORECASE) for phone in phones]
phone_combs=[' '.join(x) for x in itertools.combinations(phones, 2)]
# Dataframe for mentions and co-mentions
data_df = pd.DataFrame(data=np.zeros((69, 15)), columns=phones+phone_combs, index=time_range)

conn = psycopg2.connect("dbname=reddit_data user=brendan")
cur = conn.cursor()

for time in time_range:
    print("Fetching from week after {}".format(time))
    timepsql = '\'' + time + '\''
    cur.execute("SELECT data FROM comments WHERE date = {}".format(timepsql))
    data = cur.fetchall()
    for tupe in data:
        comment = tupe[0]
        recordMentions(data_df, phones, regs, comment, time)

    
cur.close()
conn.close()

data_df.to_csv("reddit_comments.csv")
data_df.to_excel("reddit_comments.xlsx")