from datetime import date, timedelta, datetime
import urllib.request, json 
import time
import psycopg2
import itertools

conn = psycopg2.connect("dbname=reddit_data user=brendan")
cur = conn.cursor()    

d1 = date(2016, 9, 4)  # start date
d2 = date(2017, 12, 31)  # end date
delta = d2 - d1
    
date_range = [] # create list of all dates to search
for i in range(70):
    date_range.append(((d1 + timedelta(weeks=i)).strftime("%Y-%m-%d")))
    
time_range_1 = date_range[:-1] # beginning of each week
time_range_2 = date_range[1:] # end of each week

phones=['iphone', 'galaxy', 'htc', 'lg', 'pixel']

searches = []
searches += phones
searches += ['+'.join(x) for x in itertools.combinations(phones, 2)]

url1 = "https://api.pushshift.io/reddit/search/comment/?q="
subs = ['technology', 'android', 'iphone']
day = 563
for time in time_range_1:
    print("Searching for comments in the week after {}. Day counter is at {}".format(time, day))
    date = '\'' + time + '\''
    
    for _ in range(7):
        inserted = 0
        print("-- Searching day {}".format(day))
        
        for search in searches:
            query = search.replace("+", "_")

            print("-- -- Searching for {}".format(query))


            for sub in subs:
                # print("-- -- -- Searching in subreddit {}".format(sub))

                url = "{0}{1}&subreddit={2}&after={3}d&before={4}d&size=500".format(url1, search, sub, str(day), str(day-1))
                with urllib.request.urlopen(url) as url:
                    data = json.loads(url.read().decode())
                    for comment in data['data']:
                        try:
                            cur.execute("INSERT INTO comments (date, data) VALUES (%s, %s)", (time, comment['body']))
                            # print(comment['body'])
                            conn.commit()
                            inserted += 1

                        except psycopg2.Error as e:
                            try:
                                if(str(e).index("duplicate key")):
                                    pass
        
                            except ValueError:
                                print("\n\n")
                                print(e)
                                print("\n\n")

                            conn.rollback();
                            
        day -= 1
        print("-- -- -- Inserted {} from day {}".format(inserted, day))
                            
cur.close()
conn.close()
