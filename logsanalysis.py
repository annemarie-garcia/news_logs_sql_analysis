#!/usr/bin/env python
import psycopg2

def connect(dbname="news"):
    try:
        db = psycopg2.connect("dbname={}".format(dbname))
        cursor = db.cursor()
        return db, cursor
    except:
        print("There has been an error connecting to the database")

# 1. What are the most popular three articles of all time?
def queryOne():

    db, cursor = connect()
    query = ("select articles.title, count(*) as views"
    " from log join articles on log.path like '%' || articles.slug"
    " group by articles.title"
    " order by views desc limit 3;")

    if __name__ == "__main__":
        cursor.execute(query)
        posts = cursor.fetchall()
        print "1. What are the most popular three articles of all time?" + '\n'
        for lines in posts:
            print "{} {} {} {}".format(lines[0], "-", lines[1], "views")

    db.close()


# 2. Who are the most popular article authors of all time?
def queryTwo():

    db, cursor = connect()
    query = ("select authors.name, count(*) as number from log"
    " join articles on log.path like '%' || articles.slug"
    " join authors on articles.author = authors.id"
    " group by authors.name"
    " order by number desc limit 3;")

    if __name__ == "__main__":
        cursor.execute(query)
        posts = cursor.fetchall()
        print("\n"
              "2. Who are the most popular article authors of "
              "all time?"
              "\n")
        for lines in posts:
            print "{} {} {} {}".format(lines[0], "-", lines[1], "views")

    db.close()


# 3. On which days did more than 1% of requests lead to errors?
def queryThree():

    db, cursor = connect()
    query = ("select errors.date,"
    " cast(errorcount AS FLOAT)/"
    " cast(requestcount AS FLOAT) * 100 as percentage"
    " from(select time::date as date, count(*) as errorcount"
    " from log"
    " where status != '200 OK'"
    " group by time::date) as errors"
    " join (select time::date as date, count(*) as requestcount"
    " from log"
    " group by time::date) as requests"
    " on errors.date = requests.date"
    " where (cast(errorcount AS FLOAT)/"
    " cast(requestcount AS FLOAT) * 100) > 1;")

    if __name__ == "__main__":
        cursor.execute(query)
        posts = cursor.fetchall()
        print("\n"
              "3. On which days did more than 1%"
              " of requests lead to errors?"
              "\n")
        for lines in posts:
            print "{} {} {} {}".format(lines[0], "-", lines[1], "% errors")

queryOne()
queryTwo()
queryThree()
