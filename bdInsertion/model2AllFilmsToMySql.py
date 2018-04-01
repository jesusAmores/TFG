import pymysql.cursors

HOST = 'localhost'
PORT = 3306
USER = 'root'
PASSWORD = 'pass'
AUX_DB = 'mysql'
DB = 'model2All'
TABLE = 'films'
##Connects to another created db and create database
conn = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWORD, db=AUX_DB, use_unicode=True)
try:
    cur = conn.cursor()
    print('Creating database '+DB+'...')
    cur.execute("CREATE DATABASE IF NOT EXISTS " + DB)
    conn.commit()
    print('Created db '+DB+' successfully')
finally:
    conn.close()
    try:
        file = open("films.tsv",encoding="utf8")
        ##Connects to the created db and create tables
        conn = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWORD, db=DB, use_unicode=True)
        cur = conn.cursor()
        for line in file:
            lineSplitted = line.split("\t")
            if lineSplitted[0] == "tconst":
                ##save each parameter in some variables
                tconst = lineSplitted[0]
                titleType = lineSplitted[1]
                primaryTitle = lineSplitted[2]
                originalTitle = lineSplitted[3]
                isAdult = lineSplitted[4]
                startYear = lineSplitted[5]
                endYear = lineSplitted[6]
                runtimeMinutes = lineSplitted[7]
                genres = lineSplitted[8]
                print('Creating table '+TABLE+'...')
                cur.execute("CREATE TABLE IF NOT EXISTS " + DB + "." + TABLE + " ( id int(11) NOT NULL AUTO_INCREMENT, "+tconst+" varchar(255) NOT NULL, "+titleType+" varchar(255) NOT NULL, "+primaryTitle+" varchar(255) NOT NULL, "+originalTitle+" varchar(255) NOT NULL, "+isAdult+" int(11) NOT NULL, "+startYear+" varchar(255) NOT NULL, "+endYear+" varchar(255) NOT NULL, "+runtimeMinutes+" varchar(255) NOT NULL, "+genres+" varchar(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1")
                conn.commit()
                print('Created table '+TABLE+'')
            else:
                statement = "INSERT INTO " + DB + "." + TABLE + " ("+tconst+", "+titleType+", "+primaryTitle+", "+originalTitle+", "+isAdult+", "+startYear+", "+endYear+", "+runtimeMinutes+", "+genres+") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cur.execute(statement, (lineSplitted[0], lineSplitted[1], lineSplitted[2], lineSplitted[3], lineSplitted[4], lineSplitted[5], lineSplitted[6], lineSplitted[7], lineSplitted[8]))
                conn.commit()
                print(lineSplitted[2]+' inserted')
    finally:
        file.close()
        conn.close()
        print("films inserted successfully")
