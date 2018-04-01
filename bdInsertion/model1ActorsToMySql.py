import pymysql.cursors

HOST = 'localhost'
PORT = 3306
USER = 'root'
PASSWORD = 'pass'
AUX_DB = 'mysql'
DB = 'model1actors'
TABLE = 'persons'
##Connects to another created db and create database
conn = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWORD, db=AUX_DB, use_unicode=True, charset="utf8")
try:
    cur = conn.cursor()
    print('Creating database '+DB+'...')
    cur.execute("CREATE DATABASE IF NOT EXISTS " + DB)
    conn.commit()
    print('Created db '+DB+' successfully')
finally:
    conn.close()
    try:
        file = open("testActorsInsertion.tsv",encoding="utf8")
        ##Connects to the created db and create tables
        conn = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWORD, db=DB, use_unicode=True, charset="utf8")
        cur = conn.cursor()
        for line in file:
            lineSplitted = line.split("\t")
            if lineSplitted[0] == "nconst":
                ##save each parameter in some variables
                nconst = lineSplitted[0]
                primaryName = lineSplitted[1]
                birthYear = lineSplitted[2]
                deathYear = lineSplitted[3]
                primaryProfession = lineSplitted[4]
                knownForTitles = lineSplitted[5]
                print('Creating table '+TABLE+'...')
                cur.execute("CREATE TABLE IF NOT EXISTS " + DB + "." + TABLE + " ( id int(11) NOT NULL AUTO_INCREMENT, "+nconst+" varchar(255) NOT NULL, "+primaryName+" varchar(255) NOT NULL, "+birthYear+" varchar(255) NOT NULL, "+deathYear+" varchar(255) NOT NULL, "+primaryProfession+" varchar(255) NOT NULL, "+knownForTitles+" varchar(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1")
                conn.commit()
                print('Created table '+TABLE+'')
            else:
                statement = "INSERT INTO " + DB + "." + TABLE + " ("+nconst+", "+primaryName+", "+birthYear+", "+deathYear+", "+primaryProfession+", "+knownForTitles+") VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(statement, (lineSplitted[0], lineSplitted[1], lineSplitted[2], lineSplitted[3], lineSplitted[4], lineSplitted[5]))
                conn.commit()
                print('Actor '+ lineSplitted[1]+' inserted')
    finally:
        file.close()
        conn.close()
        print("actors inserted successfully")
