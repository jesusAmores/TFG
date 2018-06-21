import psycopg2

HOST = 'localhost'
#PORT = 5432
USER = 'postgres'
PASSWORD = 'password'
DB = 'pgPrueba'
TABLE = 'testFilms'

con = None

try:
    file = open("testFilmsInsertion.tsv",encoding="utf8")
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
            ##Connects to a created db and create table
            conn = psycopg2.connect("host="+HOST+" dbname="+DB+" user="+USER+" password="+PASSWORD+"")
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " ( id SERIAL NOT NULL, "+tconst+" varchar(255) NOT NULL, "+titleType+" varchar(255) NOT NULL, "+primaryTitle+" varchar(255) NOT NULL, "+originalTitle+" varchar(255) NOT NULL, "+isAdult+" numeric(11) NOT NULL, "+startYear+" varchar(255) NOT NULL, "+endYear+" varchar(255) NOT NULL, "+runtimeMinutes+" varchar(255) NOT NULL, "+genres+" varchar(255) NOT NULL, PRIMARY KEY (id))")
            conn.commit()
            print('Created table '+TABLE)
        else:
            #insert data into db
            cur.execute("INSERT INTO " + TABLE + " ("+tconst+","+titleType+","+primaryTitle+","+originalTitle+","+isAdult+","+startYear+","+endYear+","+runtimeMinutes+","+genres+") VALUES ("+lineSplitted[0]+","+lineSplitted[1]+","+lineSplitted[2]+","+lineSplitted[3]+","+lineSplitted[4]+","+lineSplitted[5]+","+lineSplitted[6]+","+lineSplitted[7]+","+lineSplitted[8]+")")
            conn.commit()
            print(lineSplitted[2]+' inserted')
finally:
    file.close()
    if conn:
        conn.close()
    print("films inserted successfully")
