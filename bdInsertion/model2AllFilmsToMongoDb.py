from pymongo import MongoClient

try:
    file = open("films.tsv",encoding="utf8")

    client = MongoClient()
    db = client.model2AllMovies
    collection = db.films

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
            genres = lineSplitted[8].strip()

        else:
            collection.insert_one({
                tconst : lineSplitted[0],
                titleType : lineSplitted[1],
                primaryTitle : lineSplitted[2],
                originalTitle : lineSplitted[3],
                isAdult : lineSplitted[4],
                startYear : lineSplitted[5],
                endYear : lineSplitted[6],
                runtimeMinutes : lineSplitted[7],
                genres : lineSplitted[8].strip(),
            })
            print("Inserted film " + lineSplitted[3]);
finally:
    file.close()
