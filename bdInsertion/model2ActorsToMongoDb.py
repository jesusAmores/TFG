from pymongo import MongoClient

try:
    file = open("testActorsInsertion.tsv",encoding="utf8")

    client = MongoClient()
    db = client.model2Actors
    collection = db.persons

    for line in file:
        lineSplitted = line.split("\t")
        if lineSplitted[0] == "nconst":
            ##save each parameter in some variables
            nconst = lineSplitted[0]
            primaryName = lineSplitted[1]
            birthYear = lineSplitted[2]
            deathYear = lineSplitted[3]
            primaryProfession = lineSplitted[4]
            knownForTitles = lineSplitted[5].strip()

        else:
            filmsInString = lineSplitted[5].strip()
            filmsList = filmsInString.split(',')
            collection.insert_one({
                nconst : lineSplitted[0],
                primaryName : lineSplitted[1],
                birthYear : lineSplitted[2],
                deathYear : lineSplitted[3],
                primaryProfession : lineSplitted[4],
                knownForTitles : filmsList,
            })
            print("Inserted actor " + lineSplitted[1]);
finally:
    file.close()
