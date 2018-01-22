from pymongo import MongoClient

try:
    file = open("actors.tsv",encoding="utf8")

    client = MongoClient()
    db = client.persons
    collection = db.actors

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

        else:
            collection.insert_one({
                nconst : lineSplitted[0],
                primaryName : lineSplitted[1],
                birthYear : lineSplitted[2],
                deathYear : lineSplitted[3],
                primaryProfession : lineSplitted[4],
                knownForTitles : lineSplitted[5],
            })
            print("Inserted actor " + lineSplitted[1])
finally:
    file.close()
