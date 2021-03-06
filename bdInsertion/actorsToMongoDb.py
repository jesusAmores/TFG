from pymongo import MongoClient

try:
    file = open("actors.tsv",encoding="utf8")

    client = MongoClient()
    db = client.actors
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
            collection.insert_one({
                nconst : lineSplitted[0],
                primaryName : lineSplitted[1],
                birthYear : lineSplitted[2],
                deathYear : lineSplitted[3],
                primaryProfession : lineSplitted[4],
                knownForTitles : lineSplitted[5].strip(),
            })
            print("Inserted actor " + lineSplitted[1] + "with references ;" + lineSplitted[5].strip() +";");
finally:
    file.close()
