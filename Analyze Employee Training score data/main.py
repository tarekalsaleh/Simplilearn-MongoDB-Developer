from query import getdb, importJSON, insertData, dbQuery
from bson.json_util import loads


if __name__ == "__main__":

    db = getdb()
    db.training.drop()
    employee_data = importJSON("employees.json")
    insertResult = insertData(db, employee_data)
    totalCount = db.training.count_documents({})

    query1 = loads(dbQuery("q1"))[0]

    for score in query1["failCountPerTerm"]:
        if score["_id"] == "term1":
            print("- The number of employees who failed in term 1:\n {}".format(
                score["count"]))
            print("- Percentage failed:\n {:.2f}%".format(
                score["count"] * 100 / totalCount))

    for score in query1["avgScorePerTerm"]:
        if score["_id"] == "term1":
            print("- The average score of trainees for term 1:\n {:.2f}".format(
                score["avgScore"]))

    query2 = loads(dbQuery("q2"))
    print("- Employees who failed in all 3 terms are:")
    for item in query2:
        print("* ", item["name"])

    query3 = loads(dbQuery("q5"))[0]

    print("- The number of employees who failed in all 3 terms: \n {}".format(
        query3["allFailCount"]))

    print("- The number of employees who failed in any of the three:\n {}".format(
        query1["anyFailCount"][0]["count"]))

    print("- The average score of trainees:\n {:.2f}".format(
        query1["avgScore"][0]["avgScore"]))
