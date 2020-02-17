
from pymongo import MongoClient, errors
import os
import json
import pkg_resources

from bson.json_util import dumps


def getdb():
    try:
        client = MongoClient(host="localhost",
                             port=27017, serverSelectionTimeoutMS=5000)
        client.server_info()
        return client["employees"]
    except errors.ServerSelectionTimeoutError as err:
        print("ERROR:", err)
        client = None
        return client


db = getdb()


def importJSON(filename):
    filepath = pkg_resources.resource_filename(__name__,
                                               os.path.join
                                               ("resources", filename))
    trainingData = []
    with open(filepath, "r") as datafile:
        for line in datafile:
            trainingData.append(json.loads(line))

    return trainingData


def insertData(db, data):
    try:
        insertResult = db.training.insert_many(data)
        return {"success": insertResult.acknowledged}
    except Exception as err:
        return err


def dbQuery(query):
    pipeline = []

    match_stage = {'$match': {'results': {'$all':
                                          [{'$elemMatch': {'evaluation': 'term1', 'score': {'$lt': 37}}},
                                           {'$elemMatch': {'evaluation': 'term2', 'score': {'$lt': 37}}},
                                              {'$elemMatch': {'evaluation': 'term3', 'score': {'$lt': 37}}}
                                           ]}}}

    count_stage = {'$count': 'allFailCount'}

    unwind_stage = {'$unwind': {
        'path': '$results',
        'preserveNullAndEmptyArrays': True
    }
    }

    facet_stage = {'$facet': {
        'avgScorePerTerm': [
            {
                '$group': {
                    '_id': '$results.evaluation',
                    'avgScore': {
                        '$avg': '$results.score'
                    }
                }
            }
        ],
        'failCountPerTerm': [
            {
                '$match': {
                    'results.score': {
                        '$lt': 37
                    }
                }
            }, {
                '$group': {
                    '_id': '$results.evaluation',
                    'count': {
                        '$sum': 1
                    }
                }
            }
        ],
        'avgScore': [
            {
                '$group': {
                    '_id': None,
                    'avgScore': {
                        '$avg': '$results.score'
                    }
                }
            }
        ],
        'anyFailCount': [
            {
                '$match': {
                    'results.score': {
                        '$lt': 37
                    }
                }
            }, {
                '$group': {
                    '_id': '$_id',
                    'name': {
                        '$first': '$name'
                    }
                }
            }, {
                '$count': 'count'
            }
        ]
    }
    }

    if query == "q2":
        pipeline.extend([match_stage])
    elif query == "q5":
        pipeline.extend([match_stage, count_stage])
    elif query in ["q1", "q3", "q4", "q6"]:
        pipeline.extend([unwind_stage, facet_stage])
    else:
        raise AssertionError("Invalid query")

    try:
        cursor = dumps(db.training.aggregate(pipeline))
        return cursor
    except Exception as err:
        return err
