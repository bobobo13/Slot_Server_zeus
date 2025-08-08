import pymongo

def initSlotGameConfig():
    SettingList = []
    Setting = {
        "GameName": "LionDanceLegi",
        "MaxOdds": 6000,
        "SlotServiceUrl": "http://host.docker.internal:8100/SlotApi",
        "Cost": 1,
    }
    SettingList.append(Setting)

    Setting = {
        "GameName": "CashKing",
        "MaxOdds": 6000,
        "SlotServiceUrl": "http://host.docker.internal:8100/SlotApi",
        "Cost": 1,
    }
    SettingList.append(Setting)
    return SettingList

def initDb(db, SettingList):
    db["SlotLobbyConfig"].create_index([('GameName', pymongo.ASCENDING)], unique=True)
    for doc in SettingList:
        print(db["SlotLobbyConfig"].update_one({'GameName': doc["GameName"]}, {'$set': doc}, upsert=True))


if __name__ == "__main__":
    SettingList = initSlotGameConfig()

    mongouri = 'mongodb://localhost:27017/'
    # mongouri = 'mongodb://macross:3ctvm3SYDpurWdSXCuD7CtWAksEpf5d5@localhost:17050/?authSource=admin'
    # mongouri = 'mongodb+srv://macross-uat:tBKJC4pYTCTFEm11@macross-slotdb-01-uat.byjat.mongodb.net/?tls=true&tlsInsecure=true'

    db = pymongo.MongoClient(mongouri)['SlotGame']
    initDb(db, SettingList)
    print("Init SlotGameConfig Done")



