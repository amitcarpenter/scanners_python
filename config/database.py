from pymongo import MongoClient

def connect_database(collection_name):
    """
    Connect to MongoDB database and return the specified collection.
    """
    # Connect to MongoDB
    client = MongoClient("mongodb+srv://vinitaalone143:amitcarpenter11@srnlead.uucjriv.mongodb.net/?retryWrites=true&w=majority")
    db = client.scanners
    return db[collection_name]
