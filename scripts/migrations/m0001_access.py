from pymongo.database import Database


def upgrade(db: Database):
    db.ds_list.update_many({"extra.access": {"$exists": False}},
                           {"$set": {"extra.access": {"type": "public"}}})


def downgrade(db: Database):
    db.ds_list.update_many({}, {"$unset": {"extra.access": 0}})
