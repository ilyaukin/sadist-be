import pymongo
from pymongo.database import Database


def upgrade(db: Database):
    # Unique index on login - sparse because Google users might not have one
    db.app_user.create_index([("extra.login", pymongo.ASCENDING)], unique=True, sparse=True)
    
    # Unique index on email - sparse because pending users have it under toConfirm
    db.app_user.create_index([("extra.email", pymongo.ASCENDING)], unique=True, sparse=True)
    
    # Unique index on confirmation hash - sparse because only pending local users have it
    db.app_user.create_index([("confirmationHash", pymongo.ASCENDING)], unique=True, sparse=True)
    
    # Unique indexes on pending login/email to prevent duplicate signups
    db.app_user.create_index([("toConfirm.extra.login", pymongo.ASCENDING)], unique=True, sparse=True)
    db.app_user.create_index([("toConfirm.extra.email", pymongo.ASCENDING)], unique=True, sparse=True)


def downgrade(db: Database):
    db.app_user.drop_index("extra.login_1")
    db.app_user.drop_index("extra.email_1")
    db.app_user.drop_index("confirmationHash_1")
    db.app_user.drop_index("toConfirm.extra.login_1")
    db.app_user.drop_index("toConfirm.extra.email_1")
