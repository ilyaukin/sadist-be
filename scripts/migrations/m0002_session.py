from pymongo.database import Database


def upgrade(db: Database):
    db.app_user_session.delete_many({})


def downgrade(db: Database):
    pass
