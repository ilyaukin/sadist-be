from pymongo import ASCENDING
from pymongo.database import Database


def upgrade(db: Database):
    db.tg_chat.create_index([('telegramUsername', ASCENDING)])
    db.tg_chat.create_index([('status', ASCENDING)])
    db.app_user.create_index([('settings.telegram', ASCENDING)], sparse=True)


def downgrade(db: Database):
    db.tg_chat.drop_indexes()
    db.app_user.drop_index('settings.telegram_1')