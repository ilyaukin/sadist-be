import os
from unittest import mock
import mongomock
import pymongo
import pytest
from app import app
from db import conn

@pytest.fixture(scope="session", autouse=True)
def mock_mongodb():
    test_database_url = 'mongodb://localhost:27017,127.0.0.1:27018/test_sadist_be?replicaSet=rs0'
    if os.getenv('USE_MONGOMOCK'):
        test_client = mongomock.MongoClient(test_database_url)
    else:
        # Fallback to real mongo if requested, but usually we want mongomock for tests
        test_client = pymongo.MongoClient(test_database_url)
    
    with mock.patch.object(conn, 'mongo_client', return_value=test_client):
        yield test_client

@pytest.fixture
def client():
    app.config['TESTING'] = True
    # Ensure secret key is set for sessions
    if not app.config.get('SECRET_KEY'):
        app.config['SECRET_KEY'] = 'test_secret'
    with app.test_client() as client:
        yield client
