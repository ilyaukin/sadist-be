from typing import Optional

import jwt
from flask import request, session
from jwt import PyJWKClient
from mongomoron import insert_one, update_one, query_one, and_

from app import app
from db import conn, app_user
from serializer import serialize


@app.route('/user/whoami')
def whoami():
    if "user" in session:
        return user_response(session["user"])
    return user_response(anon_)


@app.route('/user/login', methods=['POST'])
def login():
    payload = request.get_json()
    app.logger.debug("User is coming: %s" % payload)

    u = payload["user"]
    user = User.of(u)
    user.validate()
    new_user = user.lookup()
    if not new_user:
        new_user = user.create()
    else:
        new_user = user.update()
    session["user"] = new_user
    return user_response(new_user)


@app.route('/user/logout', methods=['POST'])
def logout():
    del session["user"]
    return user_response(anon_)


def user_response(u):
    return {
        'user': serialize(u),
        'success': True,
    }


anon_ = {
    'type': 'anon',
}


class User(object):
    @classmethod
    def of(cls, u=anon_):
        return {
            "anon": AnonUser,
            "google": GoogleUser,
        }[u['type']](u)

    def validate(self) -> None:
        """
        Validate the login request with payload `u` (check OAuth etc.)
        :return None if valid, throw Exception if not valid
        """
        raise NotImplemented("Abstract %s::validate call" % self.__class__.__name__)

    def lookup(self) -> Optional[dict]:
        """
        Look up user in the `app_user` collection
        """
        raise NotImplemented("Abstract %s::lookup call" % self.__class__.__name__)

    def create(self) -> dict:
        """
        Create a new user in the `app_user` collection
        """
        raise NotImplemented("Abstract %s::create call" % self.__class__.__name__)

    def update(self) -> dict:
        """
        Update a user login the `app_user` collection
        """
        raise NotImplemented("Abstract %s::update call" % self.__class__.__name__)


class BaseUser(User):
    """
    Base user that implements operations with database
    """

    def __init__(self, u: dict):
        self.u = u
        self._id = None

    def create(self) -> dict:
        self._id = conn.execute(insert_one(app_user, self.u)).inserted_id
        return {'_id': self._id, **self.u}

    def update(self) -> dict:
        """
        Update all fields by default
        """
        conn.execute(update_one(app_user).filter(app_user._id == self._id).set(self.u))
        return {'_id': self._id, **self.u}


class AnonUser(BaseUser):
    def __init__(self, u):
        super(AnonUser, self).__init__(u)

    def validate(self) -> None:
        raise Exception("Anon user should not call /user/login")


class GoogleUser(BaseUser):
    JWKS_URI = 'https://www.googleapis.com/oauth2/v3/certs'
    client = None

    def __init__(self, u):
        super(GoogleUser, self).__init__(u)

    @classmethod
    def jwks_client(cls):
        if (cls.client):
            return cls.client
        cls.client = PyJWKClient(cls.JWKS_URI)
        return cls.client

    def validate(self) -> None:
        """
        Validate a google user
        """
        client = self.jwks_client()
        token = self.u['extra']['auth']['id_token']
        signing_key = client.get_signing_key_from_jwt(token)
        jwt_decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256'],
            # client_id from Google Cloud console
            audience="252961976632-l3s7f785he9psfk0fm5q33cvk4ssms7s.apps.googleusercontent.com",
        )
        app.logger.debug("Decoded id_token: %s" % jwt_decoded)
        if self.u['extra']['id'] != jwt_decoded['sub']:
            raise Exception('Request forgery: ID does not match')
        self.u.update({'jwt_decoded': jwt_decoded})

    def lookup(self) -> Optional[dict]:
        user = conn.execute(query_one(app_user).filter(and_(
            app_user.type == 'google',
            app_user.extra.id == self.u['extra']['id'])))
        if user:
            self._id = user['_id']
        return user
