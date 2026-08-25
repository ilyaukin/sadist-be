import hashlib
import secrets
from typing import Optional

import jwt
from app import app, logger
from app.email_helper import send_email
from app.url_helper import get_base_url
from collections_helper import deep_merge
from db import conn, app_user
from flask import request, session
from jwt import PyJWKClient
from mongomoron import insert_one, update_one, query_one, and_, or_
from serializer import serialize
from user_helper import anon_


@app.route('/user/whoami')
def whoami():
    return user_response(session.get("user", anon_))


@app.route('/user/login', methods=['POST'])
def login():
    payload = request.get_json()
    logger.debug("User is coming: %s" % payload)

    u = payload["user"]
    user = User.of(u)
    user.validate()
    if isinstance(user, LocalUser):
        session["user"] = user.lookup()
    else:
        user.lookup()
        session["user"] = user.create_or_update()
    return user_response(session["user"])


@app.route('/user/logout', methods=['POST'])
def logout():
    del session["user"]
    return user_response(anon_)


@app.route('/user/signup', methods=['POST'])
def signup():
    # first, add provided data under toConfirm (either to a new
    # user or to the existing one, if user with the same email had been
    # logging in through OAuth providers)
    payload = request.get_json()
    logger.debug("User sign up: %s", payload)

    u = payload['user']

    user = LocalUser.signup(u)

    return user_response(user.create_or_update())


@app.route('/user/confirm/<confirmation_hash>', methods=['GET'])
def confirm(confirmation_hash):
    # on confirmation, move date from onConfirm to the main record
    try:
        LocalUser.confirm(confirmation_hash)
        return 'Email has been confirmed'
    except Exception as e:
        return 'Email has not been confirmed: ' + str(e)


def user_response(u: dict):
    # we don't want to reveal extra, toConfirm or hashes
    clean_u = dict((key, value) for key, value in u.items()
                   if key != 'extra' and key != 'toConfirm'
                   and not key.endswith('Hash'))

    return {
        'user': serialize(clean_u),
        'success': True,
    }


class User(object):
    @classmethod
    def of(cls, u=anon_):
        return {
            "anon": AnonUser,
            "local": LocalUser,
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
        existing = conn.execute(query_one(app_user).filter(app_user._id == self._id))
        if existing:
            self.u = deep_merge(self.u, existing)

        conn.execute(update_one(app_user).filter(app_user._id == self._id).set(self.u))
        return {'_id': self._id, **self.u}

    def create_or_update(self) -> dict:
        """
        Create or update user depending on_id.
        NOTE!!! When attaching user to an existing DB user, _id must populate!!!
        """
        if self._id:
            return self.update()
        return self.create()


class AnonUser(BaseUser):
    def __init__(self, u):
        super(AnonUser, self).__init__(u)

    def validate(self) -> None:
        raise Exception("Anon user should not call /user/login")


class LocalUser(BaseUser):
    SALT = 'U(uh((9jp'

    def __init__(self, u):
        super().__init__(u)
        # placeholder for a DB record
        self.user = None
        # immediately replace pass with a hash
        if 'password' not in self.u['extra'] or not self.u['extra']['password']:
            raise Exception('Password is required!')
        self.u['extra']['password'] = hashlib.md5(
            (self.u['extra']['password'] + self.SALT).encode()).hexdigest()

    @staticmethod
    def signup(u: dict) -> 'LocalUser':
        user = User.of(u)
        if not isinstance(user, LocalUser):
            raise Exception('Attempting to sign up with non-local user type')

        # we don't allow to hijack logins
        db_user = user.by_login()
        if db_user:
            raise Exception(f"User login {u['extra']['login']} already exists")

        db_user = user.by_login_to_confirm()
        if db_user:
            # if user with the input login to confirm already exists, and input email matches email or email to confirm,
            # we should go on and update toConfirm data, instead of throwing an error.
            input_email = u['extra']['email']
            emails = [
                db_user.get('extra', {}).get('email'),
                db_user.get('toConfirm', {}).get('extra', {}).get('email')
            ]
            if input_email not in emails:
                raise Exception(f"User login {u['extra']['login']} already exists")

        # from the other hand, we allow to hijack emails, because if
        # a user haven't received email confirmation, he still should
        # be able to proceed or restart registration
        db_user = db_user or user.by_email() or user.by_email_to_confirm()
        if db_user:
            user._id = db_user['_id']
        user.u = {'toConfirm': user.u}

        # generate confirmation hash
        user.u['confirmationHash'] = secrets.token_hex(16)

        # send an email
        email = user.u['toConfirm']['extra']['email']
        subject = "Confirm your registration"
        link = f"{get_base_url()}/user/confirm/{user.u['confirmationHash']}"
        body = f"Please confirm your registration by clicking the link: {link}"

        send_email(email, subject, body)

        return user

    @staticmethod
    def confirm(confirmation_hash: str):
        db_user = conn.execute(query_one(app_user).filter(
            app_user.confirmationHash == confirmation_hash
        ))
        if not db_user:
            raise Exception('Invalid confirmation hash')

        to_confirm = db_user.get('toConfirm')
        if not to_confirm:
            raise Exception('Nothing to confirm')

        # deep merge of the user data
        merged_user = deep_merge(to_confirm, db_user)
        # ... and remove the fields to unset
        del merged_user['toConfirm']
        del merged_user['confirmationHash']

        conn.execute(update_one(app_user).filter(app_user._id == db_user['_id'])
                     .set(merged_user)
                     .unset('toConfirm', 'confirmationHash'))

    def validate(self) -> None:
        if not self.lookup():
            raise Exception('User with the given login and password not found')

    def lookup(self) -> Optional[dict]:
        # only look up once
        if self.user:
            return self.user

        self.user = conn.execute(query_one(app_user).filter(and_(
            # we'll allow logging in by both login and email
            or_(
                app_user.extra.login == self.u['extra']['login'],
                app_user.extra.email == self.u['extra']['login'],
            ),
            app_user.extra.password == self.u['extra']['password']
        )))
        if self.user:
            self._id = self.user['_id']
        return self.user

    def by_login(self):
        return conn.execute(query_one(app_user).filter(
            app_user.extra.login == self.u['extra']['login']
        ))

    def by_email(self):
        return conn.execute(query_one(app_user).filter(
            app_user.extra.email == self.u['extra']['email']
        ))

    def by_login_to_confirm(self):
        return conn.execute(query_one(app_user).filter(
            app_user.toConfirm.extra.login == self.u['extra']['login']
        ))

    def by_email_to_confirm(self):
        return conn.execute(query_one(app_user).filter(
            app_user.toConfirm.extra.email == self.u['extra']['email']
        ))


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
        logger.debug("Decoded id_token: %s" % jwt_decoded)
        if self.u['extra']['id'] != jwt_decoded['sub']:
            raise Exception('Request forgery: ID does not match')
        # merge info from jwt_decoded (email etc.) into extra
        self.u['extra'].update(jwt_decoded)

    def lookup(self) -> Optional[dict]:
        # look up by email because a same user can log in by OAuth and email/password
        user = conn.execute(query_one(app_user).filter(
            app_user.extra.email == self.u['extra']['email']))
        if user:
            self._id = user['_id']
        return user
