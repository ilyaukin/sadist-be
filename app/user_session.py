from typing import Union

from bson import ObjectId
from flask.sessions import SessionMixin, SessionInterface
from mongomoron import query_one, update_one, insert_one

import db
from db import conn, app_user_session


class UserSession(dict, SessionMixin):
    def __init__(self, session_record: dict = None):
        super().__init__()
        self._modified = False
        if session_record:
            self['id'] = session_record['_id']
            user = conn.execute(query_one(db.app_user).filter(db.app_user._id == session_record['user_id']))
            if user:
                self['user'] = user

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._modified = True

    def __delitem__(self, key):
        super().__delitem__(key)
        self._modified = True

    @staticmethod
    def load(_id: Union[str, ObjectId]) -> 'UserSession':
        if _id:
            session_record = conn.execute(
                query_one(app_user_session).filter(app_user_session._id == _id)
            )
            if session_record:
                return UserSession(session_record)

        return UserSession()

    def save(self):
        _id = self.get('id')
        user = self.get('user')
        user_id = user.get('_id') if user else None
        if _id:
            conn.execute(update_one(app_user_session).filter(app_user_session._id == _id).set({'user_id': user_id}))
        else:
            self['id'] = conn.execute(insert_one(app_user_session, {'user_id': user_id})).inserted_id
        self._modified = False

    @property
    def modified(self):
        return self._modified


class UserSessionInterface(SessionInterface):
    def open_session(self, app, request):
        if request.path.startswith(app.static_url_path):
            return UserSession()

        session_id = request.cookies.get(app.session_cookie_name)

        try:
            _id = ObjectId(session_id)
        except:
            # session_id in wrong format
            _id = None

        return UserSession.load(_id)

    def save_session(self, app, session: UserSession, response):
        if session.modified:
            session.save()
            response.set_cookie(app.session_cookie_name, str(session['id']))
