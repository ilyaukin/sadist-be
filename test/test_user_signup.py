import io
import base64
from PIL import Image

import pytest
from unittest import mock
from app import app
from bson import ObjectId
from db import conn



@mock.patch('app.user.send_email')
def test_signup_and_confirm(mock_send_email, client):
    # Clear collection before test
    conn.mongo_client().get_database().app_user.delete_many({})

    # 1. Test Signup
    signup_payload = {
        'user': {
            'type': 'local',
            'extra': {
                'login': 'testuser',
                'email': 'test@example.com',
                'password': 'password123'
            }
        }
    }
    
    response = client.post('/user/signup', json=signup_payload)
    assert response.status_code == 200
    data = response.get_json()
    assert data['success'] is True
    
    # Check if user is in DB with toConfirm
    user_in_db = conn.mongo_client().get_database().app_user.find_one({'toConfirm.extra.login': 'testuser'})
    assert user_in_db is not None
    assert 'confirmationHash' in user_in_db
    confirmation_hash = user_in_db['confirmationHash']
    
    # Check if email was "sent"
    mock_send_email.assert_called_once()
    args, _ = mock_send_email.call_args
    assert args[0] == 'test@example.com'
    assert confirmation_hash in args[2]
    
    # 2. Test Confirm
    confirm_response = client.get(f'/user/confirm/{confirmation_hash}')
    assert confirm_response.status_code == 200
    assert 'Email has been confirmed' in confirm_response.get_data(as_text=True)
    
    # Check if user record is updated (toConfirm removed, fields moved to top level)
    updated_user = conn.mongo_client().get_database().app_user.find_one({'extra.login': 'testuser'})
    assert updated_user is not None
    assert 'toConfirm' not in updated_user
    assert 'confirmationHash' not in updated_user
    assert updated_user['type'] == 'local'
    assert updated_user['extra']['email'] == 'test@example.com'


def test_avatar_upload_and_get(client):
    # 1. Mock GridFS and Image validation
    from PIL import Image
    img = Image.new('RGB', (10, 10), color='red')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    avatar_data = img_byte_arr.getvalue()

    mock_file = mock.MagicMock()
    mock_file.read.return_value = avatar_data
    mock_file.content_type = 'image/jpeg'

    with mock.patch('app.image.write_grid_file') as mock_replace, \
         mock.patch('app.image.read_grid_file', return_value=mock_file):

        # 2. Upload avatar
        data = {
            'file': (io.BytesIO(avatar_data), 'test.jpg')
        }
        response = client.post('/image/avatar', data=data, content_type='multipart/form-data')
        assert response.status_code == 200
        res_data = response.get_json()
        assert res_data['success'] is True
        filename = res_data['path']

        # Check path is pointed to the correct image type
        assert filename.startswith('/image/avatar')

        # Verify write_grid_file was called with new path format
        mock_replace.assert_called_once()
        args, _ = mock_replace.call_args
        assert args[0] == avatar_data
        assert args[1] == f"img/avatar/{filename.replace('/image/avatar/', '')}"

        # 3. Get avatar
        response = client.get(filename)
        assert response.status_code == 200
        assert response.data == avatar_data
        assert response.mimetype == 'image/jpeg'


@mock.patch('app.user.send_email')
def test_signup_with_avatar_path(mock_send_email, client):
    db = conn.mongo_client().get_database()
    db.app_user.delete_many({})

    # 1. Upload avatar as anon
    img = Image.new('RGB', (10, 10), color='green')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    avatar_data = img_byte_arr.getvalue()

    with mock.patch('app.image.write_grid_file') as mock_replace:
        response = client.post('/image/avatar', data={'file': (io.BytesIO(avatar_data), 'anon.jpg')}, content_type='multipart/form-data')
        assert response.status_code == 200
        upload_data = response.get_json()
        assert upload_data['success'] is True
        avatar_path = upload_data['path']

        # 2. Signup with avatar
        signup_payload = {
            'user': {
                'type': 'local',
                'avatar': avatar_path,
                'extra': {
                    'login': 'pathuser',
                    'email': 'path@example.com',
                    'password': 'password123'
                }
            }
        }

        response = client.post('/user/signup', json=signup_payload)
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True

        # Check if user has avatar set to filename
        user_in_db = db.app_user.find_one({'toConfirm.extra.login': 'pathuser'})
        assert user_in_db is not None
        assert user_in_db.get('toConfirm', {}).get('avatar') == avatar_path


@mock.patch('app.user.send_email')
def test_signup_with_external_avatar_path(mock_send_email, client):
    db = conn.mongo_client().get_database()
    db.app_user.delete_many({})

    signup_payload = {
        'user': {
            'type': 'local',
            'avatar': 'https://example.com/image',
            'extra': {
                'login': 'baduser',
                'email': 'bad@example.com',
                'password': 'password123'
            }
        }
    }

    response = client.post('/user/signup', json=signup_payload)
    assert response.status_code == 200
    data = response.get_json()
    assert data['success'] is True

    # Check if user has avatar set (we don't strip it anymore)
    user_in_db = db.app_user.find_one({'toConfirm.extra.login': 'baduser'})
    assert user_in_db is not None
    assert user_in_db.get('toConfirm', {}).get('avatar') == 'https://example.com/image'


def test_google_no_avatar_download(client):
    db = conn.mongo_client().get_database()
    db.app_user.delete_many({})

    google_payload = {
        'user': {
            'type': 'google',
            'avatar': 'https://google.com/avatar.jpg',
            'extra': {
                'id': 'google_id_123',
                'email': 'google@example.com',
                'picture': 'https://google.com/avatar.jpg',
                'auth': {'id_token': 'fake_token'}
            }
        }
    }

    # We need to mock validate() as well because it tries to verify JWT
    with mock.patch('app.user.GoogleUser.validate', return_value=None):
        response = client.post('/user/login', json=google_payload)
        assert response.status_code == 200
        data = response.get_json()

        # Check if user record has the original avatar URL
        user = db.app_user.find_one({'extra.email': 'google@example.com'})
        assert user is not None
        assert user.get('avatar') == 'https://google.com/avatar.jpg'

        # Check user response for avatar
        assert data['user']['avatar'] == "https://google.com/avatar.jpg"


def test_avatar_not_found(client):
    with mock.patch('app.image.read_grid_file', return_value=None):
        response = client.get(f"/image/avatar/{ObjectId()}")
        assert response.status_code == 404
