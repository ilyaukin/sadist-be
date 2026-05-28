import pytest
from unittest import mock
from app import app
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
