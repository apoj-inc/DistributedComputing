# app/db.py
def get_users_from_db():
    # This function would normally connect to a real database
    pass

# app/service.py
from app.db import get_users_from_db

def get_active_users():
    users = get_users_from_db()
    return [user for user in users if user['active'] is True]

# tests/test_service.py
from app.service import get_active_users

def test_get_active_users(mocker):
    # Mock the database function
    mock_db_call = mocker.patch('app.service.get_users_from_db')
    
    # Define the fake data it should return
    fake_users = [
        {'id': 1, 'name': 'Alice', 'active': True},
        {'id': 2, 'name': 'Bob', 'active': False}
    ]
    mock_db_call.return_value = fake_users

    # Call the function under test
    active_users = get_active_users()

    # Assertions
    assert active_users == [{'id': 1, 'name': 'Alice', 'active': True}]
    mock_db_call.assert_called_once() # Verify the call was made
