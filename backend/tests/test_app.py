"""Tests for the Flask application."""
import pytest
import sys
import os
from pathlib import Path

# Add backend directory to path
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from app import app


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_home_route(client):
    """Test that the home route returns the index.html file."""
    response = client.get('/')
    assert response.status_code == 200


def test_about_route(client):
    """Test that the about route returns expected content."""
    response = client.get('/about')
    assert response.status_code == 200
    assert b'About page coming soon' in response.data


def test_404_handler(client):
    """Test that 404 errors are handled properly."""
    response = client.get('/nonexistent-page')
    assert response.status_code == 404
    assert b'Page not found' in response.data


def test_app_has_secret_key():
    """Test that the app has a secret key configured."""
    assert app.config['SECRET_KEY'] is not None
    assert len(app.config['SECRET_KEY']) > 0
