import os
import pytest

def test_root_route_exists(client):
    """
    Basic smoke test: does the server respond at '/'?
    - 200 OK if it serves a page
    - 302 redirect if it bounces to /login, etc.
    """
    resp = client.get("/")
    assert resp.status_code in (200, 302)


def test_env_vars_are_loaded():
    """Test that required environment variables are loaded from .env"""
    assert os.getenv("LINKEDIN_CLIENT_ID")
    assert os.getenv("LINKEDIN_REDIRECT_URI")
    assert os.getenv("SECRET_KEY")


def test_about_route(client):
    """Test that the /about route is accessible"""
    resp = client.get("/about")
    assert resp.status_code == 200
    assert b"About" in resp.data or b"about" in resp.data.lower()


def test_logout_clears_session(client):
    """Test that /logout route clears the session and redirects"""
    # Set up a session with LinkedIn token
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "fake_token"
        sess["linkedin_profile"] = {"sub": "12345", "name": "Test User"}
    
    # Verify session is set
    with client.session_transaction() as sess:
        assert "linkedin_token" in sess
    
    # Call logout
    resp = client.get("/logout")
    
    # Should redirect to home
    assert resp.status_code == 302
    assert resp.location.endswith("/") or "/" in resp.location
    
    # Session should be cleared
    with client.session_transaction() as sess:
        assert "linkedin_token" not in sess
        assert "linkedin_profile" not in sess
        assert len(sess) == 0


def test_logout_with_no_session(client):
    """Test that /logout works even if there's no active session"""
    resp = client.get("/logout")
    assert resp.status_code == 302
    assert resp.location.endswith("/") or "/" in resp.location