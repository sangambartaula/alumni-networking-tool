def test_root_route_exists(client):
    """
    Basic smoke test: does the server respond at '/'?
    - 200 OK if it serves a page
    - 302 redirect if it bounces to /login, etc.
    """
    resp = client.get("/")
    assert resp.status_code in (200, 302)

import os

def test_env_vars_are_loaded():
    # These should exist because you put them in .env
    assert os.getenv("LINKEDIN_CLIENT_ID")
    assert os.getenv("LINKEDIN_REDIRECT_URI")
    assert os.getenv("SECRET_KEY")
