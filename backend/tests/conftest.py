import pytest

# If your app defines `app = Flask(__name__)` at module level:
from app import app as flask_app

@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client
