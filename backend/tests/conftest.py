import pytest
import sys
from pathlib import Path

# Add the backend directory to the path so we can import app
sys.path.insert(0, str(Path(__file__).parent.parent))

# Now import the Flask app
from app import app as flask_app

@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client