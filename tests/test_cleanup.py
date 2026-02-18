import sys
import os
from pathlib import Path
import unittest

# Setup paths
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'backend'))
sys.path.insert(0, str(project_root / 'scraper'))

from database import normalize_url
from scraper import normalize_scraped_data

class TestURLNormalization(unittest.TestCase):
    def test_normalize_url_backend(self):
        """Test backend.database.normalize_url"""
        self.assertEqual(normalize_url('https://linkedin.com/in/foo/'), 'https://linkedin.com/in/foo')
        self.assertEqual(normalize_url('https://linkedin.com/in/foo'), 'https://linkedin.com/in/foo')
        self.assertEqual(normalize_url('  https://linkedin.com/in/foo/  '), 'https://linkedin.com/in/foo')
        self.assertIsNone(normalize_url(None))

    def test_normalize_scraped_data_scraper(self):
        """Test scraper.normalize_scraped_data"""
        data = {
            'profile_url': 'https://linkedin.com/in/bar/',
            'other_url': 'https://example.com/baz/',
            'name': ' John Doe '
        }
        normalized = normalize_scraped_data(data)
        self.assertEqual(normalized['profile_url'], 'https://linkedin.com/in/bar')
        self.assertEqual(normalized['other_url'], 'https://example.com/baz') # It checks 'url' in key
        self.assertEqual(normalized['name'], 'John Doe')

if __name__ == '__main__':
    unittest.main()
