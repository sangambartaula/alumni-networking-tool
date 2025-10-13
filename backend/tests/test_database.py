"""Tests for database module."""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
from importlib import reload

# Add backend directory to path
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))


class TestDatabaseFunctions:
    """Test database utility functions."""
    
    @patch('mysql.connector.connect')
    def test_get_connection_with_db(self, mock_connect):
        """Test get_connection with database parameter."""
        # Set environment variables and reload module
        with patch.dict(os.environ, {
            'MYSQLHOST': 'testhost',
            'MYSQLUSER': 'testuser',
            'MYSQLPASSWORD': 'testpass',
            'MYSQL_DATABASE': 'testdb',
            'MYSQLPORT': '3306'
        }):
            import database
            reload(database)
            
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            result = database.get_connection(with_db=True)
            
            assert result == mock_conn
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs['host'] == 'testhost'
            assert call_kwargs['user'] == 'testuser'
            assert call_kwargs['password'] == 'testpass'
            assert call_kwargs['port'] == 3306
            assert call_kwargs['database'] == 'testdb'
    
    @patch('mysql.connector.connect')
    def test_get_connection_without_db(self, mock_connect):
        """Test get_connection without database parameter."""
        with patch.dict(os.environ, {
            'MYSQLHOST': 'testhost',
            'MYSQLUSER': 'testuser',
            'MYSQLPASSWORD': 'testpass',
            'MYSQL_DATABASE': 'testdb',
            'MYSQLPORT': '3306'
        }):
            import database
            reload(database)
            
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            result = database.get_connection(with_db=False)
            
            assert result == mock_conn
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert 'database' not in call_kwargs
    
    @patch('mysql.connector.connect')
    def test_ensure_database(self, mock_connect):
        """Test ensure_database creates database if it doesn't exist."""
        with patch.dict(os.environ, {
            'MYSQLHOST': 'testhost',
            'MYSQLUSER': 'testuser',
            'MYSQLPASSWORD': 'testpass',
            'MYSQL_DATABASE': 'testdb',
            'MYSQLPORT': '3306'
        }):
            import database
            reload(database)
            
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            database.ensure_database()
            
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert 'database' not in call_kwargs
            mock_cursor.execute.assert_called_once()
            assert 'CREATE DATABASE IF NOT EXISTS' in mock_cursor.execute.call_args[0][0]
            mock_conn.commit.assert_called_once()
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()
    
    @patch('mysql.connector.connect')
    def test_init_db(self, mock_connect):
        """Test init_db creates alumni table."""
        with patch.dict(os.environ, {
            'MYSQLHOST': 'testhost',
            'MYSQLUSER': 'testuser',
            'MYSQLPASSWORD': 'testpass',
            'MYSQL_DATABASE': 'testdb',
            'MYSQLPORT': '3306'
        }):
            import database
            reload(database)
            
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            database.init_db()
            
            # Should be called twice: once for ensure_database, once for init_db
            assert mock_connect.call_count == 2
            mock_cursor.execute.assert_called()
            # Check that the SQL contains table creation
            calls = mock_cursor.execute.call_args_list
            sql_calls = [call[0][0] for call in calls]
            table_creation = any('CREATE TABLE IF NOT EXISTS alumni' in sql for sql in sql_calls)
            assert table_creation
            mock_conn.commit.assert_called()
            mock_cursor.close.assert_called()
            mock_conn.close.assert_called()
