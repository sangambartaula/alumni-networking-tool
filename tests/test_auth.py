import pytest
import sys
import os
from pathlib import Path

# Add backend directory to module search path for imports
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir / "backend"))
os.chdir(root_dir)

from auth import (
    validate_password_policy,
    hash_password,
    verify_password,
    check_rate_limit,
    clear_rate_limit
)

def test_password_policy_valid():
    """Test passwords that meet the policy requirements."""
    valid_passwords = [
        "StrongPassw0rd!",
        "12345Abcde!",
        "MyP@ssw0rd2026",
        "P@$$w0rd123456"
    ]
    for pw in valid_passwords:
        is_valid, errors = validate_password_policy(pw)
        assert is_valid is True, f"Failed for {pw}: {errors}"
        assert len(errors) == 0

def test_password_policy_invalid_length():
    """Test passwords that are too short."""
    pw = "Sh0rt!"
    is_valid, errors = validate_password_policy(pw)
    assert is_valid is False
    assert "Password must be at least 10 characters long" in errors

def test_password_policy_invalid_missing_upper():
    """Test passwords without uppercase letters."""
    pw = "nouppercase1!"
    is_valid, errors = validate_password_policy(pw)
    assert is_valid is False
    assert "Password must contain at least one uppercase letter." in errors

def test_password_policy_invalid_missing_lower():
    """Test passwords without lowercase letters."""
    pw = "NOLOWERCASE1!"
    is_valid, errors = validate_password_policy(pw)
    assert is_valid is False
    assert "Password must contain at least one lowercase letter." in errors

def test_password_policy_invalid_missing_number():
    """Test passwords without numbers."""
    pw = "MissingNumber!"
    is_valid, errors = validate_password_policy(pw)
    assert is_valid is False
    assert "Password must contain at least one number." in errors

def test_password_policy_invalid_missing_special():
    """Test passwords without special characters."""
    pw = "NoSpecialChar123"
    is_valid, errors = validate_password_policy(pw)
    assert is_valid is False
    assert "Password must contain at least one special character." in errors

def test_password_hashing():
    """Test bcrypt hashing and verification."""
    pw = "MySecureP@ssw0rd!"
    hashed = hash_password(pw)
    
    # Needs to return True for correct password
    assert verify_password(pw, hashed) is True
    
    # Needs to return False for incorrect password
    assert verify_password("WrongPassword!", hashed) is False
    
    # Needs to return False for empty password
    assert verify_password("", hashed) is False

def test_rate_limiting():
    """Test the in-memory rate limiting logic."""
    test_email = "test.rate.limit@unt.edu"
    
    # Clear any previous runs
    clear_rate_limit(test_email)
    
    # Make 5 allowed attempts
    for _ in range(5):
        assert check_rate_limit(test_email) is True
        
    # 6th attempt should fail
    assert check_rate_limit(test_email) is False
    
    # Clearing logic allows again
    clear_rate_limit(test_email)
    assert check_rate_limit(test_email) is True
