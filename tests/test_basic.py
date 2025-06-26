# tests/test_basic.py
def test_basic_functionality():
    """Basic test to ensure CI pipeline works"""
    assert 1 + 1 == 2

def test_imports():
    """Test that we can import required modules"""
    try:
        import boto3
        import pandas as pd
        assert True
    except ImportError:
        assert False, "Required modules not available"
