import pytest
from database.db_handler import DatabaseHandler  # Your custom DatabaseHandler class
from database.db_utils import DBUtils  # Your custom DBUtils class
from scripts.libgen_search import LibgenSearch  # Your custom LibgenSearch class
from random_word import RandomWords
import os


@pytest.fixture(scope="module")
def db_handler():
    """
    Fixture to provide a DatabaseHandler instance for the test database.
    """
    # Ensure the 'database' directory exists
    try:
        os.makedirs("database", exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create the 'database' directory: {e}")

    test_db_path = os.path.abspath("database/test_books.db")
    handler = DatabaseHandler(db_name=test_db_path)
    yield handler
    handler.close()

    # Clean up the test database file after the tests
    if os.path.exists(test_db_path):
        os.remove(test_db_path)


@pytest.fixture(scope="function")
def db_utils():
    """
    Fixture to provide a DBUtils instance for the test database.
    """
    try:
        os.makedirs("database", exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to create the 'database' directory: {e}")

    test_db_path = os.path.abspath("database/test_books.db")
    utils = DBUtils(db_path=test_db_path)
    yield utils
    utils.close()


@pytest.fixture(scope="function")
def clean_test_db(db_handler):
    """
    Fixture to clean the test database before and after each test.
    """
    with db_handler.conn:
        db_handler.conn.execute("DELETE FROM books;")
    yield
    with db_handler.conn:
        db_handler.conn.execute("DELETE FROM books;")


@pytest.fixture(scope="module")
def libgen_search_instance():
    """
    Fixture to initialize a LibgenSearch instance for testing.
    """
    return LibgenSearch()


@pytest.fixture
def random_query():
    """
    Generate a random word to use as a query.
    """
    r = RandomWords()
    query = r.get_random_word()
    return query or "test"  # Fallback to "test" if no word is generated
