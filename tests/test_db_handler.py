import pytest


def test_create_table(db_handler):
    """
    Test that the books table is created successfully.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='books';"
    result = db_handler.conn.execute(query).fetchone()
    assert result is not None, "The books table was not created."


def test_insert_book(db_handler, clean_test_db):
    """
    Test inserting a single book into the database.
    """
    # Sample book data
    book = {
        "ID": "12345",
        "Author": "John Doe",
        "Title": "Sample Book",
        "Publisher": "Test Publisher",
        "Year": 2020,
        "Pages": 300,
        "Language": "English",
        "Size": "5 MB",
        "Extension": "pdf",
        "Mirror_1": ["http://example.com/mirror1"],
        "Mirror_2": ["http://example.com/mirror2"],
        "Direct_Download_Link": "http://example.com/download",
        "query": "test query",
        "search_type": "title",
    }

    db_handler.insert_book(book)

    query = "SELECT * FROM books WHERE libgen_id = ?;"
    result = db_handler.conn.execute(query, (book["ID"],)).fetchone()
    assert result is not None, "Book was not inserted into the database."


def test_prevent_duplicate_insertion(db_handler, clean_test_db):
    """
    Test that duplicate book entries are not inserted into the database.
    """
    book = {
        "ID": "12345",
        "Author": "John Doe",
        "Title": "Sample Book",
        "Publisher": "Test Publisher",
        "Year": 2020,
        "Pages": 300,
        "Language": "English",
        "Size": "5 MB",
        "Extension": "pdf",
        "Mirror_1": ["http://example.com/mirror1"],
        "Mirror_2": ["http://example.com/mirror2"],
        "Direct_Download_Link": "http://example.com/download",
        "query": "test query",
        "search_type": "title",
    }

    db_handler.insert_book(book)
    db_handler.insert_book(book)

    query = "SELECT COUNT(*) FROM books WHERE libgen_id = ?;"
    count = db_handler.conn.execute(query, (book["ID"],)).fetchone()[0]
    assert count == 1, "Duplicate book entry was inserted into the database."


def test_close_connection(db_handler):
    """
    Test closing the database connection.
    """
    db_handler.close()
    with pytest.raises(Exception, match="Cannot operate on a closed database."):
        db_handler.conn.execute("SELECT 1;")
