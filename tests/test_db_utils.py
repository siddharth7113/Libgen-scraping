import logging
# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def test_check_duplicate(db_utils, clean_test_db):
    """
    Test checking for duplicate entries in the database.
    """
    db_utils.conn.execute("INSERT INTO books (libgen_id, title) VALUES ('12345', 'Sample Book');")
    db_utils.conn.commit()

    assert db_utils.check_duplicate("12345") is True
    assert db_utils.check_duplicate("67890") is False


def test_clean_up_empty_entries(db_utils, clean_test_db):
    """
    Test cleaning up empty or incomplete entries from the database.
    """
    # Insert a valid record with all required fields populated
    db_utils.conn.execute("""
        INSERT INTO books (
            libgen_id, author, title, language, publisher, year, pages, size, extension
        ) VALUES (
            '12345', 'John Doe', 'Valid Book', 'English', 'Test Publisher', 2020, 300, '5 MB', 'pdf'
        );
    """)

    # Insert incomplete records
    db_utils.conn.execute("""
        INSERT INTO books (libgen_id, author) VALUES ('67890', 'Jane Doe');
    """)
    db_utils.conn.execute("""
        INSERT INTO books (libgen_id, title) VALUES ('13579', 'Incomplete Title');
    """)
    db_utils.conn.commit()

    # Log database state before cleanup
    cursor = db_utils.conn.cursor()
    cursor.execute("SELECT * FROM books;")
    logging.info(f"Database state before cleanup: {cursor.fetchall()}")

    # Call the cleanup method
    db_utils.clean_up_empty_entries()

    # Log database state after cleanup
    cursor.execute("SELECT * FROM books;")
    logging.info(f"Database state after cleanup: {cursor.fetchall()}")

    # Assert that only the valid record remains
    assert db_utils.get_total_books() == 1, "Incomplete entries were not removed."


def test_get_total_books(db_utils, clean_test_db):
    """
    Test retrieving the total number of books in the database.
    """
    db_utils.conn.execute("INSERT INTO books (libgen_id, title) VALUES ('12345', 'Book 1');")
    db_utils.conn.execute("INSERT INTO books (libgen_id, title) VALUES ('67890', 'Book 2');")
    db_utils.conn.commit()

    assert db_utils.get_total_books() == 2, "Total book count is incorrect."


def test_deduplicate_books(db_utils, clean_test_db):
    """
    Test deduplicating books in the database.
    """
    books = [
        ("12345", "John Doe", "Sample Book", "Publisher A", 2020, 300, "English", "5 MB", "pdf"),
        ("67890", "John Doe", "Sample Book", "Publisher A", 2019, 300, "English", "5 MB", "epub"),
    ]
    db_utils.conn.executemany(
        "INSERT INTO books (libgen_id, author, title, publisher, year, pages, language, size, extension) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
        books,
    )
    db_utils.conn.commit()

    db_utils.deduplicate_books()

    assert db_utils.get_total_books() == 1, "Duplicate entries were not removed."
