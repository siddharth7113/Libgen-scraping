import pytest
from scripts.utils import get_checkpoint, save_checkpoint, read_input_csv

def test_get_checkpoint(db_handler):
    """
    Test retrieving a checkpoint.
    """
    conn = db_handler.conn
    conn.execute("INSERT INTO checkpoints (query, search_type, last_page) VALUES ('Python', 'title', 5);")
    conn.commit()

    last_page = get_checkpoint(conn, "Python", "title")
    assert last_page == 5, "Failed to retrieve the correct checkpoint."

    last_page = get_checkpoint(conn, "NonExistentQuery", "author")
    assert last_page == 0, "Non-existent checkpoint should return 0."


def test_save_checkpoint(db_handler):
    """
    Test saving and updating checkpoints.
    """
    conn = db_handler.conn
    save_checkpoint(conn, "Python", "title", 5)
    last_page = get_checkpoint(conn, "Python", "title")
    assert last_page == 5, "Failed to save the checkpoint."

    # Update checkpoint
    save_checkpoint(conn, "Python", "title", 10)
    last_page = get_checkpoint(conn, "Python", "title")
    assert last_page == 10, "Failed to update the checkpoint."


def test_read_input_csv(tmp_path):
    """
    Test reading queries from a CSV file.
    """
    csv_file = tmp_path / "input.csv"
    csv_file.write_text("query,search_type\nPython,title\nData Science,author\n")

    queries = read_input_csv(csv_file)
    assert len(queries) == 2, "Failed to read all rows from the CSV."
    assert queries[0]["query"] == "Python" and queries[0]["search_type"] == "title", "First row mismatch."
    assert queries[1]["query"] == "Data Science" and queries[1]["search_type"] == "author", "Second row mismatch."

    # Test invalid CSV
    invalid_csv = tmp_path / "invalid.csv"
    queries = read_input_csv(invalid_csv)
    assert len(queries) == 0, "Invalid CSV should return an empty list."
