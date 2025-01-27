import sqlite3
import logging
import csv
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_checkpoint(conn, query, search_type):
    """
    Retrieve the last processed page for a given query and search type.

    Args:
        conn: SQLite database connection.
        query (str): The query being processed.
        search_type (str): The type of search.

    Returns:
        int: The last processed page (0 if no checkpoint exists).
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_page FROM checkpoints WHERE query = ? AND search_type = ?;
        """, (query, search_type))
        result = cursor.fetchone()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logging.error(f"Error fetching checkpoint: {e}")
        return 0


def save_checkpoint(conn, query, search_type, last_page):
    """
    Save or update the checkpoint for a given query and search type.

    Args:
        conn: SQLite database connection.
        query (str): The query being processed.
        search_type (str): The type of search.
        last_page (int): The last processed page.
    """
    try:
        with conn:
            conn.execute("""
                INSERT INTO checkpoints (query, search_type, last_page)
                VALUES (?, ?, ?)
                ON CONFLICT(query, search_type)
                DO UPDATE SET last_page = excluded.last_page, updated_at = CURRENT_TIMESTAMP;
            """, (query, search_type, last_page))
        logging.info(f"Checkpoint saved: query='{query}', search_type='{search_type}', last_page={last_page}")
    except sqlite3.Error as e:
        logging.error(f"Error saving checkpoint: {e}")

def read_input_csv(file_path):
    """
    Reads queries and search types from a CSV file.
    Each row must have a 'query' and 'search_type' column.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        list: A list of dictionaries with 'query' and 'search_type'.
    """
    queries = []
    try:
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    query = row.get('query', '').strip()
                    search_type = row.get('search_type', 'title').strip()
                    
                    if not query:
                        raise ValueError("Empty query field.")
                    
                    queries.append({"query": query, "search_type": search_type})
                except ValueError as e:
                    logging.warning(f"Skipping corrupted row: {row}. Error: {e}")
                except KeyError as e:
                    logging.warning(f"Missing expected column: {e}. Skipping row: {row}")
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
    
    return queries

