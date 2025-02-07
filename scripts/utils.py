"""
utils.py

This module contains utility functions related to database checkpoints
and reading an input CSV of queries. It primarily helps coordinate
search progress tracking and query loading.
"""
import aiosqlite
import logging
import csv
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --------------------------------------------------
# 1) ASYNC GET_CHECKPOINT
# --------------------------------------------------
async def get_checkpoint(conn: aiosqlite.Connection, query: str, search_type: str) -> int:
    """
    Asynchronously retrieve the last processed page for a given query and search type.

    Args:
        conn: aiosqlite.Connection
        query: The query being processed.
        search_type: The type of search (e.g., "title").

    Returns:
        int: The last processed page (0 if none).
    """
    try:
        async with conn.execute(
            """
            SELECT last_page
            FROM checkpoints
            WHERE query = ? AND search_type = ?;
            """,
            (query, search_type),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0
    except Exception as e:
        logging.error(f"Error fetching checkpoint: {e}")
        return 0

# --------------------------------------------------
# 2) ASYNC SAVE_CHECKPOINT
# --------------------------------------------------
async def save_checkpoint(conn: aiosqlite.Connection, query: str, search_type: str, last_page: int):
    """
    Asynchronously save or update the checkpoint for a given query and search type.
    """
    try:
        await conn.execute(
            """
            INSERT INTO checkpoints (query, search_type, last_page)
            VALUES (?, ?, ?)
            ON CONFLICT(query, search_type)
            DO UPDATE SET
                last_page = excluded.last_page,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (query, search_type, last_page)
        )
        await conn.commit()
        logging.info(f"Checkpoint saved: query='{query}', search_type='{search_type}', last_page={last_page}")
    except Exception as e:
        logging.error(f"Error saving checkpoint: {e}")

# --------------------------------------------------
# 3) Synchronous CSV Reading (can stay the same)
# --------------------------------------------------
def read_input_csv(file_path):
    """
    Synchronous reading of CSV is fine, no conflict with aiosqlite.
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
