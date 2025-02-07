"""
db_handler.py

This module defines an asynchronous DatabaseHandler class that manages
connecting to a SQLite database (stored locally), creating necessary
tables, inserting books, and updating link/status information.
"""

import os
import logging
import aiosqlite
from datetime import datetime

# Configure logging.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DatabaseHandler:
    """
    Provides asynchronous interactions with the SQLite database for:
    - Creating tables (books, checkpoints, batches, files).
    - Inserting records into the books table.
    - Checking duplicates / deduplicating.
    - Updating link statuses and direct download links.

    Attributes:
        db_path (str): The file path to the SQLite database.
        conn (aiosqlite.Connection or None): The active SQLite connection.
        deduplication_executed (bool): Tracks if we've already run deduplication.
    """

    def __init__(self, db_name="books.db"):
        """
        Initialize the DatabaseHandler by setting up the database path.
        """
        os.makedirs("database", exist_ok=True)
        self.db_path = os.path.join("database", db_name)
        self.conn = None
        self.deduplication_executed = False  # optional: track if deduplicate_books was run

    async def init(self):
        """
        Asynchronously initialize the database connection and create the required tables.
        Also ensure columns exist for link_status and link_error_message.
        """
        self.conn = await aiosqlite.connect(self.db_path, timeout=30)
        await self.create_tables()
        await self.ensure_columns()  # make sure columns exist

    async def create_tables(self):
        """
        Create the required tables (books, checkpoints, batches, files) if they don't exist.
        """
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                libgen_id TEXT UNIQUE,
                author TEXT,
                title TEXT,
                publisher TEXT,
                year INTEGER,
                pages INTEGER,
                language TEXT,
                size TEXT,
                extension TEXT,
                mirror_1 TEXT,
                mirror_2 TEXT,
                direct_link TEXT,
                link_status TEXT DEFAULT 'Pending',
                link_error_message TEXT DEFAULT NULL,
                query TEXT,
                search_type TEXT
            );
            
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                search_type TEXT NOT NULL,
                last_page INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(query, search_type)
            );
            
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT UNIQUE,
                total_files INTEGER,
                processed_files INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                batch_id INTEGER,
                status TEXT DEFAULT 'Pending',
                error_log TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(batch_id) REFERENCES batches(id)
            );
            """
        )
        await self.conn.commit()
        logging.info("Books, checkpoints, batches, and files tables ensured in database.")

    async def ensure_columns(self):
        """
        Make sure columns link_status and link_error_message exist in the 'books' table.
        If they already exist, ignore the error.
        """
        try:
            await self.conn.execute(
                "ALTER TABLE books ADD COLUMN link_status TEXT DEFAULT 'Pending';"
            )
            await self.conn.execute(
                "ALTER TABLE books ADD COLUMN link_error_message TEXT DEFAULT NULL;"
            )
            await self.conn.commit()
            logging.info("Ensured new columns: link_status, link_error_message.")
        except Exception as e:
            # Usually an OperationalError if columns already exist, so just log
            logging.info(f"Columns already exist or could not be added: {e}")

    async def insert_book(self, book):
        """
        Asynchronously insert a single book into the database.

        Args:
            book (dict): A dictionary containing the book metadata.
        """
        try:
            required_keys = [
                "ID", "Author", "Title", "Publisher", 
                "Year", "Pages", "Language", "Size", 
                "Extension", "Mirror_1", "Mirror_2"
            ]
            for key in required_keys:
                if key not in book:
                    logging.error(f"Missing key '{key}' in book data: {book}")
                    return

            direct_link = book.get("Direct_Download_Link", None)
            query = book.get("query", "")
            search_type = book.get("search_type", "")
            link_status = 'Pending'

            await self.conn.execute(
                """
                INSERT OR IGNORE INTO books (
                    libgen_id, author, title, publisher, year, pages, language, size,
                    extension, mirror_1, mirror_2, direct_link, link_status, query, search_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    book["ID"],
                    book["Author"],
                    str(book["Title"]),
                    book["Publisher"],
                    book["Year"],
                    book["Pages"],
                    book["Language"],
                    book["Size"],
                    book["Extension"],
                    str(book["Mirror_1"]),
                    str(book["Mirror_2"]),
                    direct_link,
                    link_status,
                    query,
                    search_type
                )
            )
            await self.conn.commit()
            logging.info(f"Inserted book ID={book['ID']} into the database.")
        except Exception as e:
            logging.error(f"Error inserting book ID={book.get('ID', 'Unknown')}: {e}")

    async def update_link_status(self, book_id, status, error_message=None):
        """
        Asynchronously update the link status and error message for a book.
        """
        try:
            await self.conn.execute(
                """
                UPDATE books
                SET link_status = ?, link_error_message = ?
                WHERE id = ?;
                """,
                (status, error_message, book_id)
            )
            await self.conn.commit()
            logging.info(f"Updated link status for book ID={book_id} to '{status}'.")
        except Exception as e:
            logging.error(f"Error updating link status for book ID={book_id}: {e}")

    async def update_direct_link(self, book_id, direct_link):
        """
        Asynchronously update the direct_link column for a given book.
        """
        try:
            await self.conn.execute(
                """
                UPDATE books
                SET direct_link = ?
                WHERE id = ?;
                """,
                (direct_link, book_id)
            )
            await self.conn.commit()
            logging.info(f"Updated direct_link for book ID={book_id}.")
        except Exception as e:
            logging.error(f"Error updating direct_link for book ID={book_id}: {e}")

    async def check_duplicate(self, libgen_id):
        """
        Asynchronously check if a book with the given LibGen ID already exists.
        Returns True if it exists, False otherwise.
        """
        try:
            async with self.conn.execute(
                "SELECT 1 FROM books WHERE libgen_id = ? LIMIT 1;",
                (libgen_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row is not None
        except Exception as e:
            logging.error(f"Error checking for duplicate (ID={libgen_id}): {e}")
            return False

    async def deduplicate_books(self):
        """
        Asynchronously deduplicate books by:
          1) Grouping by (title, author).
          2) Keeping only the latest year for that group.
          3) If multiple share that same year, prefer the first PDF entry.
        """
        if self.deduplication_executed:
            logging.info("Deduplication already executed, skipping.")
            return

        logging.info("Starting deduplication process (async).")
        try:
            await self.conn.execute("BEGIN IMMEDIATE")

            query = """
            SELECT
                title,
                author,
                MAX(year) AS latest_year,
                MIN(CASE WHEN extension = 'pdf' THEN id ELSE NULL END) AS keep_id,
                GROUP_CONCAT(id) AS all_ids
            FROM books
            GROUP BY title, author
            HAVING COUNT(*) > 1;
            """
            async with self.conn.execute(query) as cursor:
                duplicate_groups = await cursor.fetchall()

            for (title, author, latest_year, keep_id, all_ids) in duplicate_groups:
                if not all_ids:
                    continue
                all_ids_list = all_ids.split(',')

                # If no PDF found for that group, fallback to the first record
                if keep_id is None:
                    keep_id = all_ids_list[0]

                # Remove the keep_id from the list of duplicates
                if str(keep_id) in all_ids_list:
                    all_ids_list.remove(str(keep_id))

                # Delete all others
                if all_ids_list:
                    placeholders = ", ".join(all_ids_list)
                    delete_query = f"DELETE FROM books WHERE id IN ({placeholders});"
                    await self.conn.execute(delete_query)
                    logging.info(f"Deleted duplicate books: {placeholders}")

            await self.conn.commit()
            logging.info("Deduplication process completed (async).")
            self.deduplication_executed = True

        except Exception as e:
            logging.error(f"Error during deduplication: {e}")
            await self.conn.rollback()

    async def close(self):
        """
        Asynchronously close the database connection.
        """
        if self.conn:
            await self.conn.close()
        logging.info("Database connection closed.")
