import sqlite3
import logging
import os
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DatabaseHandler:
    def __init__(self, db_name="books.db"):
        """
        Initialize the database connection and create the required tables.
        """
        # Ensure the 'database' folder exists
        os.makedirs("database", exist_ok=True)

        # Set the full path for the database file
        self.db_path = os.path.join("database", db_name)
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def create_tables(self):
        """
        Create the required tables (books, checkpoints, batches, and files) if they don't already exist.
        """
        with self.conn:
            # Books table
            self.conn.execute("""
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
            """)
            logging.info("Books table ensured in database.")

            # Checkpoints table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT NOT NULL,
                    search_type TEXT NOT NULL,
                    last_page INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(query, search_type)
                );
            """)
            logging.info("Checkpoints table ensured in database.")

            # Batches table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_name TEXT UNIQUE,
                    total_files INTEGER,
                    processed_files INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'Pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logging.info("Batches table ensured in database.")

            # Files table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT,
                    batch_id INTEGER,
                    status TEXT DEFAULT 'Pending',
                    error_log TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(batch_id) REFERENCES batches(id)
                );
            """)
            logging.info("Files table ensured in database.")

    def insert_book(self, book):
        """
        Insert a single book into the database.
        
        Args:
            book (dict): A dictionary containing the book metadata.
        """
        try:
            # Ensure required keys exist
            required_keys = ["ID", "Author", "Title", "Publisher", "Year", "Pages", "Language", "Size", "Extension", "Mirror_1", "Mirror_2"]
            for key in required_keys:
                if key not in book:
                    logging.error(f"Missing key '{key}' in book data: {book}")
                    return

            # Prepare default values for optional fields
            direct_link = book.get("Direct_Download_Link", None)  # Initialize as NULL if missing
            query = book.get("query", "")
            search_type = book.get("search_type", "")
            link_status = 'Pending'

            # Insert the book into the database
            with self.conn:
                self.conn.execute("""
                    INSERT OR IGNORE INTO books (
                        libgen_id, author, title, publisher, year, pages, language, size,
                        extension, mirror_1, mirror_2, direct_link, link_status, query, search_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    book["ID"], book["Author"], str(book["Title"]),
                    book["Publisher"], book["Year"], book["Pages"], book["Language"],
                    book["Size"], book["Extension"], str(book["Mirror_1"]),
                    str(book["Mirror_2"]), direct_link, link_status, query, search_type
                ))
            logging.info(f"Inserted book ID={book['ID']} into the database.")

        except sqlite3.Error as e:
            logging.error(f"Error inserting book ID={book['ID']}: {e}")

    def update_link_status(self, book_id, status, error_message=None):
        """
        Update the link status and error message for a book.
        
        Args:
            book_id (int): The ID of the book to update.
            status (str): The new status of the link (e.g., 'Valid', 'Error').
            error_message (str): The error message, if applicable.
        """
        try:
            with self.conn:
                self.conn.execute("""
                    UPDATE books
                    SET link_status = ?, link_error_message = ?
                    WHERE id = ?;
                """, (status, error_message, book_id))
                logging.info(f"Updated link status for book ID={book_id} to '{status}'.")
        except sqlite3.Error as e:
            logging.error(f"Error updating link status for book ID={book_id}: {e}")

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()
        logging.info("Database connection closed.")
import sqlite3
import logging
import os
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DatabaseHandler:
    def __init__(self, db_name="books.db"):
        """
        Initialize the database connection and create the required tables.
        """
        # Ensure the 'database' folder exists
        os.makedirs("database", exist_ok=True)

        # Set the full path for the database file
        self.db_path = os.path.join("database", db_name)
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def create_tables(self):
        """
        Create the required tables (books, checkpoints, batches, and files) if they don't already exist.
        """
        with self.conn:
            # Books table
            self.conn.execute("""
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
            """)
            logging.info("Books table ensured in database.")

            # Checkpoints table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query TEXT NOT NULL,
                    search_type TEXT NOT NULL,
                    last_page INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(query, search_type)
                );
            """)
            logging.info("Checkpoints table ensured in database.")

            # Batches table (Retained for backward compatibility, not used anymore)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_name TEXT UNIQUE,
                    total_files INTEGER,
                    processed_files INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'Pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logging.info("Batches table ensured in database (for backward compatibility).")

            # Files table (Retained for backward compatibility, not used anymore)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT,
                    batch_id INTEGER,
                    status TEXT DEFAULT 'Pending',
                    error_log TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(batch_id) REFERENCES batches(id)
                );
            """)
            logging.info("Files table ensured in database (for backward compatibility).")

    def insert_book(self, book):
        """
        Insert a single book into the database.
        
        Args:
            book (dict): A dictionary containing the book metadata.
        """
        try:
            # Ensure required keys exist
            required_keys = ["ID", "Author", "Title", "Publisher", "Year", "Pages", "Language", "Size", "Extension", "Mirror_1", "Mirror_2"]
            for key in required_keys:
                if key not in book:
                    logging.error(f"Missing key '{key}' in book data: {book}")
                    return

            # Prepare default values for optional fields
            direct_link = book.get("Direct_Download_Link", None)  # Initialize as NULL if missing
            query = book.get("query", "")
            search_type = book.get("search_type", "")
            link_status = 'Pending'

            # Insert the book into the database
            with self.conn:
                self.conn.execute("""
                    INSERT OR IGNORE INTO books (
                        libgen_id, author, title, publisher, year, pages, language, size,
                        extension, mirror_1, mirror_2, direct_link, link_status, query, search_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    book["ID"], book["Author"], str(book["Title"]),
                    book["Publisher"], book["Year"], book["Pages"], book["Language"],
                    book["Size"], book["Extension"], str(book["Mirror_1"]),
                    str(book["Mirror_2"]), direct_link, link_status, query, search_type
                ))
            logging.info(f"Inserted book ID={book['ID']} into the database.")

        except sqlite3.Error as e:
            logging.error(f"Error inserting book ID={book['ID']}: {e}")

    def update_link_status(self, book_id, status, error_message=None):
        """
        Update the link status and error message for a book.
        
        Args:
            book_id (int): The ID of the book to update.
            status (str): The new status of the link (e.g., 'Valid', 'Error').
            error_message (str): The error message, if applicable.
        """
        try:
            with self.conn:
                self.conn.execute("""
                    UPDATE books
                    SET link_status = ?, link_error_message = ?
                    WHERE id = ?;
                """, (status, error_message, book_id))
                logging.info(f"Updated link status for book ID={book_id} to '{status}'.")
        except sqlite3.Error as e:
            logging.error(f"Error updating link status for book ID={book_id}: {e}")

    # --- Deprecated Methods: Batch-related Logic (Retained for Reference) ---
    # def create_batch(self, batch_name, files):
    #     """
    #     Create a new batch and associate files with it.
    #     """
    #     try:
    #         with self.conn:
    #             cursor = self.conn.cursor()

    #             # Insert batch metadata
    #             cursor.execute("""
    #                 INSERT INTO batches (batch_name, total_files)
    #                 VALUES (?, ?)
    #             """, (batch_name, len(files)))
    #             batch_id = cursor.lastrowid

    #             # Insert files into the batch
    #             for file in files:
    #                 cursor.execute("""
    #                     INSERT INTO files (file_name, batch_id)
    #                     VALUES (?, ?)
    #                 """, (file, batch_id))

    #             logging.info(f"Batch '{batch_name}' with {len(files)} files created.")
    #     except sqlite3.Error as e:
    #         logging.error(f"Error creating batch '{batch_name}': {e}")

    # def update_batch_progress(self, batch_id):
    #     """
    #     Update the progress of a batch based on the files processed.
    #     """
    #     try:
    #         with self.conn:
    #             cursor = self.conn.cursor()

    #             # Count processed files
    #             cursor.execute("""
    #                 SELECT COUNT(*) FROM files
    #                 WHERE batch_id = ? AND status = 'Downloaded'
    #             """, (batch_id,))
    #             processed_files = cursor.fetchone()[0]

    #             # Update batch progress
    #             cursor.execute("""
    #                 UPDATE batches
    #                 SET processed_files = ?, status = ?
    #                 WHERE id = ?
    #             """, (processed_files, 'In Progress' if processed_files > 0 else 'Pending', batch_id))

    #             logging.info(f"Batch ID {batch_id} progress updated.")
    #     except sqlite3.Error as e:
    #         logging.error(f"Error updating batch ID {batch_id}: {e}")

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()
        logging.info("Database connection closed.")
