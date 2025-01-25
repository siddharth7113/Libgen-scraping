import sqlite3
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DatabaseHandler:
    def __init__(self, db_name="books.db"):
        """
        Initialize the database connection and create the required tables.
        The database is stored in the 'database' folder.
        """
        # Ensure the 'database' folder exists
        os.makedirs("database", exist_ok=True)

        # Set the full path for the database file
        self.db_path = os.path.join("database", db_name)
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()

    def create_tables(self):
        """
        Create the required tables (books and checkpoints) if they don't already exist.
        """
        with self.conn:
            # Create books table
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
                    query TEXT,
                    search_type TEXT
                );
            """)
            logging.info("Books table ensured in database.")

            # Create checkpoints table
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


    def insert_book(self, book):
        """
        Insert a single book into the database.
        """
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT OR IGNORE INTO books (
                        libgen_id, author, title, publisher, year, pages, language, size,
                        extension, mirror_1, mirror_2, direct_link, query, search_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    book["ID"], book["Author"], str(book["Title"]),
                    book["Publisher"], book["Year"], book["Pages"], book["Language"],
                    book["Size"], book["Extension"], str(book["Mirror_1"]),
                    str(book["Mirror_2"]), book.get("Direct_Download_Link", ""),
                    book.get("query", ""), book.get("search_type", "")
                ))
            logging.info(f"Inserted book ID={book['ID']} into the database.")
        except sqlite3.Error as e:
            logging.error(f"Error inserting book ID={book['ID']}: {e}")


    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()
        logging.info("Database connection closed.")
