import sqlite3
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DBUtils:
    def __init__(self, db_path="database/books.db"):
        """
        Initialize the database utilities with the database path.
        """
        self.db_path = db_path
        self.conn = None
        self.deduplication_executed = False  # Track deduplication status

        # Ensure the database exists
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database file not found at {self.db_path}")

        self.connect()

    def connect(self):
        """
        Connect to the SQLite database.
        """
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=30)  # Add timeout to avoid locks
            logging.info("Database connection established.")
        except sqlite3.Error as e:
            logging.error(f"Failed to connect to the database: {e}")
            raise

    def check_duplicate(self, libgen_id):
        """
        Check if a book with the given LibGen ID already exists in the database.
        
        Args:
            libgen_id (str): The unique LibGen ID of the book.

        Returns:
            bool: True if the book exists, False otherwise.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM books WHERE libgen_id = ? LIMIT 1;", (libgen_id,))
            exists = cursor.fetchone() is not None
            return exists
        except sqlite3.Error as e:
            logging.error(f"Error checking for duplicate: {e}")
            return False

    def deduplicate_books(self):
        """
        Deduplicate books in the database by keeping the latest edition and preferring PDFs.
        """
        if self.deduplication_executed:
            logging.info("Deduplication already executed. Skipping redundant call.")
            return

        try:
            logging.info("Starting deduplication process...")
            self.conn.execute("BEGIN IMMEDIATE")  # Start an immediate transaction to avoid locks

            # Query to identify duplicates
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
            cursor = self.conn.cursor()
            cursor.execute(query)
            duplicate_groups = cursor.fetchall()

            # Process duplicate groups
            for group in duplicate_groups:
                title, author, latest_year, keep_id, all_ids = group
                all_ids_list = all_ids.split(',')

                if keep_id is None:
                    keep_id = all_ids_list[0]  # Keep the first ID if no PDF is available
                    logging.info(f"No PDF found for '{title}', keeping ID={keep_id}.")

                # Remove the preferred record
                all_ids_list.remove(str(keep_id))

                # Delete non-preferred records
                if all_ids_list:
                    ids_to_delete = ', '.join(all_ids_list)
                    delete_query = f"DELETE FROM books WHERE id IN ({ids_to_delete});"
                    self.conn.execute(delete_query)
                    logging.info(f"Deleted duplicate books: {ids_to_delete}")

            self.conn.commit()  # Commit the transaction
            logging.info("Deduplication process completed: latest editions retained, PDFs preferred.")

            self.deduplication_executed = True  # Mark deduplication as executed

        except sqlite3.Error as e:
            logging.error(f"Error during deduplication: {e}")
            self.conn.rollback()  # Rollback in case of an error

    def close(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")
