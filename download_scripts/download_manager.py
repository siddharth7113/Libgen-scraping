"""
download_manager.py

This module defines the DownloadManager class, which orchestrates the process
of fetching direct download links for books from LibGen mirrors and then
downloading them. It uses concurrency features (asyncio) to manage multiple
download tasks efficiently.

It integrates with:
    - DatabaseHandler (from db_handler.py) for updating book records with link statuses.
    - DownloadUtils (from download_util.py) for link extraction and actual file download logic.
"""

import os
import logging
import asyncio
from asyncio import Semaphore

from download_scripts.download_util import DownloadUtils
from database.db_handler import DatabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class DownloadManager:
    """
    Manages the overall download workflow:
      1) Checking if a direct link is already stored in the database;
      2) Fetching a download link (Mirror 1 or Mirror 2) if needed;
      3) Downloading the file to the appropriate location;
      4) Updating the database with the result (e.g., "Downloaded", "Failed", etc.).

    Attributes:
        db_handler (DatabaseHandler): The async database handler instance.
        download_utils (DownloadUtils): The utility class handling actual link extraction
            and file-download operations.
        semaphore (asyncio.Semaphore): Limits concurrent download tasks.
    """

    def __init__(self, db_handler, download_utils, max_concurrent_tasks=2):
        """
        Initialize the DownloadManager with a DatabaseHandler and DownloadUtils.

        Args:
            db_handler (DatabaseHandler): The async database handler for reading/writing
                book records (e.g., direct link, link status).
            download_utils (DownloadUtils): The utility class providing low-level download
                operations (HTTP requests, saving files, etc.).
            max_concurrent_tasks (int, optional): The maximum number of concurrent download
                tasks that can run at once. Defaults to 2.
        """
        self.db_handler = db_handler
        self.download_utils = download_utils
        self.semaphore = Semaphore(max_concurrent_tasks)

    async def fetch_download_link(self, book_id, mirror1, mirror2, retries=5, backoff_factor=1):
        """
        Attempt to fetch a direct download link for a book using Mirror 1 and Mirror 2.

        1. Check if the database already has a usable direct link.
        2. If not, repeatedly attempt to fetch from Mirror 1 and Mirror 2 up to `retries` times
           with exponential backoff.
        3. If a valid link is found, update the database with that link.

        Args:
            book_id (int): The ID of the book in the database.
            mirror1 (str): The Mirror 1 URL stored in the 'books' table.
            mirror2 (str): The Mirror 2 URL stored in the 'books' table.
            retries (int, optional): Number of retry attempts if fetching fails.
            backoff_factor (int, optional): Used in exponential backoff (sleep time grows as 2^(attempt-1)).

        Returns:
            str or None: The direct download link if found; None otherwise.
        """
        # 1. Check if a valid direct link is already stored in the database.
        async with self.db_handler.conn.execute(
            "SELECT direct_link FROM books WHERE id = ?",
            (book_id,)
        ) as cursor:
            row = await cursor.fetchone()

        if row and row[0] and not row[0].startswith("setlang.php"):
            # Already have a valid direct link
            logging.info(f"✅ Using stored direct link for book ID={book_id}: {row[0]}")
            return row[0]

        # 2. If not found, try fetching from Mirror 1 and Mirror 2
        for attempt in range(1, retries + 1):
            logging.info(f"🔍 Fetching link for book ID={book_id}, attempt {attempt}...")

            # Mirror 1 fetch
            try:
                mirror1_link = await self.download_utils.fetch_mirror1_download_link(mirror1)
            except Exception as e:
                logging.error(f"❌ Mirror 1 failed for book ID={book_id}: {e}")
                mirror1_link = None

            # Mirror 2 fetch
            try:
                mirror2_link = await self.download_utils.fetch_mirror2_download_link(mirror2)
            except Exception as e:
                logging.error(f"❌ Mirror 2 failed for book ID={book_id}: {e}")
                mirror2_link = None

            # If either link is found, use it
            if mirror1_link or mirror2_link:
                final_link = mirror2_link or mirror1_link
                # If the link is not the default "setlang.php" (invalid placeholder)
                if not final_link.startswith("setlang.php"):
                    # Update the direct_link column in the database
                    await self.db_handler.update_direct_link(book_id, final_link)
                    return final_link
                else:
                    logging.warning(f"⚠️ Ignoring invalid link: {final_link}")

            # Exponential backoff before retrying
            backoff_delay = backoff_factor * (2 ** (attempt - 1))
            logging.warning(f"⏳ Retrying book ID={book_id} in {backoff_delay} seconds...")
            await asyncio.sleep(backoff_delay)

        # 3. If all retries fail, update the status as "Failed"
        await self.db_handler.update_link_status(book_id, "Failed", "Link not found after retries")
        return None

    async def download_file(self, url, destination):
        """
        Delegate the file download task to DownloadUtils.

        Args:
            url (str): The direct download URL for the file.
            destination (str): The full path (including filename) where the file should be saved.

        Returns:
            bool: True if download succeeded; False otherwise.
        """
        return await self.download_utils.download_file(url, destination)

    async def process_book(self, book):
        """
        Process a single book entry from the database, performing:
          1) Link fetching (if necessary).
          2) Downloading the file to the appropriate path.
          3) Updating the database with the final status (Downloaded, Failed, Skipped, etc.).

        This function is called concurrently for multiple books, but is managed
        by a semaphore to limit concurrency.

        Args:
            book (tuple): A tuple of book fields, typically:
                (id, language, author, extension, title, year, mirror1, mirror2).

        Returns:
            None
        """
        async with self.semaphore:
            logging.info(f"Processing book ID={book[0]} started.")
            try:
                (book_id, language, author, extension, title, year, mirror1, mirror2) = book

                # Create the directory structure for this book based on its metadata
                metadata_entry = (language, author, extension, title, year)
                self.download_utils.create_base_directory(metadata_entry)
                file_path = self.download_utils.get_file_path(metadata_entry)

                # If the file already exists, mark status as "Skipped"
                if os.path.exists(file_path):
                    logging.info(f"File already exists: {file_path}. Skipping download.")
                    await self.db_handler.update_link_status(book_id, "Skipped")
                    return

                # Attempt to fetch a direct link (if not already in DB)
                download_link = await self.fetch_download_link(book_id, mirror1, mirror2)
                if not download_link:
                    logging.error(f"No valid link found for book ID={book_id}.")
                    return

                # Download the file; update status based on the result
                if await self.download_file(download_link, file_path):
                    await self.db_handler.update_link_status(book_id, "Downloaded")
                else:
                    await self.db_handler.update_link_status(book_id, "Failed", "Download error")

            except Exception as e:
                logging.error(f"Error processing book ID={book[0]}: {e}")
            finally:
                logging.info(f"Processing book ID={book[0]} completed.")

    async def process_files(self):
        """
        Fetch all books from the database with a 'Pending' or NULL link_status,
        then concurrently run downloads using process_book().

        The concurrency level is governed by the semaphore set in __init__.

        Returns:
            None
        """
        # Retrieve all "pending" books
        async with self.db_handler.conn.execute("""
            SELECT id, language, author, extension, title, year, mirror_1, mirror_2
            FROM books
            WHERE link_status IS NULL OR link_status = 'Pending';
        """) as cursor:
            books = await cursor.fetchall()

        # Process each book concurrently using asyncio.gather
        results = await asyncio.gather(
            *(self.process_book(book) for book in books),
            return_exceptions=True
        )

        # Aggregate results for logging
        success_count = 0
        failure_count = 0
        for result in results:
            if isinstance(result, Exception):
                failure_count += 1
                logging.error(f"Error in processing task: {result}")
            else:
                success_count += 1

        logging.info(f"File processing completed. Successes: {success_count}, Failures: {failure_count}")


# -----------------------------------------------------------------------------
# Main entry point (example usage).
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    """
    Example usage: 
      python download_manager.py

    This will instantiate the DatabaseHandler, DownloadUtils, and DownloadManager,
    then process all "pending" books in the database.
    """
    DATABASE_NAME = "books.db"
    BASE_DIRECTORY = "dataset"

    import asyncio

    async def main():
        try:
            # Initialize the async DB handler and open DB connection
            db_handler = DatabaseHandler(db_name=DATABASE_NAME)
            await db_handler.init()

            # Initialize download utilities
            download_utils = DownloadUtils(
                db_path=f"database/{DATABASE_NAME}",
                base_directory=BASE_DIRECTORY
            )

            # Create and run the download manager
            download_manager = DownloadManager(
                db_handler=db_handler,
                download_utils=download_utils
            )

            logging.info("Starting the download process...")
            await download_manager.process_files()
            logging.info("Download process completed.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        finally:
            # Clean up resources
            if 'db_handler' in locals():
                await db_handler.close()
            if 'download_utils' in locals():
                await download_utils.close_session()
                download_utils.close_connection()

    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        logging.info("Process terminated by user. Cleaning up...")
