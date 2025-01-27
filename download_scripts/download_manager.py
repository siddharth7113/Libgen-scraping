import os
import logging
import time
import requests
from download_scripts.download_util import DownloadUtils
from database.db_handler import DatabaseHandler
import asyncio
from asyncio import Semaphore

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DownloadManager:
    def __init__(self, db_handler, download_utils, max_concurrent_tasks=10):
        """
        Initialize the DownloadManager with DatabaseHandler and DownloadUtils.

        Args:
            db_handler (DatabaseHandler): Instance of the DatabaseHandler class.
            download_utils (DownloadUtils): Instance of the DownloadUtils class.
        """
        self.db_handler = db_handler
        self.download_utils = download_utils
        self.semaphore = Semaphore(max_concurrent_tasks)  # Limit concurrency

    async def fetch_download_link(self, book_id, mirror1, mirror2, retries=5, backoff_factor=1):
        """
        Fetch a download link with retries and exponential backoff.

        Args:
            book_id (int): The ID of the book being processed.
            mirror1 (str): The Mirror 1 URL.
            mirror2 (str): The Mirror 2 URL.
            retries (int): Number of retry attempts.
            backoff_factor (int): Multiplier for exponential backoff delays.

        Returns:
            str: The fetched download link or None if unsuccessful.
        """
        for attempt in range(1, retries + 1):
            logging.info(f"Fetching link for book ID={book_id}, attempt {attempt}...")
            try:
                # Fetch links concurrently for better performance
                mirror1_task = self.download_utils.fetch_mirror1_download_link(mirror1)
                mirror2_task = self.download_utils.fetch_mirror2_download_link(mirror2)
                mirror1_link, mirror2_link = await asyncio.gather(mirror1_task, mirror2_task)

                # Return the first successful link
                if mirror1_link or mirror2_link:
                    return mirror1_link or mirror2_link

            except Exception as e:
                logging.error(f"Error fetching links for book ID={book_id}, attempt {attempt}: {e}")

            # Exponential backoff before retrying
            backoff_delay = backoff_factor * (2 ** (attempt - 1))
            logging.warning(f"Retrying book ID={book_id} in {backoff_delay} seconds...")
            await asyncio.sleep(backoff_delay)

        # Mark the book as failed after retries
        await self.db_handler.update_link_status(book_id, "Failed", "Link not found after retries")
        return None



    async def download_file(self, url, destination):
        """Download a file from a URL and save it to the destination path."""
        return await self.download_utils.download_file(url, destination)

    async def process_book(self, book):
        async with self.semaphore:
            try:
                book_id, language, author, extension, title, year, mirror1, mirror2 = book
                metadata_entry = (language, author, extension, title, year)
                self.download_utils.create_base_directory(metadata_entry)
                file_path = self.download_utils.get_file_path(metadata_entry)

                if os.path.exists(file_path):
                    logging.info(f"File already exists: {file_path}. Skipping download.")
                    await self.db_handler.update_link_status(book_id, "Skipped")
                    return

                # Fetch the download link
                download_link = await self.fetch_download_link(book_id, mirror1, mirror2)
                if not download_link:
                    logging.error(f"No valid link found for book ID={book_id}.")
                    return

                # Download the file
                if await self.download_file(download_link, file_path):
                    await self.db_handler.update_link_status(book_id, "Downloaded")
                else:
                    await self.db_handler.update_link_status(book_id, "Failed", "Download error")
            except Exception as e:
                logging.error(f"Error processing book ID={book[0]}: {e}")


    async def process_files(self):
        """
        Process books to fetch valid download links, download files, and update the database.
        """
        query = """
            SELECT id, language, author, extension, title, year, mirror_1, mirror_2
            FROM books
            WHERE link_status IS NULL OR link_status = 'Pending';
        """
        cursor = self.db_handler.conn.cursor()
        cursor.execute(query)
        books = cursor.fetchall()

        # Process all books with error handling
        results = await asyncio.gather(
            *(self.process_book(book) for book in books),
            return_exceptions=True
        )

        # Log errors from gather
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Error in processing task: {result}")

        logging.info("File processing completed.")



if __name__ == "__main__":
    # Set up constants
    DATABASE_NAME = "books.db"
    BASE_DIRECTORY = "dataset"

    async def main():
        try:
            # Initialize the database handler and download utilities
            db_handler = DatabaseHandler(db_name=DATABASE_NAME)
            download_utils = DownloadUtils(db_path=f"database/{DATABASE_NAME}", base_directory=BASE_DIRECTORY)

            # Initialize the DownloadManager
            download_manager = DownloadManager(db_handler=db_handler, download_utils=download_utils)

            # Process files asynchronously
            logging.info("Starting the download process ...")
            await download_manager.process_files()
            logging.info("Download process completed.")

        except Exception as e:
            logging.error(f"An error occurred: {e}")

        finally:
            # Close database connections
            if 'db_handler' in locals():
                db_handler.close()
            if 'download_utils' in locals():
                download_utils.close_connection()
try:
    asyncio.run(main())
except asyncio.CancelledError:
    logging.info("Process terminated by user. Cleaning up...")
