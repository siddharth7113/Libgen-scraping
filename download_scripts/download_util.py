import os
import sqlite3
import logging
import requests
from bs4 import BeautifulSoup
import re
import aiohttp
import asyncio


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DownloadUtils:
    def __init__(self, db_path="database/books.db", base_directory="dataset"):
        """
        Initialize DownloadUtils with database connection and base directory for downloads.
        """
        self.db_path = db_path
        self.base_directory = base_directory
        self.conn = self.connect_to_db()
        # self.create_base_directory()

    def connect_to_db(self):
        """Connect to the SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database at {self.db_path}")
            return conn
        except sqlite3.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def close_connection(self):
        """Close the SQLite database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def create_base_directory(self, metadata_entry):
        """
        Create base directories for language and file format.
        """
        language, _, extension, _, _ = metadata_entry

        # Use only the first word of the language field
        language = (language or "Unknown").strip().split()[0]
        # Sanitize the language to remove special characters
        language = re.sub(r"[^\w\s]", "", language)

        extension = (extension or "Unknown").strip().lower()

        # Full path for language and extension
        directory = os.path.join(self.base_directory, language, extension)

        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")
            except Exception as e:
                logging.error(f"Failed to create directory {directory}: {e}")
        else:
            logging.info(f"Directory already exists: {directory}")


    def get_file_path(self, metadata_entry):
        """
        Generate the full file path for a download based on metadata.

        Args:
            metadata_entry (tuple): A tuple with fields ('Language', 'Author', 'Extension', 'Title', 'Year').

        Returns:
            str: The full path for the file to be downloaded.
        """
        language, author, extension, title, year = metadata_entry

        # Use only the first word of the language field
        language = (language or "Unknown").strip().split()[0]
        # Sanitize the language to remove special characters
        language = re.sub(r"[^\w\s]", "", language)

        extension = (extension or "Unknown").strip().lower()
        title = (title or "Untitled").strip()
        author = (author or "Unknown").strip()
        year = str(year).strip() if year else "Unknown"

        # Sanitize filename to remove illegal characters
        filename = f"{title}_{author}_{year}.{extension}"
        filename = re.sub(r"[^\w\s.-]", "_", filename)  # Replace illegal characters with an underscore
        filename = re.sub(r"\s+", "_", filename)  # Replace spaces with underscores

        # Full path including language and extension directory
        return os.path.join(self.base_directory, language, extension, filename)


    async def fetch_mirror1_download_link(self, mirror1_url):
        """Fetch the download link from Mirror 1."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(mirror1_url, timeout=10) as response:
                    if response.status != 200:
                        logging.error(f"Mirror 1 failed with HTTP {response.status}")
                        return None
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    download_section = soup.find("div", id="download")
                    if not download_section:
                        return None
                    for anchor in download_section.find_all("a"):
                        if anchor.text.strip() == "GET":
                            return anchor["href"]
        except aiohttp.ClientError as e:
            logging.error(f"Mirror 1 error: {e}")
        return None  # Explicitly return None if no link is found


    async def fetch_mirror2_download_link(self, mirror2_url):
            """Asynchronously fetch the download link from Mirror 2."""
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(mirror2_url, timeout=10) as response:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")
                        for anchor in soup.find_all("a"):
                            if "md5=" in anchor["href"]:
                                return anchor["href"]
            except aiohttp.ClientError as e:
                logging.error(f"Mirror 2 error: {e}")
            return None
    
    async def download_file(self, url, destination):
        """Asynchronously download a file."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        os.makedirs(os.path.dirname(destination), exist_ok=True)
                        with open(destination, "wb") as f:
                            while chunk := await response.content.read(8192):
                                f.write(chunk)
                        logging.info(f"Downloaded file: {destination}")
                        return True
                    else:
                        logging.error(f"Failed to download {url}. HTTP Status: {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Failed to download {url}: {e}")
        return False