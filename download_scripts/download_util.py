"""
download_util.py

This module provides the DownloadUtils class, which encapsulates helper
functions for:

- Database connections (checking open/close status).
- Directory creation for downloaded files (organized by language/extension).
- Extracting direct download links from LibGen mirrors (Mirror 1 & Mirror 2).
- Downloading files with asyncio + aiohttp (including retries and partial data handling).

Note:
  - Mirror 1 extraction uses Playwright to render JS-based pages.
  - Mirror 2 extraction parses the HTML for a known pattern (e.g., 'get.php').
  - Rate limiting is enforced to avoid overwhelming LibGen servers.
"""

import os
import sqlite3
import logging
from bs4 import BeautifulSoup
import re
import aiohttp
import asyncio
import random
from urllib.parse import urljoin
from aiohttp import ClientPayloadError
from playwright.async_api import async_playwright

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Placeholder list of proxies (not currently used)
PROXIES = []

class DownloadUtils:
    """
    Provides low-level utilities to:
      1) Maintain a persistent aiohttp session for repeated requests.
      2) Fetch and parse LibGen mirror pages, extracting direct download links.
      3) Download files with retries, partial data handling, and concurrency control.
      4) Organize downloaded files into directories based on metadata (language, extension, etc.).

    Attributes:
        db_path (str): Path to the local SQLite database file.
        base_directory (str): Top-level directory to store downloaded files.
        conn (sqlite3.Connection): Synchronous connection to the SQLite database (used minimally).
        proxies (list): Optional list of proxies (not actively used at the moment).
        rate_limit (int): Maximum concurrent HTTP requests.
        semaphore (asyncio.Semaphore): Used to enforce rate_limit in throttled_fetch.
        _session (aiohttp.ClientSession): Persistent aiohttp session for reuse.
    """

    def __init__(self, db_path="database/books.db", base_directory="dataset", proxies=PROXIES, rate_limit=3):
        """
        Initialize DownloadUtils.

        Args:
            db_path (str): The path to the SQLite database file.
            base_directory (str): Base directory where downloaded files will be stored.
            proxies (list, optional): A list of proxy URLs for rotating requests (not implemented here).
            rate_limit (int, optional): Maximum number of concurrent HTTP requests.
                                        Lower can reduce load on servers.
        """
        self.db_path = db_path
        self.base_directory = base_directory
        self.conn = self.connect_to_db()
        self.proxies = proxies or []
        self.rate_limit = rate_limit
        self.semaphore = asyncio.Semaphore(rate_limit)
        self._session = None  # Will be created on-demand in get_session()

    def connect_to_db(self):
        """
        Connect to the local SQLite database for minimal use (e.g., logging).
        
        Returns:
            sqlite3.Connection: An open connection to the SQLite database.
        
        Raises:
            sqlite3.Error: If connection fails.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database at {self.db_path}")
            return conn
        except sqlite3.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def close_connection(self):
        """
        Close the SQLite database connection if it's open.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    async def get_session(self):
        """
        Retrieve (or create) a persistent aiohttp.ClientSession with common headers.
        
        Returns:
            aiohttp.ClientSession: A session that can be reused for multiple requests.
        """
        if self._session is None or self._session.closed:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/98.0.4758.102 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "http://books.ms/"  # Example referer header
            }
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def close_session(self):
        """
        Close the persistent aiohttp session if it is open.
        """
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    def create_base_directory(self, metadata_entry):
        """
        Create a directory structure for storing downloaded files based on metadata.
        
        The pattern is: base_directory/language/extension/
        
        Args:
            metadata_entry (tuple): Typically (language, author, extension, title, year).
        """
        language, _, extension, _, _ = metadata_entry
        
        # Normalize the language field
        language = (language or "Unknown").strip().split()[0]
        language = re.sub(r"[^\w\s]", "", language)  # Remove punctuation
        extension = (extension or "Unknown").strip().lower()

        directory = os.path.join(self.base_directory, language, extension)
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")
            except OSError as e:
                logging.error(f"Failed to create directory {directory}: {e}")
        else:
            logging.info(f"Directory already exists: {directory}")

    def get_file_path(self, metadata_entry):
        """
        Generate a file path (including filename) for a book based on its metadata.
        
        Args:
            metadata_entry (tuple): (language, author, extension, title, year).

        Returns:
            str: The full path to where the file should be saved.
        """
        language, author, extension, title, year = metadata_entry

        # Clean/normalize fields
        language = (language or "Unknown").strip().split()[0]
        language = re.sub(r"[^\w\s]", "", language)
        extension = (extension or "Unknown").strip().lower()
        title = (title or "Untitled").strip()[:100]  # Limit length for filename
        author = (author or "Unknown").strip()[:50]
        year = str(year).strip() if year else "Unknown"

        filename = f"{title}_{author}_{year}.{extension}"
        # Replace invalid filename characters with underscores
        filename = re.sub(r"[^\w\s.-]", "_", filename)
        # Replace spaces with underscores
        filename = re.sub(r"\s+", "_", filename)

        return os.path.join(self.base_directory, language, extension, filename)

    def get_random_proxy(self):
        """
        Return a random proxy from the list or None if the list is empty.
        Currently unused in the throttled_fetch call.

        Returns:
            str or None: The proxy URL if available, otherwise None.
        """
        if self.proxies:
            return random.choice(self.proxies)
        return None

    async def throttled_fetch(self, url, proxy=None, timeout=10):
        """
        Perform a GET request to the specified URL, respecting the rate limit (semaphore).
        
        Args:
            url (str): The URL to fetch.
            proxy (str, optional): Proxy URL if used. Currently None or unused.
            timeout (int, optional): Timeout in seconds for the request.
        
        Returns:
            aiohttp.ClientResponse or None: The response object if successful, otherwise None.
        """
        async with self.semaphore:
            try:
                session = await self.get_session()
                async with session.get(url, timeout=timeout, proxy=proxy) as response:
                    return response
            except Exception as e:
                logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
                return None

    async def fetch_mirror1_download_link(self, mirror1_url, retries=3):
        """
        Attempt to extract the direct download link from Mirror 1 using Playwright to render
        JavaScript-enabled pages (often required to bypass certain anti-bot measures).

        Steps:
          1) Use Playwright to open the mirror page, wait for page load.
          2) Parse the rendered HTML with BeautifulSoup.
          3) Look for anchors that match certain patterns (e.g., file extensions).
          4) Optionally check for priority anchor text like "GET", "Cloudflare", etc.

        Args:
            mirror1_url (str): The URL for Mirror 1.
            retries (int, optional): Number of retries to attempt if an error occurs.

        Returns:
            str or None: Direct download URL if found, otherwise None.
        """
        priority_links = ["GET", "Cloudflare", "IPFS.io", "Pinata"]

        for attempt in range(1, retries + 1):
            try:
                async with async_playwright() as p:
                    # Launch a headless browser session
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()

                    # Load the Mirror 1 page (set a 20s timeout)
                    await page.goto(mirror1_url, timeout=20000, wait_until="load")

                    # Get the rendered HTML
                    content = await page.content()

                    # Close browser to free resources
                    await browser.close()

                soup = BeautifulSoup(content, "html.parser")

                # Attempt to find a "download" section if one is present
                download_section = soup.find("div", id="download")
                if not download_section:
                    logging.warning(
                        "Download section not found in rendered Mirror 1 page; checking all anchors..."
                    )

                # Track anchors in a dictionary with anchor_text -> href
                links = {}
                for anchor in soup.find_all("a"):
                    text = anchor.get_text(strip=True)
                    href = anchor.get("href", "").strip()
                    full_url = urljoin(mirror1_url, href)

                    # If the anchor text is one of our priority keywords, store it
                    if text in priority_links:
                        links[text] = full_url

                    # If the href indicates a known file extension, use that
                    if any(ext in href.lower() for ext in [".pdf", ".epub", ".djvu", ".mobi", ".azw3", ".chem"]):
                        logging.info(f"Found direct book link via Playwright: {full_url}")
                        return full_url

                # If we didn't find direct extension links, try priority links
                for key in priority_links:
                    if key in links:
                        logging.info(f"Mirror 1 (Playwright): Selected link ({key}): {links[key]}")
                        return links[key]

            except Exception as e:
                logging.error(f"Playwright-based Mirror 1 fetch error on attempt {attempt}: {e}")

            # Exponential backoff
            await asyncio.sleep(2 ** attempt)

        logging.error(f"Exhausted retries for Mirror 1 URL: {mirror1_url}")
        return None

    async def fetch_mirror2_download_link(self, mirror2_url):
        """
        Extract the download link from Mirror 2's HTML page by searching for anchors
        that contain 'get.php' in their href and have text containing 'GET'.

        Args:
            mirror2_url (str): The URL for Mirror 2.

        Returns:
            str or None: Direct download URL if found, otherwise None.
        """
        try:
            session = await self.get_session()
            async with session.get(mirror2_url, timeout=10) as response:
                if response and response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")

                    for anchor in soup.find_all("a"):
                        href = anchor.get("href", "").strip()
                        anchor_text = anchor.get_text(strip=True)

                        if "get.php" in href.lower() and "GET" in anchor_text.upper():
                            # Construct absolute URL
                            full_url = urljoin(mirror2_url, href)
                            logging.info(f"Mirror2: Found download link: {full_url}")
                            return full_url

                    logging.warning(f"No valid download link found on Mirror2: {mirror2_url}")
                else:
                    if response:
                        logging.error(f"Mirror 2 failed with HTTP status {response.status}.")
                    else:
                        logging.error("Mirror 2 response object is None.")

        except aiohttp.ClientError as e:
            logging.error(f"Mirror 2 error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in fetch_mirror2_download_link: {e}")

        return None

    async def download_file(self, url, destination, retries=3):
        """
        Asynchronously download a file from a direct URL, with retry logic and
        partial data handling (ClientPayloadError).

        Args:
            url (str): The direct link to the file (e.g., from Mirror 1 or Mirror 2).
            destination (str): The full path (including filename) where the file should be saved.
            retries (int, optional): Number of attempts before giving up.

        Returns:
            bool: True if download was successful, False otherwise.
        """
        # 600s total timeout for the request
        timeout = aiohttp.ClientTimeout(total=600)

        for attempt in range(1, retries + 1):
            try:
                session = await self.get_session()
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        # Ensure the destination directory exists
                        os.makedirs(os.path.dirname(destination), exist_ok=True)

                        temp_file = destination + ".tmp"
                        with open(temp_file, "wb") as f:
                            try:
                                # Stream download content in chunks
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)
                            except ClientPayloadError as e:
                                # Log a warning if the payload appears incomplete
                                logging.warning(f"ClientPayloadError encountered: {e}. Proceeding with downloaded data.")

                        # Rename the temp file to the final destination
                        os.rename(temp_file, destination)
                        logging.info(f"Successfully downloaded: {destination}")
                        return True
                    else:
                        logging.error(f"Failed to download {url}. HTTP Status: {response.status} (Attempt {attempt})")
                        # Short delay before retry
                        await asyncio.sleep(1)
            except asyncio.TimeoutError:
                logging.error(f"Timeout occurred for {url} on attempt {attempt}")
                await asyncio.sleep(1)
            except aiohttp.ClientError as e:
                logging.error(f"Error downloading {url}: {e} (Attempt {attempt})")
                await asyncio.sleep(1)

        # If we exhaust all retries, return False
        logging.error(f"Exhausted retries for {url}")
        return False
