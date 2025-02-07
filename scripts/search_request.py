"""
search_request.py

This module defines the SearchRequest class, which handles the logic of
searching LibGen (Library Genesis) pages, parsing the results, and providing
structured metadata output for each book. It includes pagination support
and the ability to aggregate results across multiple pages.
"""

import requests
from bs4 import BeautifulSoup
import urllib.parse
import logging

# Set up logging for better debugging and monitoring
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SearchRequest:
    """
    A class to handle searching Library Genesis (LibGen) with pagination for large datasets.

    Attributes:
        query (str): The user-provided query string (e.g., book title or author).
        search_type (str): The type of search ("title", "author", or "default").
        results_per_page (int): Number of results to fetch per page.
    """

    col_names = [
        "ID", "Author", "Title", "Publisher", "Year", "Pages",
        "Language", "Size", "Extension", "Mirror_1", "Mirror_2", "Edit"
    ]

    def __init__(self, query, search_type="title", results_per_page=100):
        """
        Initialize the SearchRequest object with query parameters.

        Args:
            query (str): The search query (e.g., book title, author name, or keyword).
            search_type (str): The type of search to perform ('title', 'author', or 'default').
            results_per_page (int): Number of results to fetch per page (default is 100).

        Raises:
            ValueError: If the query is empty or the search_type is invalid.
        """
        self.query = query.strip()
        self.search_type = search_type.lower()
        self.results_per_page = results_per_page

        if len(self.query) < 1:
            raise ValueError("Query must be at least 1 character long.")
        if self.search_type not in ["title", "author", "default"]:
            raise ValueError("Search type must be one of: 'title', 'author', 'default'.")

        logging.info(
            f"Initialized SearchRequest with query: '{self.query}', "
            f"search_type: '{self.search_type}', "
            f"and results_per_page: {self.results_per_page}"
        )

    def get_search_page(self, page=1):
        """
        Fetch the results page for the given query and page number.

        Args:
            page (int): The page number to fetch (default is 1).

        Returns:
            str: The raw HTML content of the search results page.

        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        query_parsed = urllib.parse.quote_plus(self.query)
        base_url = "https://libgen.is/search.php"

        if self.search_type == "title":
            search_url = f"{base_url}?req={query_parsed}&column=title&res={self.results_per_page}&page={page}"
        elif self.search_type == "author":
            search_url = f"{base_url}?req={query_parsed}&column=author&res={self.results_per_page}&page={page}"
        else:
            search_url = f"{base_url}?req={query_parsed}&column=def&res={self.results_per_page}&page={page}"

        logging.info(f"Fetching search page {page} from URL: {search_url}")

        try:
            response = requests.get(search_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Failed to fetch search page {page}: {e}")
            raise

        return response.text

    def parse_search_results(self, html_content):
        """
        Parse the HTML content of a single search-results page to extract book data.

        Args:
            html_content (str): The HTML content of the LibGen search results page.

        Returns:
            list of dict: Each dict contains metadata for one book. If the table 
                          structure is missing or invalid, an empty list is returned.
        """
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove <i> tags to avoid them breaking table parsing
        for subheading in soup.find_all("i"):
            subheading.decompose()

        # Attempt to find the results table
        try:
            information_table = soup.find_all("table")[2]
        except IndexError:
            logging.error("Failed to locate the search results table in the HTML.")
            return []

        # Skip the header row
        rows = information_table.find_all("tr")[1:]
        if not rows:
            logging.warning("No results found on this page.")
            return []

        structured_data = []
        for row in rows:
            tds = row.find_all("td")

            # We expect 12 columns in each row
            if len(tds) < 12:
                continue

            row_cells = []
            for col_idx in range(12):
                td = tds[col_idx]

                # Mirror_1, Mirror_2, Edit columns contain one or more links
                if col_idx in (9, 10, 11):  # Indices for Mirror_1, Mirror_2, Edit
                    anchors = td.find_all("a")
                    link_list = [a.get("href", "") for a in anchors]
                    row_cells.append(link_list)
                else:
                    row_cells.append(td.text.strip())

            book_data = dict(zip(self.col_names, row_cells))
            structured_data.append(book_data)

        logging.info(f"Extracted {len(structured_data)} rows from the search page.")
        return structured_data

    def aggregate_request_data(self, max_pages=None, start_page=1):
        """
        Orchestrate the entire workflow to fetch and parse data across multiple pages.

        Args:
            max_pages (int, optional): Maximum number of pages to fetch. If None, 
                continues until no more results are found.
            start_page (int): The page to start fetching from (default is 1).

        Returns:
            list of dict: A list of dictionaries containing all fetched book data.
        """
        all_data = []
        page = start_page

        while True:
            logging.info(f"Processing page {page}...")
            try:
                html_content = self.get_search_page(page)
                parsed_data = self.parse_search_results(html_content)
            except Exception as e:
                logging.error(f"Error processing page {page}: {e}")
                break

            if not parsed_data:
                logging.info("No more results found. Stopping pagination.")
                break

            all_data.extend(parsed_data)

            # If a max_pages is specified, stop when it's reached
            if max_pages and page >= start_page + max_pages - 1:
                logging.info(f"Reached the maximum number of pages ({max_pages}). Stopping pagination.")
                break

            page += 1

        logging.info(f"Total books fetched: {len(all_data)}")
        return all_data
