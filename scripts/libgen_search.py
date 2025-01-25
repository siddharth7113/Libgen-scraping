# A wrapper on search_requests

import logging
from .search_request import SearchRequest

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class LibgenSearch:
    """
    A high-level wrapper around the SearchRequest class to provide filtered
    and simplified search capabilities for LibGen.
    """

    def __init__(self, results_per_page=100):
        """
        Initialize LibgenSearch with default results per page.

        Args:
            results_per_page (int): Number of results to fetch per page (default: 100).
        """
        self.results_per_page = results_per_page

    def search_default(self, query, max_pages=None):
        """
        Perform a default search (general search) on LibGen.

        Args:
            query (str): The search query.
            max_pages (int): Maximum number of pages to fetch (default: None, fetch all).

        Returns:
            list: List of books matching the query.
        """
        search_request = SearchRequest(query, search_type="default", results_per_page=self.results_per_page)
        return search_request.aggregate_request_data(max_pages)

    def search_title(self, query, max_pages=None):
        """
        Perform a title-based search on LibGen.

        Args:
            query (str): The title to search for.
            max_pages (int): Maximum number of pages to fetch (default: None, fetch all).

        Returns:
            list: List of books matching the title.
        """
        search_request = SearchRequest(query, search_type="title", results_per_page=self.results_per_page)
        return search_request.aggregate_request_data(max_pages)

    def search_author(self, query, max_pages=None):
        """
        Perform an author-based search on LibGen.

        Args:
            query (str): The author to search for.
            max_pages (int): Maximum number of pages to fetch (default: None, fetch all).

        Returns:
            list: List of books matching the author.
        """
        search_request = SearchRequest(query, search_type="author", results_per_page=self.results_per_page)
        return search_request.aggregate_request_data(max_pages)

    def search_with_filters(self, query, search_type="default", filters=None, exact_match=True, max_pages=None):
        """
        Perform a filtered search on LibGen.

        Args:
            query (str): The search query.
            search_type (str): Type of search ('default', 'title', 'author').
            filters (dict): Dictionary of filters to apply (e.g., {"Language": "English"}).
            exact_match (bool): Whether to require exact matches for filters (default: True).
            max_pages (int): Maximum number of pages to fetch (default: None, fetch all).

        Returns:
            list: List of books matching the query and filters.
        """
        search_request = SearchRequest(query, search_type=search_type, results_per_page=self.results_per_page)
        results = search_request.aggregate_request_data(max_pages)
        if filters:
            return self.filter_results(results, filters, exact_match)
        return results

    @staticmethod
    def filter_results(results, filters, exact_match=True):
        """
        Filter results based on provided criteria.

        Args:
            results (list): List of book dictionaries to filter.
            filters (dict): Dictionary of filters to apply.
            exact_match (bool): Whether to require exact matches for filters.

        Returns:
            list: Filtered list of books.
        """
        filtered_results = []
        if exact_match:
            for result in results:
                if filters.items() <= result.items():
                    filtered_results.append(result)
        else:
            for result in results:
                if all(query.lower() in str(result.get(field, "")).lower() for field, query in filters.items()):
                    filtered_results.append(result)
        return filtered_results
