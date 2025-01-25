import pytest
from scripts.search_request import SearchRequest


@pytest.fixture
def search_request_instance():
    """
    Fixture to initialize a SearchRequest instance for testing.
    """
    return SearchRequest("Python", search_type="title", results_per_page=10)


def test_initialization():
    """
    Test the initialization of the SearchRequest class.
    """
    request = SearchRequest("Python", search_type="author", results_per_page=10)
    assert request.query == "Python"
    assert request.search_type == "author"
    assert request.results_per_page == 10


def test_invalid_initialization():
    """
    Test invalid initialization of the SearchRequest class.
    """
    with pytest.raises(ValueError):
        SearchRequest("", search_type="title")  # Empty query

    with pytest.raises(ValueError):
        SearchRequest("Python", search_type="invalid")  # Invalid search type


def test_get_search_page(search_request_instance):
    """
    Test fetching a search page.
    """
    html_content = search_request_instance.get_search_page(page=1)
    assert isinstance(html_content, str), "HTML content should be a string."
    assert "<html" in html_content.lower(), "Response should contain valid HTML."


def test_parse_search_results(search_request_instance):
    """
    Test parsing search results.
    """
    html_content = search_request_instance.get_search_page(page=1)
    results = search_request_instance.parse_search_results(html_content)
    assert isinstance(results, list), "Parsed results should be a list."
    assert all(isinstance(book, dict) for book in results), "Each result should be a dictionary."


def test_aggregate_request_data(search_request_instance):
    """
    Test aggregating search data across pages.
    """
    all_data = search_request_instance.aggregate_request_data(max_pages=2)
    assert isinstance(all_data, list), "Aggregated data should be a list."
    assert len(all_data) > 0, "There should be some results in the aggregated data."
