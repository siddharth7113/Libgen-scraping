def test_search_title(libgen_search_instance, random_query):
    results = libgen_search_instance.search_title(random_query, max_pages=2)
    if not results:
        results = libgen_search_instance.search_title("Python", max_pages=2)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all("Title" in book for book in results)


def test_search_author(libgen_search_instance, random_query):
    results = libgen_search_instance.search_author(random_query, max_pages=2)
    if not results:
        results = libgen_search_instance.search_author("Stephen King", max_pages=2)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all("Author" in book for book in results)


def test_search_default(libgen_search_instance, random_query):
    results = libgen_search_instance.search_default(random_query, max_pages=2)
    if not results:
        results = libgen_search_instance.search_default("Python", max_pages=2)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all("Title" in book for book in results)
