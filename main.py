import logging
import time
from database.db_handler import DatabaseHandler
from scripts.search_request import SearchRequest
from database.db_utils import DBUtils
from database.view_stats import view_stats
from scripts.utils import get_checkpoint, save_checkpoint, read_input_csv

def main(query=None, search_type="title", max_pages=None, input_csv=None):
    """
    Main function to run the script logic.
    """
    start_time = time.time()
    logging.info("Starting the script...")

    # Initialize database connections
    db = DatabaseHandler()
    db_utils = DBUtils()
    conn = db.conn

    # Load queries from CSV or use provided query
    queries = []
    if input_csv:
        queries = read_input_csv(input_csv)
        logging.info(f"Parsed queries: {queries}")
    elif query:
        queries = [{"query": query, "search_type": search_type}]
    else:
        logging.error("No query provided and no CSV file specified. Exiting...")
        return

    deduplication_triggered = False  # Track deduplication status

    for item in queries:
        current_query = item["query"]
        current_search_type = item["search_type"]

        # Retrieve checkpoint
        last_page = get_checkpoint(conn, current_query, current_search_type)
        logging.info(f"Resuming query '{current_query}' ({current_search_type}) from page {last_page + 1}")

        try:
            # Initialize search
            search = SearchRequest(current_query, current_search_type, results_per_page=100)
            all_books = search.aggregate_request_data(max_pages=max_pages, start_page=last_page + 1)

            # Log the type and structure of all_books
            # logging.info(f"Type of all_books: {type(all_books)}, Sample: {all_books[:5]}")

            # Process books and save checkpoints
            for book in all_books:
                if isinstance(book, dict) and "ID" in book:
                    if not db_utils.check_duplicate(book["ID"]):
                        db.insert_book(book)
                else:
                    logging.error(f"Invalid book entry: {book}")

            # Clear checkpoint after query completion
            conn.execute("DELETE FROM checkpoints WHERE query = ? AND search_type = ?", (current_query, current_search_type))
            logging.info(f"Completed query '{current_query}', checkpoint cleared.")

        except Exception as e:
            save_checkpoint(conn, current_query, current_search_type, last_page)
            logging.error(f"Error processing query '{current_query}': {e}")
            continue  # Skip to the next query


        deduplication_triggered = True  # Set flag to indicate deduplication needs to be run

    # Deduplicate books if required
    if deduplication_triggered:
        logging.info("Starting deduplication process...")
        db_utils.deduplicate_books()

    # View statistics
    logging.info("Viewing database statistics...")
    view_stats(db_path=db.db_path)

    # Close database connections
    db.close()
    db_utils.close()

    # Log script completion
    logging.info(f"Script completed in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    import argparse

    # Argument parsing
    parser = argparse.ArgumentParser(description="Run LibGen scraping script with optional CSV automation.")
    parser.add_argument("--query", type=str, help="Search query (e.g., book title, author, or topic).")
    parser.add_argument("--search_type", type=str, default="title", choices=["title", "author", "default"], help="Type of search (default: title).")
    parser.add_argument("--max_pages", type=int, default=None, help="Maximum number of pages to fetch (default: None, fetch all).")
    parser.add_argument("--input_csv", type=str, help="Path to a CSV file containing queries and search types.")
    args = parser.parse_args()

    main(query=args.query, search_type=args.search_type, max_pages=args.max_pages, input_csv=args.input_csv)
