#!/usr/bin/env python3
"""
GenScavenger - LibGen Scraping and Downloading Tool

This is the main entry point for GenScavenger. It can be used in two ways:
  1. Non-interactive command-line usage (via argparse flags).
  2. Interactive menu (if no CLI arguments are provided).

Features:
  - Reading queries (either from CLI arguments or from a CSV).
  - Using the SearchRequest (or LibgenSearch) class to gather metadata.
  - Storing results in the SQLite database (via DatabaseHandler).
  - Handling deduplication and generating statistics (view_stats).
  - Initiating downloads for all pending books (optional) via DownloadManager.

Usage Examples:
  - Non-interactive, single query:
      python main.py --query "Deep Learning" --search_type title --max_pages 2

  - Non-interactive, multiple queries from CSV:
      python main.py --input_csv "input.csv"

  - If called with no arguments, an interactive menu will appear.

Requirements:
  - pyfiglet (for ASCII banner): pip install pyfiglet
  - If you want download functionality, ensure `download_scripts/` is present 
    and properly installed or importable.
"""

import logging
import time
import sys
import asyncio

from pyfiglet import figlet_format  # For ASCII banner

from database.db_handler import DatabaseHandler
from scripts.search_request import SearchRequest

from database.view_stats import view_stats
from scripts.utils import get_checkpoint, save_checkpoint, read_input_csv

# For optional download functionality
try:
    from download_scripts.download_manager import DownloadManager
    from download_scripts.download_util import DownloadUtils
    DOWNLOAD_FEATURE_AVAILABLE = True
except ImportError:
    DOWNLOAD_FEATURE_AVAILABLE = False
    logging.warning(
        "Download scripts not found. Download functionality will be disabled."
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def print_ascii_banner():
    """
    Print an ASCII banner for GenScavenger using pyfiglet.
    """
    banner = figlet_format("GenScavenger")
    print(banner)

async def scrape_books(query=None, search_type="title", max_pages=None, input_csv=None):
    """
    Scrape books from LibGen based on the provided query/search_type,
    or from a CSV file of queries. Results are stored in 'books.db'.

    Args:
        query (str, optional): A single search query (title, author, or general).
        search_type (str, optional): The search type (title, author, default). 
                                    Defaults to 'title'.
        max_pages (int, optional): The maximum pages to fetch. If None, fetches 
                                all pages until no results found.
        input_csv (str, optional): Path to a CSV file containing multiple queries.
    """
    start_time = time.time()
    logging.info("Starting the scraping logic...")

    # Initialize database connections
    db = DatabaseHandler()   # create object
    await db.init()          # asynchronously connect & create tables
    conn = db.conn           # now an aiosqlite.Connection


    # Determine which queries to run
    queries = []
    if input_csv:
        # Load queries from CSV
        queries = read_input_csv(input_csv)
        logging.info(f"Parsed queries from CSV: {queries}")
    elif query:
        # Use a single CLI query
        queries = [{"query": query, "search_type": search_type}]
    else:
        logging.error("No query provided and no CSV file specified. Exiting scrape_books...")
        return

    deduplication_triggered = False  # Track if deduplication is needed

    for item in queries:
        current_query = item["query"]
        current_search_type = item["search_type"]

        # Retrieve checkpoint from the database
        last_page = await get_checkpoint(conn, current_query, current_search_type)
        logging.info(
            f"Resuming query '{current_query}' ({current_search_type}) from page {last_page + 1}"
        )

        try:
            # Initialize the search
            search = SearchRequest(
                current_query, current_search_type, results_per_page=100
            )

            # Fetch book data (resuming where we left off)
            all_books = search.aggregate_request_data(
                max_pages=max_pages, 
                start_page=last_page + 1
            )

            # Insert each book, avoiding duplicates
            for book in all_books:
                if isinstance(book, dict) and "ID" in book:
                    if not await db.check_duplicate(book["ID"]):
                        db.insert_book(book)
                else:
                    logging.error(f"Invalid book entry encountered: {book}")

            # Clear checkpoint after query completion
            await conn.execute(
                "DELETE FROM checkpoints WHERE query = ? AND search_type = ?",
                (current_query, current_search_type)
            )
            await conn.commit()
            logging.info(f"Completed query '{current_query}', checkpoint cleared.")

        except Exception as e:
            # If something goes wrong, save the current progress as a checkpoint
            await save_checkpoint(conn, current_query, current_search_type, last_page)
            logging.error(f"Error processing query '{current_query}': {e}")
            continue

        deduplication_triggered = True

    # Run deduplication if needed
    if deduplication_triggered:
        logging.info("Starting deduplication process...")
        await db.deduplicate_books()

    # Display summary statistics
    logging.info("Viewing database statistics...")
    view_stats(db_path=db.db_path)

    # Close the database connections
    await db.close()

    elapsed = time.time() - start_time
    logging.info(f"Scraping completed in {elapsed:.2f} seconds.")

async def download_all_pending_books():
    """
    Example async function to download all pending books using DownloadManager.
    This will only be called if download scripts are installed 
    (DOWNLOAD_FEATURE_AVAILABLE == True).
    """
    db_handler = DatabaseHandler(db_name="books.db")
    await db_handler.init()  # async init

    download_utils = DownloadUtils(db_path="database/books.db", base_directory="dataset")
    download_manager = DownloadManager(
        db_handler, download_utils, max_concurrent_tasks=2
    )

    await download_manager.process_files()

    # Close resources
    await db_handler.close()
    await download_utils.close_session()
    download_utils.close_connection()

def interactive_menu():
    """
    Present an interactive menu of options for the user to choose from.
    This approach allows you to do various tasks (scrape, download, stats, etc.)
    without needing to provide CLI arguments.
    """
    while True:
        print("\n--- GenScavenger Main Menu ---")
        print("1) Scrape books from LibGen")
        if DOWNLOAD_FEATURE_AVAILABLE:
            print("2) Download all 'Pending' books")
        print("3) View database statistics")
        print("4) Exit GenScavenger")

        choice = input("Enter your choice (1-4): ").strip()

        if choice == "1":
            # Option 1: Prompt user for scraping parameters
            query = input("Enter your query (or leave blank if you'd like to use a CSV): ").strip()
            csv_path = ""
            if not query:
                csv_path = input("Enter CSV path (e.g., 'input.csv'): ").strip()
                if not csv_path:
                    print("No query or CSV provided. Returning to menu.")
                    continue
            
            # Optionally ask for search type and max_pages
            search_type = input("Search type (title/author/default)? [title]: ").strip() or "title"
            max_pages_str = input("Max pages to fetch? (Press ENTER for no limit): ").strip()
            max_pages = int(max_pages_str) if max_pages_str else None

            # Perform scraping
            asyncio.run(scrape_books(
                query=query if query else None,
                search_type=search_type,
                max_pages=max_pages,
                input_csv=csv_path if csv_path else None
            ))
        elif choice == "2" and DOWNLOAD_FEATURE_AVAILABLE:
            # Download all pending books
            print("Initiating download for all pending books. This may take a while...")
            asyncio.run(download_all_pending_books())
            print("Download process completed.")

        elif choice == "3":
            # View stats
            print("Displaying database statistics...")
            view_stats("database/books.db")

        elif choice == "4":
            print("Exiting GenScavenger. Goodbye!")
            sys.exit(0)

        else:
            print("Invalid choice or feature not available. Please try again.")

def main_cli():
    """
    Main function to run from CLI (non-interactive).
    If no arguments are provided, it falls back to the interactive menu.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Run GenScavenger scraping script with optional CSV automation."
    )
    parser.add_argument("--query", type=str, help="Search query (e.g., book title, author, or topic).")
    parser.add_argument(
        "--search_type", 
        type=str, 
        default="title", 
        choices=["title", "author", "default"], 
        help="Type of search (default: title)."
    )
    parser.add_argument(
        "--max_pages", 
        type=int, 
        default=None, 
        help="Maximum number of pages to fetch (default: None, fetch all)."
    )
    parser.add_argument(
        "--input_csv", 
        type=str, 
        help="Path to a CSV file containing queries and search types."
    )
    args = parser.parse_args()

    # If no CLI arguments, jump to interactive menu
    if not any([args.query, args.input_csv]) and len(sys.argv) == 1:
        print_ascii_banner()
        interactive_menu()
    else:
        # Otherwise, proceed with scraping in non-interactive mode
        print_ascii_banner()
        scrape_books(
            query=args.query,
            search_type=args.search_type,
            max_pages=args.max_pages,
            input_csv=args.input_csv
        )

# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main_cli()
