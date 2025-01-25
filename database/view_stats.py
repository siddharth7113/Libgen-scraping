import sqlite3
import logging
from tabulate import tabulate  # For pretty-printing tables

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def view_stats(db_path="database/books.db"):
    """
    View and analyze statistics from the books database for LLM scraping purposes.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Total number of books
        cursor.execute("SELECT COUNT(*) FROM books;")
        total_books = cursor.fetchone()[0]

        # Top 5 keywords in titles
        cursor.execute("""
            SELECT title, COUNT(*) as count
            FROM books
            WHERE title IS NOT NULL AND title != ''
            GROUP BY title
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_titles = cursor.fetchall()

        # Most common languages
        cursor.execute("""
            SELECT language, COUNT(*) as count
            FROM books
            WHERE language IS NOT NULL AND language != ''
            GROUP BY language
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_languages = cursor.fetchall()

        # Most common extensions (file formats)
        cursor.execute("""
            SELECT extension, COUNT(*) as count
            FROM books
            WHERE extension IS NOT NULL AND extension != ''
            GROUP BY extension
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_extensions = cursor.fetchall()

        # Average size of books by format
        cursor.execute("""
            SELECT extension, AVG(CAST(REPLACE(size, ' MB', '') AS REAL)) AS avg_size
            FROM books
            WHERE size LIKE '% MB' AND extension IS NOT NULL
            GROUP BY extension
            ORDER BY avg_size DESC;
        """)
        avg_size_by_format = cursor.fetchall()

        # Display results
        logging.info(f"Total number of books: {total_books}")
        print("\nTop 5 Titles:")
        print(tabulate(top_titles, headers=["Title", "Count"], tablefmt="grid"))

        print("\nTop 5 Languages:")
        print(tabulate(top_languages, headers=["Language", "Count"], tablefmt="grid"))

        print("\nTop 5 File Formats:")
        print(tabulate(top_extensions, headers=["Extension", "Count"], tablefmt="grid"))

        print("\nAverage File Size by Format (MB):")
        print(tabulate(avg_size_by_format, headers=["Format", "Avg Size (MB)"], tablefmt="grid"))

    except sqlite3.Error as e:
        logging.error(f"Error accessing the database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    view_stats()
