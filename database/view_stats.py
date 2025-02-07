import sqlite3
import logging
from tabulate import tabulate  # For pretty-printing tables

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def view_stats(db_path="database/books.db"):
    """
    View and analyze statistics from the books database for LLM scraping purposes.

    Args:
        db_path (str, optional): Path to the SQLite database file.
                                 Defaults to "database/books.db".
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1️⃣ Total number of books
        cursor.execute("SELECT COUNT(*) FROM books;")
        total_books = cursor.fetchone()[0]

        # 2️⃣ Download status breakdown
        cursor.execute("SELECT COUNT(*) FROM books WHERE link_status = 'Downloaded';")
        downloaded_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM books WHERE link_status = 'Pending';")
        pending_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM books WHERE link_status = 'Failed';")
        failed_count = cursor.fetchone()[0]

        # 3️⃣ Top 5 repeated titles
        cursor.execute("""
            SELECT title, COUNT(*) as count
            FROM books
            WHERE title IS NOT NULL AND title != ''
            GROUP BY title
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_titles = cursor.fetchall()

        # 4️⃣ Most common languages
        cursor.execute("""
            SELECT language, COUNT(*) as count
            FROM books
            WHERE language IS NOT NULL AND language != ''
            GROUP BY language
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_languages = cursor.fetchall()

        # 5️⃣ Most common extensions (file formats)
        cursor.execute("""
            SELECT extension, COUNT(*) as count
            FROM books
            WHERE extension IS NOT NULL AND extension != ''
            GROUP BY extension
            ORDER BY count DESC
            LIMIT 5;
        """)
        top_extensions = cursor.fetchall()

        # 6️⃣ Download status per language
        cursor.execute("""
            SELECT language, 
                   SUM(CASE WHEN link_status = 'Downloaded' THEN 1 ELSE 0 END) as downloaded,
                   SUM(CASE WHEN link_status = 'Pending' THEN 1 ELSE 0 END) as pending,
                   SUM(CASE WHEN link_status = 'Failed' THEN 1 ELSE 0 END) as failed
            FROM books
            WHERE language IS NOT NULL
            GROUP BY language
            ORDER BY downloaded DESC;
        """)
        lang_download_stats = cursor.fetchall()

        # 7️⃣ Average size of books by format
        cursor.execute("""
            SELECT extension, AVG(CAST(REPLACE(size, ' MB', '') AS REAL)) AS avg_size
            FROM books
            WHERE size LIKE '% MB' AND extension IS NOT NULL
            GROUP BY extension
            ORDER BY avg_size DESC;
        """)
        avg_size_by_format = cursor.fetchall()

        # 8️⃣ Total size by format
        cursor.execute("""
            SELECT extension, SUM(CAST(REPLACE(size, ' MB', '') AS REAL)) AS total_size
            FROM books
            WHERE size LIKE '% MB' AND extension IS NOT NULL
            GROUP BY extension
            ORDER BY total_size DESC;
        """)
        total_size_by_format = cursor.fetchall()

        # 9️⃣ Total size per language
        cursor.execute("""
            SELECT language, SUM(CAST(REPLACE(size, ' MB', '') AS REAL)) AS total_size
            FROM books
            WHERE size LIKE '% MB' AND language IS NOT NULL
            GROUP BY language
            ORDER BY total_size DESC;
        """)
        total_size_by_language = cursor.fetchall()

        # 🔟 Overall total size (for all books)
        cursor.execute("""
            SELECT SUM(CAST(REPLACE(size, ' MB', '') AS REAL))
            FROM books
            WHERE size LIKE '% MB';
        """)
        total_size_all = cursor.fetchone()[0] or 0.0

        # 📊 Display results
        print("\n📚 **Overall Statistics**")
        print(tabulate([[total_books, downloaded_count, pending_count, failed_count]], 
                       headers=["Total Books", "Downloaded", "Pending", "Failed"], tablefmt="grid"))

        print("\n📜 **Top 5 Titles**")
        print(tabulate(top_titles, headers=["Title", "Count"], tablefmt="grid"))

        print("\n🌎 **Top 5 Languages**")
        print(tabulate(top_languages, headers=["Language", "Count"], tablefmt="grid"))

        print("\n📂 **Top 5 File Formats**")
        print(tabulate(top_extensions, headers=["Extension", "Count"], tablefmt="grid"))

        print("\n📊 **Download Status by Language**")
        print(tabulate(lang_download_stats, headers=["Language", "Downloaded", "Pending", "Failed"], tablefmt="grid"))

        print("\n💾 **Average File Size by Format (MB)**")
        print(tabulate(avg_size_by_format, headers=["Format", "Avg Size (MB)"], tablefmt="grid"))

        print("\n📦 **Total File Size by Format (MB)**")
        print(tabulate(total_size_by_format, headers=["Format", "Total Size (MB)"], tablefmt="grid"))

        print("\n🏋️ **Total File Size by Language (MB)**")
        print(tabulate(total_size_by_language, headers=["Language", "Total Size (MB)"], tablefmt="grid"))

        print(f"\n📊 **Total Size of All Books**: {total_size_all:.2f} MB")

    except sqlite3.Error as e:
        logging.error(f"Error accessing the database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    view_stats()
