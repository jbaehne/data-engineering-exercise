import requests
import csv
import logging
import os
from sqlalchemy import create_engine, Column, String, ForeignKey, func
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

subject = "science_fiction"
url = f"https://openlibrary.org/subjects/{subject}.json"
headers = {
    "User-Agent": "BookPipeline/1.0 (your_email@gmail.com)"
}

DB_FILE = "books_authors.db"
engine = create_engine(f'sqlite:///{DB_FILE}')
Base = declarative_base()

class Author(Base):
    __tablename__ = 'authors'
    author_id = Column(String, primary_key=True)
    author_name = Column(String)

class Book(Base):
    __tablename__ = 'books'
    book_id = Column(String, primary_key=True)
    book_title = Column(String)
    author_id = Column(String, ForeignKey('authors.author_id'))
    author = relationship("Author")

def fetch_books_by_subject(subject_url):
    logger.info("Fetching data from Open Library API...")
    try:
        response = requests.get(subject_url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        return None

def extract_books_and_authors(data):
    if not data or 'works' not in data:
        logger.error("Invalid data format.")
        return [], []

    books = []
    authors = set()

    for work in data['works']:
        book_title = work.get('title', 'Unknown Title')
        book_id = work.get('key', 'Unknown ID').split('/')[-1]

        for author in work.get('authors', []):
            author_name = author.get('name', 'Unknown Author')
            author_id = author.get('key', 'Unknown ID').split('/')[-1]
            authors.add((author_id, author_name))
            books.append((book_id, book_title, author_id, author_name))

    return books, list(authors)

def save_to_csv(books, authors):
    logger.info("Saving books and authors to CSV files...")

    try:
        with open('books.csv', mode='w', newline='', encoding='utf-8') as book_file:
            book_writer = csv.writer(book_file)
            book_writer.writerow(['Book ID', 'Book Title', 'Author ID', 'Author Name'])
            book_writer.writerows(books)

        with open('authors.csv', mode='w', newline='', encoding='utf-8') as author_file:
            author_writer = csv.writer(author_file)
            author_writer.writerow(['Author ID', 'Author Name'])
            author_writer.writerows(authors)

        logger.info("CSV files have been created.")
    except IOError as e:
        logger.error(f"Error saving CSV files: {e}")

def create_database():
    logger.info("Creating SQLite database and tables if they don't exist...")
    Base.metadata.create_all(engine)
    logger.info("Database and tables created.")

def csv_to_sqlite():
    logger.info("Inserting data from CSV files into SQLite database...")

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        with open('authors.csv', mode='r', encoding='utf-8') as author_file:
            author_reader = csv.reader(author_file)
            next(author_reader)
            for row in author_reader:
                session.merge(Author(author_id=row[0], author_name=row[1]))

        with open('books.csv', mode='r', encoding='utf-8') as book_file:
            book_reader = csv.reader(book_file)
            next(book_reader)
            for row in book_reader:
                session.merge(Book(book_id=row[0], book_title=row[1], author_id=row[2]))

        session.commit()
        logger.info("Data has been inserted into the database.")
    except IOError as e:
        logger.error(f"Error inserting data into the database: {e}")
    finally:
        session.close()

def average_books_per_author():
    logger.info("Calculating the average number of books written by each author...")

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        subquery = session.query(Book.author_id, func.count(Book.book_id).label('book_count')).group_by(Book.author_id).subquery()
        # SQL: SELECT author_id, COUNT(book_id) AS book_count FROM books GROUP BY author_id
        result = session.query(func.avg(subquery.c.book_count)).scalar()
        # SQL: SELECT AVG(book_count) FROM (SELECT author_id, COUNT(book_id) AS book_count FROM books GROUP BY author_id)
        logger.info(f"Average number of books written by an author: {result}")
        return result
    except Exception as e:
        logger.error(f"Error calculating average books per author: {e}")
        return None
    finally:
        session.close()

def run_pipeline():
    data = fetch_books_by_subject(url)
    books, authors = extract_books_and_authors(data)
    save_to_csv(books, authors)
    create_database()
    csv_to_sqlite()
    average_books_per_author()

if __name__ == "__main__":
    run_pipeline()
