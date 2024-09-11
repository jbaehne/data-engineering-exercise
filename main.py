print('Hello')
import requests
import logging
import io
import boto3
import os
from sqlalchemy import create_engine, Column, String, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Open Library Subject API
subject = "science_fiction"
url = f"https://openlibrary.org/subjects/{subject}.json"
headers = {
    "User-Agent": "BookPipeline/1.0 (your_email@gmail.com)"
}

# Environment variables for database
load_dotenv()
DB_URL = os.environ['DB_URL']
SSL_CERT_PATH = '&sslrootcert=./root.crt'

# Set up database connection
engine = create_engine(f'{DB_URL}{SSL_CERT_PATH}')
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


def save_to_db(books, authors):
    logger.info("Inserting data into CockroachDB...")

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Insert Authors into the database
        for author_id, author_name in authors:
            session.merge(Author(author_id=author_id, author_name=author_name))

        # Insert Books into the database
        for book_id, book_title, author_id, author_name in books:
            session.merge(Book(book_id=book_id, book_title=book_title, author_id=author_id))

        session.commit()
        logger.info("Data has been inserted into the database.")
    except Exception as e:
        logger.error(f"Error inserting data into the database: {e}")
    finally:
        session.close()


def average_books_per_author():
    logger.info("Calculating the average number of books written by each author...")

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        subquery = session.query(Book.author_id, func.count(Book.book_id).label('book_count')).group_by(
            Book.author_id).subquery()
        result = session.query(func.avg(subquery.c.book_count)).scalar()
        logger.info(f"Average number of books written by an author: {result}")
        return result
    except Exception as e:
        logger.error(f"Error calculating average books per author: {e}")
        return None
    finally:
        session.close()

def create_tables_if_not_exists():
    logger.info("Checking and creating tables if they do not exist...")
    try:
        # Create all tables in the database which are defined in the Base metadata
        Base.metadata.create_all(engine)
        logger.info("Tables checked and created if necessary.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise



def lambda_handler(event, context):
    # Ensure tables exist
    create_tables_if_not_exists()

    data = fetch_books_by_subject(url)
    books, authors = extract_books_and_authors(data)

    if books and authors:
        save_to_db(books, authors)

    average_books_per_author()

    return {
        'statusCode': 200,
        'body': 'Pipeline executed successfully'
    }


if __name__ == "__main__":
    lambda_handler(event=None,context=None)