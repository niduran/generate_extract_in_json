import os
import time
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import logging

load_dotenv()

def connect_postgres():
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST')
    )
    conn.autocommit = True
    return conn

def generate_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        last_name VARCHAR(100),
        gender VARCHAR(10),
        date_of_birth DATE,
        home_address VARCHAR(255),
        home_city VARCHAR(100),
        postal_code VARCHAR(20),
        country VARCHAR(100),
        iso_country_code VARCHAR(10),
        mobile_phone VARCHAR(20),
        email VARCHAR(100) UNIQUE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS retail_preferences (
        retail_preference_id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        favourite_color VARCHAR(50),
        favourite_category VARCHAR(50),
        favourite_subcategory VARCHAR(50),
        shirt_size VARCHAR(10),
        pants_size VARCHAR(10),
        shoe_size INT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS marketing_preferences (
        marketing_preference_id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        consent BOOLEAN,
        preferred_communication VARCHAR(20)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS loyalty_data (
        loyalty_data_id SERIAL PRIMARY KEY,
        loyalty_number_id BIGINT UNIQUE,
        customer_id INT REFERENCES customers(customer_id),
        date_joined DATE,
        points INT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS system_data (
        system_data_id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        profile_creation_date TIMESTAMP
    );
    """)

    create_watermark_query = """
    CREATE TABLE IF NOT EXISTS watermark (
        last_extracted TIMESTAMP
    );
    """
    cur.execute(create_watermark_query)

    cur.execute("SELECT COUNT(*) FROM watermark;")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO watermark (last_extracted) VALUES (%s);", (datetime.min,))

    conn.commit()
    cur.close()

if __name__ == "__main__":
    conn = connect_postgres()
    generate_tables(conn)

    conn.close()
