import os
import random
import time
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from faker import Faker
import logging

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

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

def generate_customer_profile():
    fake = Faker()

    profile = {
        "name": fake.first_name(),
        "last_name": fake.last_name(),
        "gender": random.choice(["male", "female", "undefined"]),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=85),
        "home_address": fake.street_address(),
        "home_city": fake.city(),
        "postal_code": fake.zipcode(),
        "country": fake.country(),
        "iso_country_code": fake.country_code(),
        "mobile_phone": fake.phone_number()[:20],
        "email": fake.unique.email(),
        "favourite_color": random.choice(["Red", "Blue", "Green", "Black", "White", "Rose"]),
        "favourite_category": random.choice(["Mens", "Womens", "Kids"]),
        "favourite_subcategory": random.choice(["Shirts", "Pants", "Shoes", "Accessories"]),
        "shirt_size": random.choice(["S", "M", "L", "XL"]),
        "pants_size": random.choice(["XS", "S", "M", "L"]),
        "shoe_size": random.randint(19, 48),
        "consent": random.choice([True, False]),
        "preferred_communication": random.choice(["email", "push", "sms"]),
        "loyalty_number_id": fake.unique.random_number(digits=10),
        "date_joined": fake.date_this_decade(),
        "points": random.randint(0, 1000),
        "profile_creation_date": datetime.now()
    }
    return profile

def insert_customer_profile(conn, profile):
    cur = conn.cursor()
    
    customer_query = """
    INSERT INTO customers (
        name, last_name, gender, date_of_birth, home_address, home_city, postal_code,
        country, iso_country_code, mobile_phone, email
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id;
    """
    cur.execute(customer_query, (
        profile["name"], profile["last_name"], profile["gender"], profile["date_of_birth"],
        profile["home_address"], profile["home_city"], profile["postal_code"],
        profile["country"], profile["iso_country_code"], profile["mobile_phone"], profile["email"]
    ))
    customer_id = cur.fetchone()[0]

    retail_query = """
    INSERT INTO retail_preferences (
        customer_id, favourite_color, favourite_category, favourite_subcategory,
        shirt_size, pants_size, shoe_size
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cur.execute(retail_query, (
        customer_id, profile["favourite_color"], profile["favourite_category"],
        profile["favourite_subcategory"], profile["shirt_size"], profile["pants_size"], profile["shoe_size"]
    ))

    marketing_query = """
    INSERT INTO marketing_preferences (
        customer_id, consent, preferred_communication
    )
    VALUES (%s, %s, %s);
    """
    cur.execute(marketing_query, (
        customer_id, profile["consent"], profile["preferred_communication"]
    ))

    loyalty_query = """
    INSERT INTO loyalty_data (
        loyalty_number_id, customer_id, date_joined, points
    )
    VALUES (%s, %s, %s, %s);
    """
    cur.execute(loyalty_query, (
        profile["loyalty_number_id"], customer_id, profile["date_joined"], profile["points"]
    ))

    system_query = """
    INSERT INTO system_data (
        customer_id, profile_creation_date
    )
    VALUES (%s, %s);
    """
    cur.execute(system_query, (
        customer_id, profile["profile_creation_date"]
    ))
    logging.info(f"Generated and inserted profile: {profile['name']} {profile['last_name']}")
    cur.close()

def generate_and_insert_profiles(conn, num_profiles=10):
    for _ in range(num_profiles):
        profile = generate_customer_profile()
        insert_customer_profile(conn, profile)
        
        delay = random.uniform(1, 5)
        time.sleep(delay)

if __name__ == "__main__":
    conn = connect_postgres()
    generate_and_insert_profiles(conn, 5)

    conn.close()
