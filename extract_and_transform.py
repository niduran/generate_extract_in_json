import os
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor
from faker import Faker

def connect_postgres():
    load_dotenv()
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST')
    )
    conn.autocommit = True
    return conn

def load_last_extracted(conn):
    cur = conn.cursor()
    cur.execute("SELECT last_extracted FROM watermark LIMIT 1;")
    last_extracted = cur.fetchone()[0]
    cur.close()
    return last_extracted

def update_last_extracted(conn, last_date):
    cur = conn.cursor()
    cur.execute("UPDATE watermark SET last_extracted = %s;", (last_date,))
    conn.commit()
    cur.close()

def transform_to_json(profile, customer_id):
    return {
        "createDate": profile["profile_creation_date"].isoformat() if profile["profile_creation_date"] else None,
        "identification": {
            "customerId": customer_id,
            "email": profile["email"],
            "loyaltyId": profile["loyalty_number_id"],
            "phoneNumber": profile["mobile_phone"]
        },
        "individualCharacteristics": {
            "core": {
                "age": (datetime.now().year - profile["date_of_birth"].year) if profile["date_of_birth"] else None,
                "favouriteCategory": profile["favourite_category"],
                "favouriteSubCategory": profile["favourite_subcategory"]
            },
            "retail": {
                "favoriteColor": profile["favourite_color"],
                "pantsSize": profile["pants_size"],
                "shirtSize": profile["shirt_size"],
                "shoeSize": profile["shoe_size"]
            }
        },
        "userAccount": {
            "ID": customer_id
        },
        "loyalty": {
            "loyaltyID": profile["loyalty_number_id"],
            "joinDate": profile["date_joined"].isoformat() if profile["date_joined"] else None,
            "points": profile["points"]
        },
        "consents": {
            "collect": {
                "val": "y" if profile["consent"] else "n"
            },
            "marketing": {
                "preferred": profile["preferred_communication"]
            }
        },
        "homeAddress": {
            "city": profile["home_city"],
            "country": profile["country"],
            "countryCode": profile["iso_country_code"],
            "street1": profile["home_address"],
            "postalCode": profile["postal_code"]
        },
        "mobilePhone": {
            "number": profile["mobile_phone"]
        },
        "person": {
            "birthDayAndMonth": profile["date_of_birth"].strftime("%m-%d") if profile["date_of_birth"] else None,
            "birthYear": profile["date_of_birth"].year if profile["date_of_birth"] else None,
            "name": {
                "lastName": profile["last_name"],
                "fullName": f"{profile['name']} {profile['last_name']}",
                "firstName": profile["name"]
            },
            "gender": profile["gender"]
        },
        "personalEmail": {
            "address": profile["email"]
        },
        "testProfile": True
    }

def fetch_new_profiles(conn, last_extracted_date):
    cur = conn.cursor(cursor_factory=DictCursor)  
    query = """
    SELECT 
        customers.customer_id, customers.name, customers.last_name, customers.gender, customers.date_of_birth, 
        customers.home_address, customers.home_city, customers.postal_code, customers.country, 
        customers.iso_country_code, customers.mobile_phone, customers.email,
        retail_preferences.favourite_color, retail_preferences.favourite_category, retail_preferences.favourite_subcategory, 
        retail_preferences.shirt_size, retail_preferences.pants_size, retail_preferences.shoe_size,
        marketing_preferences.consent, marketing_preferences.preferred_communication,
        loyalty_data.loyalty_number_id, loyalty_data.date_joined, loyalty_data.points,
        system_data.profile_creation_date
    FROM customers
    INNER JOIN retail_preferences ON customers.customer_id = retail_preferences.customer_id
    INNER JOIN marketing_preferences ON customers.customer_id = marketing_preferences.customer_id
    INNER JOIN loyalty_data ON customers.customer_id = loyalty_data.customer_id
    INNER JOIN system_data ON customers.customer_id = system_data.customer_id
    WHERE system_data.profile_creation_date > %s 
    ORDER BY system_data.profile_creation_date;
    """
    cur.execute(query, (last_extracted_date,))
    new_profiles = cur.fetchall()
    cur.close()
    return new_profiles

def export_profiles(conn):
    last_extracted_date = load_last_extracted(conn)
    all_profiles = []

    while True:
        new_profiles = fetch_new_profiles(conn, last_extracted_date)
        
        if not new_profiles:
            time.sleep(1)
            continue

        for profile in new_profiles:
            customer_id = profile["customer_id"]
            json_profile = transform_to_json(profile, customer_id)
            all_profiles.append(json_profile)

            last_extracted_date = profile["profile_creation_date"]
            update_last_extracted(conn, last_extracted_date)

        save_profiles_to_json(all_profiles)
        all_profiles.clear()

        time.sleep(1)

def save_profiles_to_json(profiles_list, filename="profiles.json"):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)

    
    with open(filepath, 'a') as json_file:
        if os.path.getsize(filepath) > 0: 
            json_file.write(',\n')
        json.dump(profiles_list, json_file, indent=4)

    print(f"Profiles appended to {filepath}")


if __name__ == "__main__":
    conn = connect_postgres()
    try:
        export_profiles(conn)
    finally:
        conn.close()
