import json
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
import os

from parse_yelpdata import get_attributes

# Load environment variables
load_dotenv()

# Database connection parameters
psql_params = {
    "DB_NAME": os.getenv("POSTGRES_PROJECT"),
    "DB_USER": os.getenv("POSTGRES_USER"),
    "DB_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    "DB_HOST": "localhost",
    "DB_PORT": "5432"
}

@contextmanager
def connect_psql(db_params):
    """
    Context manager for PostgreSQL connection with error handling and rollback.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=db_params["DB_NAME"],
            user=db_params["DB_USER"],
            password=db_params["DB_PASSWORD"],
            host=db_params["DB_HOST"],
            port=db_params["DB_PORT"]
        )
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback the transaction on error
        print(f"Database error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def cleanStr4SQL(s):
    """
    Cleans a string for safe SQL insertion by replacing problematic characters.
    """
    return s.replace("'", "`").replace("\n", " ")

def insert_business():
    """
    Inserts business data into the Business table.
    Skips insertion if the business_id already exists.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_business.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                try:
                    cursor.execute("""
                        INSERT INTO Business (business_id, business_name, business_address, city, state, zip_code, latitude, longitude, stars, is_open, tip_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::BOOLEAN, %s)
                        ON CONFLICT (business_id) DO NOTHING;
                    """, (
                        data['business_id'], cleanStr4SQL(data["name"]), cleanStr4SQL(data["address"]),
                        data["city"], data["state"], data["postal_code"], data["latitude"], data["longitude"],
                        data["stars"], 'TRUE' if data["is_open"] == 1 else 'FALSE', 0
                    ))
                except Exception as e:
                    print(f"Insert to Business table failed for business_id {data.get('business_id', 'unknown')}: {e}")
                line = f.readline()

def insert_business_categories():
    """
    Inserts business categories into the BusinessCategory table.
    Ensures all categories exist in the Category table before insertion.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_business.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                business_id = data['business_id']
                categories = data.get("categories", "").split(", ")
                for category in categories:
                    try:
                        # Ensure the category exists in the Category table
                        cursor.execute("""
                            INSERT INTO Category (name)
                            VALUES (%s)
                            ON CONFLICT (name) DO NOTHING;
                        """, (cleanStr4SQL(category),))

                        # Insert into BusinessCategory table
                        cursor.execute("""
                            INSERT INTO BusinessCategory (business_id, category_name)
                            VALUES (%s, %s);
                        """, (business_id, cleanStr4SQL(category)))
                    except Exception as e:
                        # Rollback the transaction and log the error
                        cursor.connection.rollback()
                        print(f"Insert to BusinessCategory table failed for business_id {business_id}: {e}")
                line = f.readline()

def insert_business_attributes():
    """
    Inserts business attributes into the BusinessAttributeValue table.
    Ensures all attributes exist in the Attribute table before insertion.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_business.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                business_id = data['business_id']
                attributes = data.get("attributes", {})
                for attr, value in get_attributes(attributes):
                    try:
                        # Ensure the attribute exists in the Attribute table
                        cursor.execute("""
                            INSERT INTO Attribute (name)
                            VALUES (%s)
                            ON CONFLICT (name) DO NOTHING;
                        """, (cleanStr4SQL(attr),))

                        # Insert into BusinessAttributeValue table
                        cursor.execute("""
                            INSERT INTO BusinessAttributeValue (business_id, attribute_name, attribute_value)
                            VALUES (%s, %s, %s);
                        """, (business_id, cleanStr4SQL(attr), cleanStr4SQL(str(value))))
                    except Exception as e:
                        # Rollback the transaction and log the error
                        cursor.connection.rollback()
                        print(f"Insert to BusinessAttributeValue table failed for business_id {business_id}: {e}")
                line = f.readline()

def insert_hours():
    """
    Inserts business hours into the Hours table.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_business.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                business_id = data['business_id']
                hours = data.get("hours", {})
                for day, time_range in hours.items():
                    open_time, close_time = time_range.split("-")
                    try:
                        cursor.execute("""
                            INSERT INTO Hours (business_id, day_of_week, open_time, close_time)
                            VALUES (%s, %s, %s, %s);
                        """, (business_id, day, open_time, close_time))
                    except Exception as e:
                        print(f"Insert to Hours table failed for business_id {business_id}: {e}")
                line = f.readline()

def insert_users():
    """
    Inserts user data into the YelpUser table.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_user.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                try:
                    cursor.execute("""
                        INSERT INTO YelpUser (user_id, user_name, yelping_since, tip_count, fans, average_stars, funny, useful, cool)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO NOTHING;
                    """, (
                        data['user_id'], cleanStr4SQL(data["name"]), data["yelping_since"],
                        data["tipcount"], data["fans"], data["average_stars"],
                        data["funny"], data["useful"], data["cool"]
                    ))
                except Exception as e:
                    print(f"Insert to YelpUser table failed for user_id {data.get('user_id', 'unknown')}: {e}")
                line = f.readline()

def insert_friends():
    """
    Inserts friendship data into the Friendship table.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_user.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                user_id = data['user_id']
                friends = data.get("friends", "").split(", ")
                for friend_id in friends:
                    try:
                        cursor.execute("""
                            INSERT INTO Friendship (follower_id, followee_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (user_id, friend_id))
                    except Exception as e:
                        print(f"Insert to Friendship table failed for user_id {user_id}: {e}")
                line = f.readline()

def insert_tips():
    """
    Inserts tip data into the Tip table.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_tip.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                try:
                    cursor.execute("""
                        INSERT INTO Tip (user_id, business_id, timestamp, likes, text)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        data['user_id'], data['business_id'], data['date'],
                        data['likes'], cleanStr4SQL(data['text'])
                    ))
                except Exception as e:
                    print(f"Insert to Tip table failed for user_id {data.get('user_id', 'unknown')}: {e}")
                line = f.readline()

def insert_checkins():
    """
    Inserts check-in data into the Checkin table.
    """
    with connect_psql(psql_params) as cursor:
        with open('./yelp_checkin.JSON', 'r') as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                business_id = data['business_id']
                checkins = data.get("date", "").split(", ")
                for timestamp in checkins:
                    try:
                        cursor.execute("""
                            INSERT INTO Checkin (business_id, timestamp)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (business_id, timestamp))
                    except Exception as e:
                        # Rollback the transaction and log the error
                        cursor.connection.rollback()
                        print(f"Insert to Checkin table failed for business_id {business_id}: {e}")
                line = f.readline()

# Call the functions in the correct order
insert_business()
insert_business_categories()
insert_business_attributes()
insert_hours()
insert_users()
insert_friends()
insert_tips()
insert_checkins()