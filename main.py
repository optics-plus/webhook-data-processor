import os
import sqlite3
import json
import boto3
import snowflake.connector
from dotenv import load_dotenv
from flask import Flask, request, jsonify, abort
from typing import Dict

load_dotenv()

app = Flask(__name__)


# Basic Configuration
DATABASE_NAME       = 'webhook_data.db'
AWS_REGION          = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_SECRET          = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_CLIENT_ID       = os.getenv("AWS_ACCESS_KEY_ID", "")
KINESIS_STREAM_NAME = 'location-stream'
S3_BUCKET_NAME      = os.getenv("AWS_BUCKET_NAME", "milk-moovement-test-data")
SNOWFLAKE_CONFIGURATION = {
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASS", ""),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "database": os.getenv("SNOWFLAKE_DB", ""),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", ""),
    "warehouse": os.getenv("SNOWFLAKE_WH", "")
}

_S3_CLIENT       = None # store S3 Client Intance
_KINESIS_CLIENT  = None # store Kinesis Client Intance

def init_db() -> None:
    """
    Initiation script for the SQLite database. Currently just 
    creates a table with a single text field but feel free to modify
    this as required for your solution below.
    """
    with sqlite3.connect(DATABASE_NAME) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS webhook_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT
            )
        """)

        # Set structure location table
        c.execute("""
            CREATE TABLE IF NOT EXISTS location_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                trip_id TEXT,
                timestamp TEXT,
                latitude REAL,
                longitude REAL,
                event_type TEXT
            )
        """)

        conn.commit()

def get_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_CLIENT_ID,
            aws_secret_access_key=AWS_SECRET,
        )
    return _S3_CLIENT

def get_kinesis_client():
    global _KINESIS_CLIENT
    if _KINESIS_CLIENT is None:
        _KINESIS_CLIENT = boto3.client(
            "kinesis",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_CLIENT_ID,
            aws_secret_access_key=AWS_SECRET,
        )
    return _KINESIS_CLIENT


@app.route('/webhook-endpoint', methods=['POST'])
def webhook_listener():
    try:
        data = request.json
        handle_webhook(data)

        return jsonify({"message": "Data received successfully!"}), 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": "Failed to decode data."}), 400


def handle_webhook(data: Dict[str, any]) -> None:
    """
    Process the data as required. In this case, we're just printing it.

    Instructions:
    Write a function that handles the incoming webhooks and places them into
    a SQLite database. Treat this database like you would a RAW layer in a 
    warehouse like Snowflake/Redshift.
    """
    try:

        # Capture raw data for debugging
        store_raw_event(data)

        location = data.get("location", {})
        trip = data.get("trip", {})

        if not location or "coordinates" not in location:
            abort(400, description="Invalid location data")

        structured = {
            "user_id": data.get("MMUserId"),
            "trip_id": trip.get("_id"),
            "timestamp": data.get("created_at"),
            "latitude": float(location.get("coordinates",{}).get("latitude","")),
            "longitude": float(location.get("coordinates",{}).get("longitude","")),
            "event_type": data.get("type"),
        }

        """
            # Other data extract could be:
            {
                "location": {
                    "user_id": data.get("MMUserId"),
                    "latitude": float(location.get("coordinates",{}).get("latitude","")),
                    "longitude": float(location.get("coordinates",{}).get("longitude","")),
                    "created_at": data.get("created_at"),
                    "event_type": data.get("type"),
                },
                "trip": {
                    "trip_id": trip.get("_id"]),
                    "external_id": trip.get("externalId"),
                    "user_id": trip.get("MMUserId"),
                    "created_at": trip.get("createdAt"),
                    "updated_at": trip.get("updatedAt"),
                    "started_at": trip.get("startedAt"),
                    "route_session_type": trip.get("metadata",{}).get("route_session_type","),
                },
                "user": {
                    "user_id": data.get("MMUserId"),
                    "event_id": data.get("id"),
                    "created_at": data.get("created_at"),
                    "live": data.get("live").upper() == "TRUE",
                }
            }
        """

        # Store essential data
        store_location(structured)          # store to SQLite and
        push_to_s3(structured)              # push to S3 Bucket or
        # store_into_snowflake(structured)  # store into Snowflake

        # For geofenced event data
        if structured["event_type"] in ["user.entered_geofence", "user.exited_geofence"]:
            print(f"Geofence event pushed to : {structured}")
            push_to_stream(structured)

        print("Received webhook data:", data)
    
    except Exception as e:
        print(f"Error procesing webhook: {e}")
        abort(500, description="rocessing error")


def store_raw_event(event: Dict[str, any]) -> None:
    # Store raw event in tabele for log purpose...
    try:
        with sqlite3.connect(DATABASE_NAME) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO webhook_data (payload) VALUES (?)", (json.dumps(event),))
            conn.commit()
    except Exception as e:
        print(f"Error storing raw event to SQLite: {e}")


def store_location(location: Dict[str, any]) -> None:
    # Store structured location data in  SQLite DB
    try:
        with sqlite3.connect(DATABASE_NAME) as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO location_data (user_id, trip_id, timestamp, latitude, longitude, event_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """,(
                location["user_id"],
                location["trip_id"],
                location["timestamp"],
                location["latitude"],
                location["longitude"],
                location["event_type"]
            ))
            conn.commit()
    except Exception as e:
        print(f"Error storing structured location data to SQLite: {e}")


def push_to_s3(obj: Dict[str, any]) -> None:
    # Store data object to S3 Bucket
    try:
        object_key = f"locations/{obj["user_id"]}/{obj["trip_id"]}/{obj["timestamp"]}.json"
        s3 = get_s3_client()
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_key,
            Body=json.dumps(obj)
        )
    except Exception as e:
        print(f"Error pushing data object to s3 bucket: {e}")


def store_into_snowflake(location: Dict[str, any]) -> None:
    # Store data into Snowflake
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGURATION)
        cursor = conn.cursor()

        query = """
            INSERT INTO location_data (user_id, trip_id, timestamp, latitude, longitude, event_type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query, (location["user"], location["trip_id"], location["timestamp"], location["latitude"], location["longitude"], location["event_type"]))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting into Snowflake: {e}")

def push_to_stream(obj: Dict[str, any]) -> None:
    try:
        kinesis = get_kinesis_client()
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(obj),
            PartitionKey=obj["user_id"]
        )
    except Exception as e:
        print(f"Error pushing data object to Kinesis stream: {e}")



if __name__ == "__main__":
    init_db()
    app.run(port=65530)
