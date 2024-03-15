import json
import boto3
import random
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # Generate 20 records of mock Airbnb booking data
    booking_data_list = []
    for i in range(20):
        if i < 15:
            booking_data_list.append(generate_mock_booking_data_more_than_one_day())
        else:
            booking_data_list.append(generate_mock_booking_data_one_day())
    
    # Publish the data to the AirbnbBookingQueue
    publish_to_sqs(booking_data_list)
    
    return {
        'statusCode': 200,
        'body': 'Mock Airbnb booking data published successfully to AirbnbBookingQueue'
    }

def generate_mock_booking_data_more_than_one_day():
    # Generate mock data
    booking_id = random.randint(1000, 9999)
    user_id = random.randint(1000, 9999)
    property_id = random.randint(1000, 9999)
    location = "Chennai, India"
    start_date = datetime.now()  # 30 days from now
    end_date = start_date + timedelta(days=7)  # 7 days duration
    price = round(random.uniform(10.0, 500.0), 2)  # Mock price in USD
    duration = (end_date - start_date).days
    
    
    # Construct mock data structure
    mock_booking_data = {
        "bookingId": booking_id,
        "userId": user_id,
        "propertyId": property_id,
        "location": location,
        "startDate": start_date.strftime('%Y-%m-%d'),
        "endDate": end_date.strftime('%Y-%m-%d'),
        "price": price,
        "duration": duration
        
    }
    
    return mock_booking_data

def generate_mock_booking_data_one_day():
    # Generate mock data
    booking_id = random.randint(1000, 9999)
    user_id = random.randint(1000, 9999)
    property_id = random.randint(1000, 9999)
    location = "Chennai, India"
    today = datetime.now()
    start = today 
    end = today
    start_date = today.strftime('%Y-%m-%d')  # Same date as today
    end_date = today.strftime('%Y-%m-%d')  # Same date as today
    price = round(random.uniform(10.0, 500.0), 2)  # Mock price in USD
    duration = (start - end).days
    
    # Construct mock data structure
    mock_booking_data = {
        "bookingId": booking_id,
        "userId": user_id,
        "propertyId": property_id,
        "location": location,
        "startDate": start_date,
        "endDate": end_date,
        "price": price,
        "duration": duration
    }
    
    return mock_booking_data

def publish_to_sqs(data_list):
    # Initialize SQS client
    sqs_client = boto3.client('sqs')
    
    # Publish messages to AirbnbBookingQueue
    queue_url = 'https://sqs.us-east-1.amazonaws.com/455296827336/AirbnbBookingQueue'  # Replace with the URL of your AirbnbBookingQueue
    for data in data_list:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(data)
        )
    
    return response
