import argparse
import logging
import json
import os
from pipeline import Pipeline

def setup_logging():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return logger

def load_config():
    """
    Load configuration settings from a JSON file.

    Returns:
        dict: The configuration settings loaded from 'config.json'.
    """
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

def load_subscription_key():
    """
    Load the subscription key from a secret file.
    To create your on secret.txt file follow this format:
    Subscription-Key=YourSubscriptionKeyHere
    Note: Donot add any space or empty line

    Returns:
        str: The subscription key extracted from 'secret.txt'.
    """
    with open('secret.txt', 'r') as file:
        content =file.read()
        subscription_key=str(content).split('=')[1]
    return subscription_key

def main(city, street, house_number, zip_code):
    """
    Main function to process an address and run the pipeline.

    Args:
        city (str): The city of the address.
        street (str): The street of the address.
        house_number (str): The house number of the address.
        zip_code (str): The ZIP code of the address.
    """
    logger = setup_logging()
    config = load_config()
    subscription_key = load_subscription_key()

    address = {
        'city': city,
        'street': street,
        'house_number': house_number,
        'zip_code': zip_code
    }

    logger.info(f"Processing address: {address['street']} {address['house_number']}, {address['zip_code']} {address['city']}")

    # Run the pipeline
    pipeline=Pipeline(address, subscription_key, config)
    pipeline.run_pipeline()


if __name__ == "__main__":
    city = "Augsburg"
    street = "Katharinengasse"
    house_number = "13"
    zip_code = "86150"

    # city = "Munich"
    # street = "Engadiner Str"
    # house_number = "32"
    # zip_code = "81475"

    # city = "Munich"
    # street = "Sperlstrasse"
    # house_number = "14"
    # zip_code = "81476"

    # city = "Marsberg"
    # street = "Bahnhofstrasse"
    # house_number = "9"
    # zip_code = "34431"

    # city = "Ludwigsfelde"
    # street = "Kiefernweg"
    # house_number = "19"
    # zip_code = "14974"

    # city = "Ludwigsfelde"
    # street = "Kiefernweg"
    # house_number = "17"
    # zip_code = "14974"

    # city = "Berlin"
    # street = "Chausseestrasse"
    # house_number = "109"
    # zip_code = "10115"
    main(city, street, house_number, zip_code)