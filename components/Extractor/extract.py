# Data Sources and Formats:
#
# Ad Impressions:
#
# Data Source: AdvertiseX serves digital ads to various online platforms and websites.
# Data Format: Ad impressions data is generated in JSON format, containing information such as ad creative ID, user ID, timestamp, and the website where the ad was displayed.
# Clicks and Conversions:
#
# Data Source: AdvertiseX tracks user interactions with ads, including clicks and conversions (e.g., sign-ups, purchases).
# Data Format: Click and conversion data is logged in CSV format and includes event timestamps, user IDs, ad campaign IDs, and conversion type.
# Bid Requests:
#
# Data Source: AdvertiseX participates in real-time bidding (RTB) auctions to serve ads to users.
# Data Format: Bid request data is received in a semi-structured format, mostly in Avro, and includes user information, auction details, and ad targeting criteria.
# Case Study Requirements:
#
# Data Ingestion:
#
# Implement a scalable data ingestion system capable of collecting and processing ad impressions (JSON), clicks/conversions (CSV), and bid requests (Avro) data.
# Ensure that the ingestion system can handle high data volumes generated in real-time and batch modes.

import json
import csv
import avro.schema
import avro.io
import io
from utils import read_json_file, read_csv_file, read_avro_file

class DataExtractor:
    def __init__(self, ad_impressions_path, clicks_conversions_path, bid_requests_path):
        self.ad_impressions_path = ad_impressions_path
        self.clicks_conversions_path = clicks_conversions_path
        self.bid_requests_path = bid_requests_path

    def extract_ad_impressions(self):
        return read_json_file(self.ad_impressions_path)

    def extract_clicks_conversions(self):
        return read_csv_file(self.clicks_conversions_path)

    def extract_bid_requests(self):
        return read_avro_file(self.bid_requests_path)

if __name__ == "__main__":
    extractor = DataExtractor('path/to/ad_impressions.json', 'path/to/clicks_conversions.csv', 'path/to/bid_requests.avro')
    ad_impressions = extractor.extract_ad_impressions()
    clicks_conversions = extractor.extract_clicks_conversions()
    bid_requests = extractor.extract_bid_requests()
