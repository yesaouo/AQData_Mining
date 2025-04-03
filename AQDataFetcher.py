import requests
import csv
import time
from datetime import datetime, timedelta
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AQDataFetcher:
    BASE_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_488"
    NUM_STATIONS = 87  # Number of monitoring stations
    API_LIMIT = 1000   # Max records per API request

    def __init__(self, api_key):
        if not api_key:
            raise ValueError("API key is required.")
        self.api_key = api_key
        self.headers = {"accept": "*/*"}
        self._column_headers = None # To store fetched headers

    def _make_api_request(self, params):
        try:
            response = requests.get(self.BASE_URL, headers=self.headers, params=params, timeout=30) # Added timeout
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.Timeout:
            logging.error(f"Request timed out with params: {params}")
            return None
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err} - URL: {response.url}")
            logging.error(f"Response text: {response.text}")
            return None
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request exception occurred: {req_err}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred during API request: {e}")
            return None

    def _get_headers_and_latest_time(self):
        params = {
            "api_key": self.api_key,
            "limit": 1,
            "offset": 0
        }
        logging.info("Fetching latest record to determine headers and start time...")
        data = self._make_api_request(params)

        if data and data.get("records") and data.get("fields"):
            try:
                # Extract headers (field IDs)
                headers = [field['id'] for field in data['fields']]
                self._column_headers = headers # Store headers
                logging.info(f"Successfully fetched headers: {headers}")

                # Extract and parse latest timestamp
                latest_record = data["records"][0]
                latest_time_str = latest_record.get("datacreationdate")
                if not latest_time_str:
                    logging.error("Could not find 'datacreationdate' in the latest record.")
                    return None, None

                latest_dt = datetime.strptime(latest_time_str, "%Y-%m-%d %H:%M")
                logging.info(f"Latest data timestamp found: {latest_dt.strftime('%Y-%m-%d %H:%M')}")
                return headers, latest_dt
            except (ValueError, KeyError, IndexError) as e:
                logging.error(f"Error processing latest record data: {e}")
                logging.error(f"Received data structure: {data}")
                return None, None
        else:
            logging.error("Failed to fetch or parse the initial API response.")
            if data:
                 logging.error(f"Received data: {data}") # Log data if received but invalid structure
            return None, None

    def _calculate_start_offset(self, latest_dt):
        # Calculate how many hours have passed in the day of the latest record
        hours_passed_today = latest_dt.hour + 1
        logging.info(f"Hours passed in the latest day ({latest_dt.date()}): {hours_passed_today} (up to {latest_dt.hour}:00)")

        # Calculate offset to rewind past these hours for all stations
        offset = hours_passed_today * self.NUM_STATIONS
        logging.info(f"Calculated starting offset to reach end of previous day (23:00): {offset}")
        return offset

    def fetch_and_save(self, days_to_fetch, output_dir="."):
        if days_to_fetch <= 0:
            logging.error("days_to_fetch must be a positive integer.")
            return

        headers, latest_dt = self._get_headers_and_latest_time()
        if not headers or not latest_dt:
            logging.error("Could not retrieve initial data or headers. Aborting fetch.")
            return

        start_offset = self._calculate_start_offset(latest_dt)
        records_per_day = self.NUM_STATIONS * 24
        total_records_to_fetch = records_per_day * days_to_fetch

        # Determine date range for filename
        end_date = (latest_dt - timedelta(days=1)).date() # End date is the day before the latest record's day
        start_date = (end_date - timedelta(days=days_to_fetch - 1)) # Start date is 'days_to_fetch' days before the end date
        output_filename = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        output_path = os.path.join(output_dir, output_filename)

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        logging.info(f"Starting data fetch for {days_to_fetch} days ({start_date} to {end_date}).")
        logging.info(f"Total records to fetch: {total_records_to_fetch}")
        logging.info(f"Saving data to: {output_path}")

        records_fetched_count = 0
        current_offset = start_offset

        try:
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile: # utf-8-sig for Excel compatibility
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader() # Write the header row

                while records_fetched_count < total_records_to_fetch:
                    # Determine how many records to fetch in this batch
                    remaining_records = total_records_to_fetch - records_fetched_count
                    limit_for_this_request = min(self.API_LIMIT, remaining_records)

                    if limit_for_this_request <= 0:
                        break # Should not happen with correct logic, but safe check

                    params = {
                        "api_key": self.api_key,
                        "limit": limit_for_this_request,
                        "offset": current_offset
                    }

                    logging.info(f"Fetching batch: offset={current_offset}, limit={limit_for_this_request}")
                    data = self._make_api_request(params)

                    if data and data.get("records"):
                        records_batch = data["records"]
                        if not records_batch:
                            logging.warning(f"API returned no more records at offset {current_offset}. Expected more data.")
                            break # Stop if API returns empty list unexpectedly

                        processed_batch = []
                        for record in records_batch:
                            processed_record = {header: record.get(header, '') for header in headers}
                            processed_batch.append(processed_record)

                        writer.writerows(processed_batch)
                        num_in_batch = len(records_batch)
                        records_fetched_count += num_in_batch
                        current_offset += num_in_batch # Crucially, offset increments by records *received*

                        logging.info(f"Fetched {num_in_batch} records. Total fetched: {records_fetched_count}/{total_records_to_fetch}")

                        # Optional: Add a small delay to be polite to the API server
                        time.sleep(0.5)

                    elif data is None: # Handle case where _make_api_request returned None due to error
                         logging.error("API request failed for this batch. Trying to continue might lead to data gaps.")
                         # Decide whether to break or retry (retry logic not implemented here)
                         logging.warning("Continuing fetch process, but there might be data gaps.")
                         # If we continue, we need to increment offset based on requested limit to avoid getting stuck
                         current_offset += limit_for_this_request
                         time.sleep(2) # Longer sleep after an error
                    else:
                        # Response received but "records" key is missing or empty unexpectedly
                        logging.warning(f"API response structure might be invalid or empty at offset {current_offset}. Response: {data}")
                        break

            logging.info(f"Successfully fetched {records_fetched_count} records.")
            if records_fetched_count < total_records_to_fetch:
                logging.warning(f"Expected {total_records_to_fetch} records, but only fetched {records_fetched_count}. Data might be incomplete.")
            logging.info(f"Data saved to {output_path}")

        except IOError as e:
            logging.error(f"Could not write to CSV file {output_path}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred during CSV writing or fetch loop: {e}", exc_info=True) # Log traceback

# --- Example Usage ---
if __name__ == "__main__":
    API_KEY = "fd64c486-c224-43d1-835f-8ce005dfb93c" # Replace with your key

    if API_KEY == "YOUR_API_KEY_HERE" or not API_KEY:
        print("Please replace 'YOUR_API_KEY_HERE' with your actual API key.")
    else:
        fetcher = AQDataFetcher(api_key=API_KEY)

        # Parameters for fetching data
        days_to_get = 7       # How many past full days of data to retrieve
        output_directory = "air_quality_data" # Where to save the CSV

        # Run the fetch process
        fetcher.fetch_and_save(days_to_fetch=days_to_get, output_dir=output_directory)

        print("\n--- Fetch complete ---")