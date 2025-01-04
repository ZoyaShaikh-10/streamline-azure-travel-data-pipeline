import os
import pandas as pd
import requests

USD_API = "https://api.exchangerate-api.com/v4/latest/INR"
class AgencyDataProcessor:
    
    def __init__(self, agency_folder_path):
        self.agency_folder_path = agency_folder_path 
        
    def reformat_date(self, date_str):
        """Using a base function rather than using pandas date time"""
        ## As pandas pd datetime is not reflecting much changes to certain values
        # Split the date string into parts
        day, month, year = date_str.split('-')
        # Convert to desired format: mm/dd/yy
        return f"{month}/{day}/{year[2:]}"
 
    def convert_inr_to_usd(self, amount_inr):
        """FUnction to convert INR to USD Currency"""
        try:
            # Fetch exchange rate data
            response = requests.get(USD_API)
            response.raise_for_status()
            exchange_data = response.json()
            
            # Get the USD rate
            rate_usd = exchange_data["rates"]["USD"]
            
            # Convert INR to USD
            amount_usd = amount_inr * rate_usd
            return amount_usd
        except Exception as e:
            print(f"Error fetching exchange rate: {e}")
            return None

    def read_booking_data(self):
        """Function to perform some data manipulations on booking data"""
        booking_data = pd.read_csv(f"{self.agency_folder_path}/booking_data.csv", encoding="utf-8")
        
        # Filter out rows where 'Status' is 'pending'
        filtered_booking_data = booking_data[booking_data['Booking Status'] != 'Pending']
        # Split the 'Names' column into first and last names
        filtered_booking_data['First Name'], filtered_booking_data['Last Name'] = zip(*(name.split(maxsplit=1) for name in filtered_booking_data['Name']))
        ## date validation
        filtered_booking_data['Start Date'] = filtered_booking_data['Start Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_booking_data['Start Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        filtered_booking_data['End Date'] =  filtered_booking_data['End Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_booking_data['End Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        # print(filtered_booking_data)
        return filtered_booking_data
        
    def process_cars_data(self):
        """Function to perform some data manipulations on booking data"""

        cars_data = pd.read_csv(f"{self.agency_folder_path}/cars_data.csv", encoding="utf-8")
        # Filter out rows where 'Status' is 'pending'
        filtered_cars_data = cars_data[cars_data['Rental Status'] != 'Pending']
        ## date validation
        filtered_cars_data['Rental Start Date'] = filtered_cars_data['Rental Start Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_cars_data['Rental Start Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        filtered_cars_data['Rental End Date'] = filtered_cars_data['Rental End Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_cars_data['Rental End Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        # Convert Currency from INR to USD
        filtered_cars_data['Rental Cost (USD)'] = filtered_cars_data['Rental Cost (INR)'].apply(self.convert_inr_to_usd)
        return filtered_cars_data
        
        
    def process_hotel_data(self):
        """Function to perform some data manipulations on booking data"""
        
        hotels_data = pd.read_csv(f"{self.agency_folder_path}/hotels_data.csv", encoding="utf-8")       
        # Filter out rows where 'Status' is 'pending'
        filtered_hotels_data = hotels_data[hotels_data['Booking Status'] != 'Pending']    
        ## date validation
        filtered_hotels_data['Check-in Date'] =  filtered_hotels_data['Check-in Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_hotels_data['Check-in Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        filtered_hotels_data['Check-out Date'] =  filtered_hotels_data['Check-out Date'].apply(self.reformat_date)
        # pd.to_datetime(filtered_hotels_data['Check-out Date'], format='%d-%m-%Y').dt.strftime('%m/%d/%Y')
        # Convert Currency from INR to USD
        filtered_hotels_data['Hotel Cost (USD)'] = filtered_hotels_data['Hotel Cost (INR)'].apply(self.convert_inr_to_usd)
        return filtered_hotels_data
    
    def save_to_csv(self, filtered_data, filename):
        # Directory and file path
        directory = f"{self.agency_folder_path}/filtered_data"
        csv_file_path = os.path.join(directory, f"filtered_{filename}_data.csv")
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
        filtered_data.to_csv(csv_file_path, index=False)
        
        
    def handler(self):
        filtered_booking_data = self.read_booking_data()
        self.save_to_csv(filtered_booking_data, filename='booking')
        filtered_hotel_data = self.process_hotel_data()
        self.save_to_csv(filtered_hotel_data, filename='hotel')
        filtered_cars_data = self.process_cars_data()
        self.save_to_csv(filtered_cars_data, filename='cars')
       
