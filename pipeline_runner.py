import os
import configparser
from agency_data_processor import AgencyDataProcessor
from upload_to_blob import AzureClient

class PipelineHandler:
    def __init__(self, config_file='pipeline_config.ini'):
        self.config = self._load_config(config_file)
        self.agency_base_folder = self.config['Paths']['AGENCY_BASE_FOLDER']
        self.storage_account_name = self.config['Storage']['storage_account_name']
        self.storage_account_url = self.config['Storage']['storage_account_url']
        self.storage_account_key = self.config['Storage']['storage_account_key']
        self.azure_client = AzureClient(
            storage_account_name=self.storage_account_name,
            storage_account_url=self.storage_account_url,
            storage_account_key=self.storage_account_key
        )

    def _load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def process_agency(self, agency_folder):
        """Cleans data and uploads files for a single agency."""
        print(f"Processing agency: {agency_folder}")
        agency_path = os.path.join(self.agency_base_folder, agency_folder)

        # Check if folder is empty
        if not os.listdir(agency_path):
            print(f"No files to process in {agency_folder}. Skipping.")
            return

        # Clean data
        processor = AgencyDataProcessor(agency_folder_path=agency_path)
        processor.handler()

        # Upload cleaned files
        cleaned_files = [
            f for f in os.listdir(f"{agency_path}/filtered_data")
            if f.startswith('filtered_') and f.endswith('.csv')
        ]
        
        if not cleaned_files:
            print(f"No cleaned files found in {agency_folder}. Skipping upload.")
            return

        for file_name in cleaned_files:
            file_path = os.path.join(f"{agency_path}/filtered_data", file_name)

            # Adjust blob_name logic
            blob_name = '_'.join(file_name.split('_')[1:])  # Remove 'filtered_' prefix
            self.azure_client.create_container(container_name=agency_folder) # Create Container if it doesn't exist
            self.azure_client.upload_file(container_name=agency_folder, blob_name=blob_name, file_path=file_path)

    def process_all_agencies(self):
        """Processes all agencies in the base folder."""
        print(f"Processing all agencies in {self.agency_base_folder}")
        for agency_folder in os.listdir(self.agency_base_folder):
            agency_path = os.path.join(self.agency_base_folder, agency_folder)
            if os.path.isdir(agency_path):
                self.process_agency(agency_folder)

        print("All agencies processed successfully.")

if __name__ == "__main__":
    pipeline_handler = PipelineHandler(config_file='pipeline_config.ini')
    pipeline_handler.process_all_agencies()



