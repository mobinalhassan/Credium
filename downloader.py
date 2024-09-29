import os
import requests
import zipfile
import logging

class Downloader:
    def __init__(self, config):
        self.state_name = config["state_name"]
        self.EPSG = config["EPSG"]
        self.base_url = config["base_url"]
        self.source_ext = config["source_ext"]
        self.data_file_ext = config["data_file_ext"]
        self.download_dir = 'downloaded_files'
        os.makedirs(self.download_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def download_any_file(self, file_name):
        url= f"{self.base_url}{file_name}"

        local_file_path = os.path.join(self.download_dir, f"{file_name}")
        if os.path.exists(local_file_path):
            self.logger.info(f"File name: {local_file_path} already exists.")
            if self.source_ext == 'zip':
                local_file_path = f"{local_file_path.split('.')[0]}.{self.data_file_ext}"
            return local_file_path

        self.logger.info(f"Downloading {url}")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download {url}: {e}")
            return None

        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        self.logger.info(f"Saved {local_file_path}")

        # we can add further type of file handling here
        if self.source_ext == 'zip':
            self.extract_zip_file(local_file_path)
            local_file_path = f"{local_file_path.split('.')[0]}.{self.data_file_ext}"
        # elif self.source_ext in ['tar', 'tar.gz', 'tgz']:
        #     self.extract_tar_file(local_file_path)
        # else:
        #     self.logger.info(f"No extraction needed for {local_file_path}")

        return local_file_path  # Return the path to the downloaded file
    
    def extract_zip_file(self, file_path):
        self.logger.info(f"Extracting {file_path}")
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(self.download_dir)
            self.logger.info(f"Extracted {file_path}")
        except zipfile.BadZipFile as e:
            self.logger.error(f"Failed to extract {file_path}: {e}")
