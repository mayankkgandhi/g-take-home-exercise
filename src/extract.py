import os
import ssl
import time
import pandas as pd
from io import BytesIO, StringIO
from urllib.request import urlopen
from urllib.error import URLError
from zipfile import ZipFile


class CSV_Extract:
    ENCODING = "windows-1252"

    def __init__(self, config):
        self.config = config
        if config.url_path is None:
            raise ValueError("URL path is missing")
        self.path = config.url_path
        self.s3bucket = config.destination_path
        self.unzip = False if config.unzip is None else config.unzip

    def _extract(self):
        """
        This method downloads the zip file from the URL and Unzips the file
        and stores under 's3_bucket/extract' path. (Mimicking s3 bucket here)
        """
        try:
            print("Start download from: " + self.path)
            response = urlopen(self.path)
            zipfile = ZipFile(BytesIO(response.read()))
            file = zipfile.namelist()[0]
            data = [str(row, self.ENCODING)
                    for row in zipfile.open(file, 'r').readlines()]
            print("Downloaded the data")
            self.s3_bucket(data)
        except URLError as url_error:
            print("Error reading from the url: " + self.path)
            raise url_error
        except Exception as e:
            print("Exception reading from the url: " + self.path)
            raise e

    def s3_bucket(self, data):
        backup = False
        s3_bucket = 's3_bucket/'
        filepath = s3_bucket + self.s3bucket
        if os.path.isfile(filepath):
            backup = True
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            if backup:
                backup_path = os.path.dirname(filepath) + "/files/" + time.strftime("%Y%m%d-%H%M%S") + "-" + \
                    filepath.split("/")[-1]
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                os.rename(filepath, backup_path)
            with open(filepath, "w") as f:
                f.writelines(data)
            print("Data written to file: " + filepath)
        except Exception as e:
            print("Exception writing data to : " + filepath)
            raise e
