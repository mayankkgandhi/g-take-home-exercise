from extract import CSV_Extract
from load import Csv_Load
from transform import Csv_Transform


class Pipeline:
    def __init__(self, extract_config, transform_config, load_config):
        self.extract = extract_config
        self.transform = transform_config
        self.load = load_config

    def run(self):
        """
        Run respective ETL pipleine
        :return: None
        """
        CSV_Extract(self.extract)._extract()
        Csv_Transform(self.transform)._transform()
        Csv_Load(self.load)._load()
