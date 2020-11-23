import pandas as pd
from abc import abstractmethod


class Loader:

    def _before_load(self): pass

    @abstractmethod
    def _load(self):
        raise NotImplementedError

    def _after_load(self): pass

    def load(self):
        try:
            self._before_load()
            self._load()
            self._after_load()
        except Exception as e:
            print("Exception loading data")
            raise e


class Csv_Load:

    def __init__(self, config):
        self.backup = True if config.backup is None else config.backup
        self.updated_source = (config.transformed_source)
        self.current_source = (config.current_source_path)
        self.destination = (config.destination_path)

        if config.primary_keys is None:
            self.primary_keys = ["id"]
        else:
            self.primary_keys = config.primary_keys

        self.new_df = pd.DataFrame()

    def _load(self):
        print("Loading files")
        """
        Check duplicates by address, county, sales
        """
        current_df = pd.read_csv(self.current_source,
                                 encoding='unicode_escape')
        print("current data: " + str(current_df.shape[0]))

        upsert_df = pd.read_csv(self.updated_source,
                                encoding='unicode_escape')
        unique_transform = self.unique(upsert_df)
        print("transform data: " + str(unique_transform.shape[0]))

        max_id = current_df['id'].max()
        new_rows = self.difference(unique_transform, current_df)
        print("new rows: " + str(new_rows.shape[0]))
        # adding ID to new rows and incrementing after the max id in table
        new_rows["id"] = list(
            range(int(max_id+1), int(max_id + 1 + new_rows.shape[0])))

        outdated_rows = self.difference(current_df, unique_transform)
        print("outdated rows: " + str(outdated_rows.shape[0]))

        self.new_df = self.difference(current_df, outdated_rows)
        self.new_df = self.new_df.append(new_rows)
        self.new_df = self.new_df.sort_values(by=["id"])

        # load_data and writing to file.
        print("Loading the data:: " + str(self.new_df.shape[0]))

        self.new_df.to_csv(self.destination)
        print("loaded new data!")

    def unique(self, df):
        """
        This method returns unique rows from dataframe
        :return: unique rows
        """
        new_df = df.drop_duplicates(self.primary_keys)
        return new_df

    def difference(self, df1, df2):
        """
        This method performs set operation of 2 dataframs
        :param df1: first pandas dataframe
        :param df2: second pandas dataframe
        :return: df1-df2 dataframe
        """
        return pd.concat([df1, df2, df2]).drop_duplicates(self.primary_keys, keep=False)
