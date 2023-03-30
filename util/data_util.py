"""
Author: atramirez

This script will take all the csvs in scraped data bring into one dataframe and offer a few filterings for other utils
"""
# Python Imports
import pathlib
from os import listdir
from os.path import isfile, join

# External imports
import pandas as pd

class DataContainer():
    """
    Class containing and managing all scraped CSV files as Pandas Dataframes
    """
    def __init__(self) -> None:
        # Private data
        self.mono_df = {}
        self.csv_df_dict: dict = {}

        # Initialization functions
        self._read_csvs()
        self._create_mono_df()

    def _read_csvs(self) -> None:
        """
        **Private: Do not Use**
        Reads in the CSVs to the class
        """
        csv_path = self.get_csvs_path()
        csv_files = [f for f in listdir(csv_path) if isfile(join(csv_path, f))]
        
        for csv in csv_files:
            this_csv_path = join(csv_path, csv)
            self.csv_df_dict[csv] = pd.read_csv(this_csv_path)

    def _create_mono_df(self) -> None:
        """
        **Private: Do not Use**
        Creates one monolithic dataframe from all scraped CSVs
        """
        # for i in self.csv_df_dict.values():
        #     pd.concat(i)
        self.mono_df= pd.concat(self.csv_df_dict.values())

    def get_csvs_path(self) -> pathlib.Path:
        """
        This gets the path where all scraped CSVs are stored
        """
        csv_path = pathlib.Path(__file__).resolve()
        csv_path = csv_path.parent
        csv_path = csv_path.parent
        csv_path = pathlib.Path(csv_path, "data", "init_scrape")
        return csv_path

    def get_monolith_df(self):
        """
        Returns: Single Pandas Dataframe containing all scraped data
        """
        return self.mono_df

    def get_single_df(self, filename:str):
        """
        Params
            filename: csv file name to get as a df
        Returns: Pandas Dataframe of requested CSV
        """
        return self.csv_df_dict[filename]

    def get_all_geodata_users(self):
        """
        Returns: Pandas Dataframe masked for only users with GeoJSON data
        """
        #TODO ATR make this effcient if it gets very big
        return self.mono_df[self.mono_df['GeoJSON'].notna()]
    

class DataAnonymizer():
    """
    atramirez initial data anonymization method
    """
    def __init__(self):
        pass

    def anonymize_tweets_df(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Params
            df - fataframe to anonymize
        Returns: anonymized dataframe
        """
        print(df)
        df["UID"] = df.groupby("User").ngroup()
        df = df.drop(columns=['User', 'Tweet'], axis=1)
        df = df.sample(frac=1).reset_index(drop=True)
    

        return df

if __name__ == "__main__":
    #! This is for testing and not meant to be used!
    print(len(DataContainer().get_all_geodata_users()))
