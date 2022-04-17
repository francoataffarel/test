import json
import os
import pandas as pd


class CSVData:
    project = None
    brand = None

    def __init__(self, project, marketplace, **kwargs):
        super().__init__(**kwargs)
        self.path = open(
            os.path.join(os.path.dirname(__file__), "./brand_data/brand_list.csv")
        )
        self.df = pd.read_csv(self.path)
        self.project = project
        self.marketplace = marketplace

    def get_data(self, **kwargs):
        return self.df[
            (self.df["project"] == self.project)
            & (self.df["marketplace"] == self.marketplace)
            & (self.df["brand_type"] == "Internal")
            & (self.df["active"] == True)
        ].values.tolist()

    def close_file(self):
        self.path.close()
