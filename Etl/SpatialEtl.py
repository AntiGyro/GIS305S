class SpatialEtl:

    # define the initial parameters of the object
    def __init__(self, config_dict):
        self.config_dict = config_dict

    # Add the extract, transform, and load methods
    def extract(self):
        print(f"Extracting data from {self.remote} to {self.local_dir}")

    def transform(self):
        print(f"Transforming {self.data_format}")

    def load(self):
        print(f"Loading data into {self.destination}")