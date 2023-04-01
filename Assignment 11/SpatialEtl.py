class SpatialEtl:

    # define the initial parameters of the object
    def __init__(self, remote, local_dir, data_format, destination):
        self.remote = remote
        self.local_dir = local_dir
        self.data_format = data_format
        self.destination = destination

    # Add the extract, transform, and load methods
    def extract(self):
        print(f"Extracting data from {self.remote} to {self.local_dir}")

    def transform(self):
        print(f"Transforming {self.data_format}")

    def load(self):
        print(f"Loading data into {self.destination}")