from SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):

    # Inherit the properties from the superclass, in this case, SpatialEtl
    def __init__(self, remote, local_dir, data_format, destination):
        super().__init__(self, remote, local_dir, data_format, destination)


    def process(self):
        super().extract()
        super().transform()
        super().load()