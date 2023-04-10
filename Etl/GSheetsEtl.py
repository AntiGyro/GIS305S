import requests
import csv
import arcpy
import json
from Etl.SpatialEtl import SpatialEtl

class GSheetsEtl:
    def __init__(self, config_dict):
        self.config_dict = config_dict

    def extract(self):
        print("Extracting addresses from google form spreadsheet...")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
            output_file.write(data)

    def transform(self):
        print("Add City, State")

        with open(fr"{self.config_dict.get('proj_dir')}addresses.csv",
                  "r") as input_file, \
                open(fr"{self.config_dict.get('proj_dir')}output.csv", "w",
                     newline='') as output_file:
            csv_reader = csv.DictReader(input_file)
            fieldnames = csv_reader.fieldnames + ['X', 'Y', 'Type']
            csv_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            csv_writer.writeheader()
            for row in csv_reader:
                address = row["Street Address:"] + " Boulder CO"
                print(address)
                geocode_url = fr"{self.config_dict.get('geocoder_prefix_url')}" + address + fr"{self.config_dict.get('geocoder_suffix_url')}"

                print(geocode_url)
                r = requests.get(geocode_url)
                resp_dict = r.json()
                address_matches = resp_dict['result']['addressMatches']
                if address_matches:
                    x = address_matches[0]['coordinates']['x']
                    y = address_matches[0]['coordinates']['y']
                else:
                    print(f"No coordinates found for address: {address}")
                    continue

                row['X'] = x
                row['Y'] = y
                row['Type'] = 'Residential'
                csv_writer.writerow(row)

    def load(self):

        arcpy.env.workspace = rf"{self.config_dict.get('proj_dir')}WestNileOutbreak.gdb\\"
        arcpy.env.overwriteOutput = True

        in_table = self.config_dict.get('proj_dir') + "output.csv"
        out_feature_class = "avoid_points"
        x_coords = "X"
        y_coords = "Y"

        arcpy.management.Delete(out_feature_class, "FeatureClass")

        arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

        print(arcpy.GetCount_management(out_feature_class))

    def process(self):
        self.extract()
        self.transform()
        self.load()

