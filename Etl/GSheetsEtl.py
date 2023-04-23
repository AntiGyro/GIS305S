"""
This module contains the GSheetsEtl class, which inherits from the SpatialEtl superclass and provides a specific
implementation for working with Google Sheets data. The GSheetsEtl class is designed to extract address data from a
Google Sheets form, transform it by adding city, state, and geocoded coordinates, and load the transformed data into
an ArcGIS feature class. This class can be used to process spatial data from Google Sheets and integrate it with other
GIS data sources.

"""


import requests
import csv
import arcpy
import logging
import json
from Etl.SpatialEtl import SpatialEtl

class GSheetsEtl(SpatialEtl):
    """
    A class to represent the ETL (Extract, Transform, Load) process for Google Sheets data.
    """

    def __init__(self, config_dict):
        """
        Initializes the object with a configuration dictionary.
        :param config_dict: A dictionary containing configuration settings
        :return: None
        """
        self.config_dict = config_dict

    def extract(self):
        """
        Extracts addresses from a Google Sheets form and writes them to a CSV file.
        :param: None
        :return: None
        """
        logging.debug("Entering extract function")

        try:
            logging.debug("Extracting addresses from google form spreadsheet")
            r = requests.get(self.config_dict.get('remote_url'))
            r.encoding = "utf-8"
            data = r.text
            with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
                output_file.write(data)
        except Exception as e:
            print(f"Error in the GSheets extract function{e}")

        logging.debug("Exiting extract function")

    def transform(self):
        """
        Adds city, state, and geocoded X, Y coordinates to the extracted addresses.
        :param: None
        :return: None
        """
        logging.debug("Entering transform function")

        try:
            logging.debug("Add City, State")

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
                    logging.debug(address)
                    geocode_url = fr"{self.config_dict.get('geocoder_prefix_url')}" + address + fr"{self.config_dict.get('geocoder_suffix_url')}"


                    r = requests.get(geocode_url)
                    resp_dict = r.json()
                    address_matches = resp_dict['result']['addressMatches']
                    if address_matches:
                        x = address_matches[0]['coordinates']['x']
                        y = address_matches[0]['coordinates']['y']
                    else:
                        logging.debug(f"No coordinates found for address: {address}")
                        continue

                    row['X'] = x
                    row['Y'] = y
                    row['Type'] = 'Residential'
                    csv_writer.writerow(row)
        except Exception as e:
            print(f"Error in the GSheets transform function{e}")

        logging.debug("Exiting transform function")

    def load(self):
        """
        Loads the transformed addresses into an ArcGIS feature class.
        :param: None
        :return: None
        """
        logging.debug("Entering load function")

        try:
            arcpy.env.workspace = rf"{self.config_dict.get('proj_dir')}WestNileOutbreak.gdb\\"
            arcpy.env.overwriteOutput = True

            in_table = self.config_dict.get('proj_dir') + "output.csv"
            out_feature_class = "avoid_points"
            x_coords = "X"
            y_coords = "Y"

            arcpy.management.Delete(out_feature_class, "FeatureClass")

            arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

            logging.debug(arcpy.GetCount_management(out_feature_class))
        except Exception as e:
            print(f"Error in the GSheets load function{e}")


        logging.debug("Exiting load function")

    def process(self):
        """
        Executes the full ETL process (extract, transform, and load).
        :param: None
        :return: None
        """
        logging.debug("Entering ETL processing function")
        self.extract()
        self.transform()
        self.load()
        logging.debug("Exiting ETL processing function")
