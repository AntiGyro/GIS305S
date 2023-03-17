import arcpy
import requests
import csv


def extract():
    print("Calling extract function...")
    r = requests.get(r"https://docs.google.com/spreadsheets/d/e/2PACX-1vTDjitOlmILea7koCORJkq6QrUcwBJM7K3vy4guXB0mU_nWR6wsPn136bpH6ykoUxyYMW7wTwkzE37l/pub?output=csv")
    r.encoding = "utf-8"
    data = r.text
    with open(r"C:\Users\natha\Documents\School\Nathan\Fall 2023\ProgForGis\Assignment 9\addresses.csv", "w") as output_file:
        output_file.write(data)





def transform():
    print("Add City, State")

    with open(r"C:\Users\natha\Documents\School\Nathan\Fall 2023\ProgForGis\Assignment 9\addresses.csv", "r") as input_file, \
         open(r"C:\Users\natha\Documents\School\Nathan\Fall 2023\ProgForGis\Assignment 9\output.csv", "w", newline='') as output_file:
        csv_reader = csv.DictReader(input_file)
        fieldnames = csv_reader.fieldnames + ['X', 'Y', 'Type']
        csv_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            address = row["Street Address"] + " Boulder CO"
            print(address)
            geocode_url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=" + address + "&benchmark=2020&format=json"
            print(geocode_url)
            r = requests.get(geocode_url)
            resp_dict = r.json()
            x = resp_dict['result']['addressMatches'][0]['coordinates']['x']
            y = resp_dict['result']['addressMatches'][0]['coordinates']['y']
            row['X'] = x
            row['Y'] = y
            row['Type'] = 'Residential'
            csv_writer.writerow(row)

def load():
    arcpy.env.workspace = r"C:\Users\natha\Documents\School\Nathan\Fall 2023\ProgForGis\Lab1\WestNileOutbreak.gdb\\"
    arcpy.env.overwriteOutput = True

    in_table = r"C:\Users\natha\Documents\School\Nathan\Fall 2023\ProgForGis\Assignment 9\output.csv"
    out_feature_class = "avoid_points"
    x_coords = "X"
    y_coords = "Y"

    arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords,y_coords)

    print(arcpy.GetCount_management(out_feature_class))

if __name__ == "__main__":
    extract()
    transform()
    load()
