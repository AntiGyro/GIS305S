# West Nile Virus Spray Zones #

This program is used to map West Nile Virus high-risk areas in Boulder County.
It provides a set of Python modules for performing Extract, Transform, and Load (ETL) operations on spatial data, 
specifically focusing on address data from Google Sheets. 
The ETL process extracts addresses from a Google Sheets form, transforms them by adding city, state, and geocoded 
coordinates, and loads the transformed data into an ArcGIS feature class. This enables the processing and integration 
of spatial data from Google Sheets with other GIS data sources.
The output from this process is a pdf showing the areas to spray and the target addresses within those areas.

## The project consists of the following modules: ##

****finalproject.py:****
Contains the main code for the program.
    
****SpatialEtl.py:****
Contains the SpatialEtl superclass, which provides a high-level structure for the ETL process for spatial data.

****GSheetsEtl.py:****
Inherits from the SpatialEtl superclass and provides a specific implementation for working with 
Google Sheets data. This class includes methods for extracting, transforming, and loading address data from a 
Google Sheets form into an ArcGIS feature class.

## To run the code, follow these steps: ##

****Ensure you have the necessary dependencies installed. You will need:****

- Python 3.7 or later
- requests library
- csv library
- ArcPy library (ArcGIS Pro Python environment is recommended)

****Set up the configuration file with the required parameters for the ETL process. ****

The config file should contain the following keys and values:

remote_url: The URL of the Google Sheets form containing the addresses.
proj_dir: The project directory where the input and output files should be stored.
geocoder_prefix_url: The prefix URL of the geocoding service to use for address geocoding.
geocoder_suffix_url: The suffix URL of the geocoding service to use for address geocoding.
buffer_layer_list: A list of layers that will be used for buffering analysis.

****Run finalproject.py****

****Input buffer distances for each layer.****

****Input a sub-title name for the exported map.****
