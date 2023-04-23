"""
This script automates the process of identifying and notifying addresses at risk of West Nile Virus outbreak.
It performs ETL operations on data from Google Sheets, applies buffers on multiple input layers, and intersects
them to create an area of interest. It then erases buffered avoid points from the intersect layer, performs a
spatial join with the address layer, and selects target addresses for notification. The script also modifies the
symbology of layers and exports the map to a PDF file with a user-defined subtitle and current date/time.
"""

import yaml
import arcpy
import logging
import datetime
from Etl.GSheetsEtl import GSheetsEtl

config_dict = None

# Create an empty list for output layer names for later use in the intersect function
buffer_layer_name_list = []


def etl():
    """
    Extracts, transforms, and loads data from Google Sheets.
    :param: None
    :return: None
    """
    global config_dict
    logging.debug("Entering etl function")

    try:
        etl_instance = GSheetsEtl(config_dict)
        etl_instance.process()
    except Exception as e:
        print(f"Error in ETL {e}")


    logging.debug("Exiting etl function")



def setup():
    """
    Reads configuration data, sets up the workspace and logging.
    :param: None
    :return: Configuration dictionary
    """
    global config_dict
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)


    arcpy.env.workspace = fr"{config_dict.get('proj_dir')}WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    logging.basicConfig(filename=f"{config_dict.get('proj_dir')}wnv.log", filemode="w", level=logging.DEBUG)

    return config_dict


def delete_if_exists(layer):
    """
    A helper function that deletes a specified layer to keep things tidy.
    :param layer: Layer to be deleted if it exists
    :return: None
    """
    logging.debug(f"Entering delete_if_exists function for layer {layer}")

    try:
        if arcpy.Exists(layer):
            arcpy.Delete_management(layer)
    except Exception as e:
        print(f"Error in delete_if_exists {layer} {e}")

    logging.debug("Exiting delete_if_exists function")

def buffer(layer_name):
    """
    Buffers a layer by the specified distance and adds output layer name to buffer_layer_name_list.
    :param layer_name: The name of the layer to buffer
    :return: None
    """
    logging.debug("Entering buffer function")
    # Ask for a buffer distance for the given layer
    buf_dist = input(f"Please input a buffer distance for {layer_name}")

    try:
        # Buffer the incoming layer by the buffer distance and add names to the list
        output_buffer_layer_name = f"buf_{layer_name}"
        logging.debug(f"Buffering {layer_name} to generate {output_buffer_layer_name} layer...")
        buffer_layer_name_list.append(output_buffer_layer_name)

        # Run the buffer analysis
        arcpy.analysis.Buffer(layer_name, output_buffer_layer_name, buf_dist, "FULL", "ROUND", "All")
    except Exception as e:
        print(f"Error in buffer function {e}")

    logging.debug("Exiting buffer function")


def buffer_processing():
    """
    Buffers multiple input layers based on the configuration dictionary.
    :param: None
    :return: None
    """
    global config_dict
    logging.debug("Entering buffer_processing function")

    try:
        buffer_layer_list = config_dict["buffer_layer_list"]

        # Loop through the layers in layer list and create the appropriate buffer for each layer
        for layer in buffer_layer_list:
            buffer(layer)
    except Exception as e:
        print(f"Error in buffer_processing {e}")

    logging.debug("Exiting buffer_processing function")


def buffer_avoid_points():
    """
    Buffers avoid points and returns the buffered layer.
    :param: None
    :return: Buffered avoid points layer name
    """
    logging.debug("Entering buffer_avoid_points function")

    try:
        Avoid_Points = "Avoid_Points"
        buf_Avoid_Points = "buf_Avoid_Points"
        buf_avoid_answer = input("Please give a buffer distance for points to avoid")
        delete_if_exists(buf_Avoid_Points)
        arcpy.analysis.Buffer(Avoid_Points, buf_Avoid_Points, buf_avoid_answer, "FULL", "ROUND", "All")
    except Exception as e:
        print(f"Error in buffer_avoid_points {e}")

    logging.debug("Exiting buffer_avoid_points function")
    return buf_Avoid_Points


def intersect(intersect_lyr_name="intersect"):
    """
    Run an intersect operation on multiple input layers.
    :param intersect_lyr_name: Name of the output intersect layer
    :return: None
    """
    logging.debug("Entering intersect function")

    try:
        arcpy.Intersect_analysis(buffer_layer_name_list, intersect_lyr_name)
    except Exception as e:
        print(f"Error in intersect function {e}")

    logging.debug("Exiting intersect function")


def spatial_join(intersect_minus_avoidPoints_lyr):
    """
    Joins the address layer with the intersect_minus_avoidPoints layer.
    :param intersect_minus_avoidPoints_lyr: Layer name to be joined with the address layer
    :return: None
    """
    logging.debug("Entering join function")

    try:
        delete_if_exists("joined_addresses")
        arcpy.analysis.SpatialJoin("Addresses", intersect_minus_avoidPoints_lyr, "joined_addresses")
    except Exception as e:
        print(f"Error in spatial_join function {e}")

    logging.debug("Exiting join function")

def process_joined_addresses(buf_Avoid_Points, intersect_lyr_name):
    """
    Performs the intersect, erase, and spatial join operations.
    :param buf_Avoid_Points: Buffered avoid points layer name
    :param intersect_lyr_name: Name of the output intersect layer
    :return: None
    """
    logging.debug("Entering process_joined_addresses function")

    try:
        intersect(intersect_lyr_name)
        erase(buf_Avoid_Points, intersect_lyr_name)
        spatial_join("intersect_minus_avoidPoints")

        # Create a feature layer from the joined_addresses feature class and print the count
        arcpy.management.MakeFeatureLayer("joined_addresses", "joined_addresses_layer")
        count = count_addresses_within_layer("joined_addresses_layer", "intersect_minus_avoidPoints")
        print(f"{count} addresses need to be notified.")
    except Exception as e:
        print(f"Error in process_joined_addresses function {e}")

    logging.debug("Exiting process_joined_addresses function")


def erase(buf_Avoid_Points, intersect_lyr_name):
    """
    Erases the avoid point buffers from the intersect layer and adds the new layer to the map.
    :param buf_Avoid_Points: Buffered avoid points layer name
    :param intersect_lyr_name: Name of the output intersect layer
    :return: None
    """
    global config_dict
    logging.debug("Entering erase function")

    try:
        logging.debug("Creating new layer called intersect_minus_avoidPoints")
        delete_if_exists("intersect_minus_avoidPoints")
        intersect_minus_avoidPoints = "intersect_minus_avoidPoints"
        arcpy.analysis.Erase(intersect_lyr_name, buf_Avoid_Points, intersect_minus_avoidPoints)

        add_layer_to_map("intersect_minus_avoidPoints")
    except Exception as e:
        print(f"Error in erase function{e}")

    logging.debug("Exiting erase function")


def add_layer_to_map(added_layer_name):
    """
    A helpful function which adds a layer to the map after first deleting it if it already exists.
    :param added_layer_name: Layer name to be added to the map
    :return: None
    """
    global config_dict
    logging.debug(f"Entering add layer function for {added_layer_name}")

    try:
        proj_path = fr"{config_dict.get('proj_dir')}"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}WestNileOutbreak.aprx")

        map_doc = aprx.listMaps()[0]

        # Remove the layer with the same name if it already exists
        for lyr in map_doc.listLayers():
            if lyr.name == added_layer_name:
                map_doc.removeLayer(lyr)
                break

        map_doc.addDataFromPath(rf"{proj_path}WestNileOutbreak.gdb\{added_layer_name}")

        aprx.save()
    except Exception as e:
        print(f"Error in add_layer_to_map function {e}")

    logging.debug("Exiting add layer function")


def count_addresses_within_layer(target_layer, intersect_minus_avoidPoints):
    """
    Counts the number of addresses within the intersect_minus_avoidPoints layer.
    :param target_layer: Target layer with addresses
    :param intersect_minus_avoidPoints: Intersect layer for selecting addresses
    :return: Count of addresses within the intersect layer
    """
    logging.debug("Entering count addresses function")

    try:
        # Select features from the target_layer that are within the intersect_layer
        arcpy.management.SelectLayerByLocation(target_layer, "WITHIN", intersect_minus_avoidPoints)

        # Count the selected features
        count = int(arcpy.GetCount_management(target_layer).getOutput(0))
    except Exception as e:
        print(f"Error in {e}")

    logging.debug("Exiting count addresses function")

    return count

def select_target_addresses():
    """
    Selects addresses within the intersect_minus_avoidPoints buffer and exports them to a new layer.
    :param: None
    :return: None
    """
    logging.debug("Entering select_target_addresses function")

    try:
        delete_if_exists("target_addresses")
        arcpy.management.MakeFeatureLayer("joined_addresses", "joined_addresses_layer")
        arcpy.management.SelectLayerByAttribute("joined_addresses_layer", "NEW_SELECTION", "Join_Count = 1")
        arcpy.management.CopyFeatures("joined_addresses_layer", "target_addresses")

        # Add the target_addresses layer to the map
        add_layer_to_map("target_addresses")
    except Exception as e:
        print(f"Error in select_target_addresses function {e}")

    logging.debug("Exiting select_target_addresses function")


def pre_export_symbology(lyr_name):
    """
    Changes the symbology of a layer before exporting the map.
    :param lyr_name: Layer name for which the symbology is to be changed
    :return: None
    """
    global config_dict
    logging.debug("Entering pre_export_symbology function")

    try:
        proj_path = fr"{config_dict.get('proj_dir')}"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}WestNileOutbreak.aprx")
        map_doc = aprx.listMaps()[0]

        # Change spatial reference
        map_doc.spatialReference = arcpy.SpatialReference(2231)

        # Set layer symbology
        lyr = map_doc.listLayers(lyr_name)[0]
        sym = lyr.symbology

        sym.renderer.symbol.color = {'RGB': [255, 0, 0, 100]}
        sym.renderer.symbol.outlineColor = {'RGB': [0, 0, 0, 100]}
        lyr.symbology = sym
        lyr.transparency = 50

        aprx.save()
    except Exception as e:
        print(f"Error in pre_export_symbology function {e}")

    logging.debug("Exiting pre_export_symbology function")


def exportMap():
    """
    Exports the map with user-defined subtitle and current date/time.
    :param: None
    :return: None
    """
    global config_dict
    logging.debug("Entering Export function")

    try:
        aprx = arcpy.mp.ArcGISProject(f"{config_dict.get('proj_dir')}WestNileOutbreak.aprx")
        lyt = aprx.listLayouts()[0]
        user_subtitle = input("Please enter the sub-title for the output map: ")

        current_date = datetime.datetime.now().strftime("%m/%d/%Y")
        current_time = datetime.datetime.now().strftime("%I:%M %p")

        for el in lyt.listElements():
            logging.debug(el.name)
            if "Title" in el.name:
                el.text = el.text + user_subtitle
            elif "Date" in el.name:
                el.text = f"{current_date}"
            elif "Time" in el.name:
                el.text = f"{current_time}"

        pdf_output = f"{config_dict.get('proj_dir')}WestNileOutbreak_{user_subtitle}.pdf"
        lyt.exportToPDF(pdf_output)
    except Exception as e:
        print(f"Error in export_map function {e}")

    logging.debug("Exiting Export function")

def main():
    """
    The main function that runs the entire script.
    :param: None
    :return: None
    """
    global config_dict
    config_dict = setup()
    logging.info("Starting West Nile Virus Simulation")
    logging.debug(config_dict)
    etl()

    buffer_processing()

    buf_Avoid_Points = buffer_avoid_points()
    process_joined_addresses(buf_Avoid_Points, "intersect")
    pre_export_symbology("intersect_minus_avoidPoints")
    select_target_addresses()
    exportMap()

if __name__ == '__main__':
    main()