import yaml
import arcpy
from Etl.GSheetsEtl import GSheetsEtl


# Create an empty list for output layer names for later use in the intersect function
buffer_layer_name_list = []


def etl(config_dict):
    print("etling...")
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()



def setup():

    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)


    arcpy.env.workspace = fr"{config_dict.get('proj_dir')}WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    return config_dict


def delete_if_exists(layer):
    if arcpy.Exists(layer):
        arcpy.Delete_management(layer)


def buffer(layer_name):
    # Ask for a buffer distance for the given layer
    buf_dist = input(f"Please input a buffer distance for {layer_name}")

    # Buffer the incoming layer by the buffer distance and add names to the list
    output_buffer_layer_name = f"buf_{layer_name}"
    print(f"Buffering {layer_name} to generate {output_buffer_layer_name} layer...")
    buffer_layer_name_list.append(output_buffer_layer_name)

    # Run the buffer analysis
    arcpy.analysis.Buffer(layer_name, output_buffer_layer_name, buf_dist, "FULL", "ROUND", "All")


def intersect(intersect_lyr_name):
    # Run an intersect operation on multiple input layers
    arcpy.Intersect_analysis(buffer_layer_name_list, intersect_lyr_name)
    print("Intersect operation running...")


def spatial_join(intersect_lyr_name):
    # join the address layer and the intersect layer
    print("Join operation running...")
    arcpy.analysis.SpatialJoin("Addresses", intersect_lyr_name, "joined_addresses")

def erase(buf_Avoid_Points, intersect_lyr_name):
    print("Erasing avoid points from the intersect layer")
    print("Creating new layer called intersect_minus_avoidPoints")
    intersect_minus_avoidPoints = "intersect_minus_avoidPoints"
    arcpy.analysis.Erase(intersect_lyr_name, buf_Avoid_Points, intersect_minus_avoidPoints)

# I had to add this function in order to pass the joined_addresses variable into the mix when adding it to the map
def add_layer_to_map(joined_addresses):

    proj_path = fr"{config_dict.get('proj_dir')}"
    aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")

    map_doc = aprx.listMaps()[0]
    map_doc.addDataFromPath(rf"{proj_path}WestNileOutbreak.gdb\{joined_addresses}")

    aprx.save()


def count_addresses_within_layer(target_layer, intersect_layer):
    # Select features from the target_layer that are within the intersect_layer
    arcpy.management.SelectLayerByLocation(target_layer, "WITHIN", intersect_layer)

    # Count the selected features
    count = int(arcpy.GetCount_management(target_layer).getOutput(0))

    return count


def main():
    global config_dict
    config_dict = setup()
    print(config_dict)
    etl(config_dict)

    # To buffer the layers appropriately, I had to create a dictionary to store the value pairs
    # This way it would ask for the buffer values all at once instead of one at a time, which was technically the instruction, I think.
    #buffer_answer_list = {}

    #### VERIFY YOUR LAYER NAMES HERE ####
    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs___Boulder_County", "OSMP_Properties"]
    # Loop through the layers in layer list and create the appropriate buffer for each layer
    for layer in buffer_layer_list:
        buffer(layer)

    # Buffer the avoid points too
    Avoid_Points = "Avoid_Points"
    buf_Avoid_Points = "buf_Avoid_Points"
    buf_avoid_answer = input("Please give a buffer distance for points to avoid")
    delete_if_exists(buf_Avoid_Points)
    arcpy.analysis.Buffer(Avoid_Points, buf_Avoid_Points, buf_avoid_answer, "FULL", "ROUND", "All")

    # Ask user for an output layer name here, so it's outside the function
    intersect_lyr_name = input("What would you like to name the layer generated by our intersect function?")

    intersect(intersect_lyr_name)
    spatial_join(intersect_lyr_name)
    add_layer_to_map("joined_addresses")
    erase(buf_Avoid_Points, intersect_lyr_name)
    # Create a feature layer from the joined_addresses feature class
    arcpy.management.MakeFeatureLayer("joined_addresses", "joined_addresses_layer")

    count = count_addresses_within_layer("joined_addresses_layer", "intersect_minus_avoidPoints")
    print(f"{count} joined_addresses need to be notified.")


if __name__ == '__main__':
    main()



