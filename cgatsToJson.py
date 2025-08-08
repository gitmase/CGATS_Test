import json
import uuid
import os
import argparse
import re
from datetime import date


# Version 1.3
# -*- coding: utf-8 -*-

# Converts a CGATS Color Data file to JSON format
# This script reads the CGATS file, extracts measurement data and descriptive lines,
# and saves the data to a JSON file.
# The script uses the uuid module to generate a unique identifier for each measurement.
# The script uses the json module to save the data in JSON format.
# The script uses the os module to handle file paths.
# The script defines a MeasurementData class to store measurement data.
# The script defines several functions to process the CGATS file:
# - remove_first_word: removes the first word from a line
# - get_first_word: gets the first word from a line
# - replace_tabs_with_spaces: replaces tabs with spaces in a line
# - to_number: converts a string to a number (int or float)
# - convert_array_values: converts an array of strings to numbers
# - convert_values: converts a list of keys and values to a dictionary
# - parse_descriptive_lines: parses the descriptive lines of the CGATS file
# - extract_keys_and_data: extracts keys and data from the CGATS file
# - save_to_json: saves the data to a JSON file
# The script is designed to work with CGATS files that contain measurement data and descriptive lines.
# The script is designed to be run as a standalone program.
# The script is designed to be run in Python 3.x.
#
# Updates
# 27 Jul 2025 Version 1.3: 
#     Added command line arguments 
#     Added --debug and --verbose arguments
#     Not useing MeasurementData Class which may be useful for database functionality
#     Corrected some minor formating problems

# vdebug = False
# verbose = False

class MeasurementData:
    # This is not implemented yet
    # Measurement data class which will be stored as fields in a database table
    def __init__(self,
                 Measurement_ID=None,
                 Measurement_Name=None,
                 Measurement_Date=None,
                 Originator=None,
                 Description=None,
                 Creation_Date=None,
                 Comment=None,
                 Target_ID=None,
                 Target_Type=None,
                 Media_Manufacturer=None,
                 Media_Material=None,
                 Media_Prod_Date=None,
                 Media_Lot_Number=None,
                 Device_Manufacturer=None,
                 Device_Model=None,
                 Device_Serial_Number=None,
                 Measurement_Geometry=None,
                 Measurement_Illumination=None,
                 Measurement_Angle=None,
                 Measurement_Filter=None,
                 Measurement_Polarization=None,
                 Sample_Backing=None,
                 Software=None,
                 Heat_Press_Name=None,
                 Data_Format=None ):
        self.Measurement_ID = Measurement_ID
        self.Measurement_Name = Measurement_Name
        self.Measurement_Date =Measurement_Date
        self.Originator = Originator
        self.Description = Description
        self.Creation_Date = Creation_Date
        self.Comment = Comment
        self.Target_ID = Target_ID
        self.Target_Type = Target_Type
        self.Media_Manufacturer = Media_Manufacturer
        self.Media_Material = Media_Material
        self.Media_Prod_Date = Media_Prod_Date
        self.Media_Lot_Number = Media_Lot_Number
        self.Device_Manufacturer = Device_Manufacturer
        self.Device_Model = Device_Model
        self.Device_Serial_Number = Device_Serial_Number
        self.Measurement_Geometry = Measurement_Geometry
        self.Measurement_Illumination = Measurement_Illumination
        self.Measurement_Angle = Measurement_Angle
        self.Measurement_Filter = Measurement_Filter
        self.Measurement_Polarization = Measurement_Polarization
        self.Sample_Backing = Sample_Backing
        self.Software = Software
        self.Heat_Press_Name = Heat_Press_Name
        self.Data_Format = Data_Format


def log(msg, verbose=False, debug=False):
    if debug:
        print(f"[DEBUG] {msg}")
    elif verbose:
        print(f"[INFO] {msg}")

def to_dict(self):
    # Convert the object to a dictionary.
    return self.__dict__


def remove_first_word(input_string):
    # Split on tabs or space
    # Strip leading and trailing quotes, spaces and tabs
    # Assumes that there is max of one tab in the input string
    # Return None on error
    try:
        # Either a tab or a space can be the separator
        input_string = input_string.strip(' /t')
        # Split on the first occurrence of either a space or tab
        split_char = '\t' if '\t' in input_string else ' '
        _, rest = input_string.split(split_char, 1)
    except ValueError:
        return None
    return rest.strip(' \t"')


def get_first_word(input_string: str) -> str:
        # Split on tabs or space
        # Strip leading and trailing quotes, spaces and tabs
        # Assumes that there is max of one tab in the input string
        # Return None on error
        try:
            # Either a tab or a space can be the separator
            input_string = input_string.strip(' /t')
            # Split on the first occurrence of either a space or tab
            split_char = '\t' if '\t' in input_string else ' '
            first, rest = input_string.split(split_char, 1)
            first =first.strip()
            # Remove trailing colon if present
            first = first[:-1] if first.endswith(':') else first

        except ValueError:
            return None
        return first


def replace_tabs_with_spaces(input_string):
    return input_string.replace('\t', ' ')


def convert_values(key_array, values):
    """Creates a dictionary from keys and values, converting numeric strings to numbers."""
    def smart_convert(value):
        float_pattern = re.compile(r'^[+-]?(?:\d+\.\d*|\.\d+|\d+\.)$')
        int_pattern = re.compile(r'^[+-]?\d+$')

        if isinstance(value, str):
            value = value.strip()
            if float_pattern.match(value):
                return float(value)
            elif int_pattern.match(value):
                return int(value)
        return value  # return as-is if not a convertible stringr

    return dict(zip(key_array, (smart_convert(v) for v in values)))


def parse_descriptive_lines(file_path_local,measurement_uuid):
    log(f"Reading file: {file_path_local}",verbose,debug)
    keys_array = ['MEASUREMENT_ID']
    values = [measurement_uuid]
    
    key = None
    today_str = date.today().isoformat()
    bdf = False
    with open(file_path_local, 'r') as file:
        for line in file:
            line = line.strip().upper()
            if line.startswith("BEGIN_DATA_FORMAT"):
                bdf = True
                continue
            elif line.startswith("BEGIN"):
                break
            elif bdf:
                line = replace_tabs_with_spaces(line)
                # measurement.Data_Format = line
                keys_array.append('DATA_FORMAT')
                values.append(line)
                bdf = False
                continue
            else:
                try:
                    if remove_first_word(line) is None:
                        continue
                except IndexError:
                    log(f"Error processing line: {line}",verbose,debug)
                    continue
            #
            if line.startswith(("CREATOR","ORIGINATOR")):
                # measurement.Originator = remove_first_word(line)
                key = "ORIGINATOR"
            elif line.startswith(("CREATION_DATE","CREATED")):
                # measurement.Creation_Date = remove_first_word(line)
                key = "CREATION_DATE"
                # remove_first_word(line)
            elif line.startswith("TARGET"):
                # measurement.Target_ID = remove_first_word(line)
                key = "TARGET_ID"
            elif line.startswith(("MANUFACTURER","MEDIA_MANUFACTURER")):
                # measurement.Media_Manufacturer = remove_first_word(line)
                key = "MEDIA_MANUFACTURER"
            elif line.startswith(("MATERIAL","MEDIA_MATERIAL")):
                # measurement.Media_Material = remove_first_word(line)
                key = "MEDIA_MATERIAL"
            elif line.startswith(("PROD_DATE","CREATED","MEDIA_PROD_DATE")):
                # measurement.Media_Prod_Date = remove_first_word(line)
                key = "MEDIA_PROD_DATE"
            elif line.startswith(("SERIAL","DEVICE_SERIAL_NUMBER")):
                # measurement.Device_Serial_Number = remove_first_word(line)
                key = "DEVICE_SERIAL_NUMBER"
            elif line.startswith(("INSTRUMENTATION","INSTRUMENT","DEVICE")):
                # measurement.Device_Model = remove_first_word(line)
                key = "DEVICE_MODEL"
            elif line.startswith("SOFTWARE"):
                # measurement.Software = remove_first_word(line)
                key = "SOFTWARE"
            elif line.startswith("COMMENT"):
                # measurement.Comment = remove_first_word(line)
                key = "COMMENT"
            elif line.startswith(("JOB_NAME","JOB_ID","MEASUREMENT_NAME")):
                # measurement.Measurement_Name = remove_first_word(line)
                key = "MEASUREMENT_NAME"
            elif line.startswith(("DESCRIPTION","DESCRIPTOR")):
                # measurement.Description = remove_first_word(line)
                key = "DESCRIPTION"
            elif line.startswith("HEAT_PRESS_NAME"):
                # measurement.Heat_Press_Name = remove_first_word(line)
                key = "HEAT_PRESS_NAME"
            elif line.startswith("HEAT_PRESS_TEMP"):
                # measurement.Heat_Press_Temp = remove_first_word(line)
                key = "HEAT_PRESS_TEMP"
            elif line.startswith("HEAT_PRESS_TIME"):
                # line = "HEAT_PRESS_TIME: " + remove_first_word(line)
                key = "HEAT_PRESS_TIME"
            elif line.startswith("PRINTER_NAME"):
                # line = "PRINTER_NAME: " + remove_first_word(line)
                key = "PRINTER_NAME"
            elif line.startswith("INK_MANUFACTURER"):
                # measurement.Ink_Manufacturer = remove_first_word(line)
                key = "INK_MANUFACTURER"
            elif line.startswith("INK_SET"):
                # measurement.Ink_Set = remove_first_word(line)
                key = "INK_SET"
            elif line.startswith("INK_TYPE"):
                # measurement.Ink_Type = remove_first_word(line)
                key = "INK_TYPE"
            elif line.startswith("SAMPLE_BACKING"):
                # measurement.Sample_Backing = remove_first_word(line)
                key = "SAMPLE_BACKING"
            elif line.startswith(("ILLUMINANT","ILLUMINATION_NAME","MEASUREMENT_ILLUMINATION")):
                # measurement.Measurement_Illumination = remove_first_word(line)
                key = "MEASUREMENT_ILLUMINATION"
            elif line.startswith(("OBSERVER","OBSERVER_ANGLE","MEASUREMENT_ANGLE")):
                # measurement.Measurement_Angle = remove_first_word(line)
                key = "MEASUREMENT_ANGLE"
            elif line.startswith("NUMBER_OF_FIELDS"):
                # measurement.Number_Of_Fields = remove_first_word(line)
                key = "NUMBER_OF_FIELDS"
            elif line.startswith("NUMBER_OF_SETS"):
                # measurement.Number_Of_Sets = remove_first_word(line)
                key = "NUMBER_OF_SETS"
            elif line.startswith(("LGOROWLENGTH","ROW_LENGTH")):
                # measurement.Row_Length = remove_first_word(line)
                key = "ROW_LENGTH"
            else:
                log(f"Line: (line)",verbose,debug)
                key = '# ' + get_first_word(line)
                log(f"Key: {key}",verbose,debug)
                
            # all measurements (values) are defined by removing first word from line
            data = remove_first_word(line)
            log(f"measurement.key: {key} - {data} ",verbose,debug)
            
            if not key.startswith("#"):
              # data_array = remove_first_word(line)
              data = replace_tabs_with_spaces(data)
              log(f"Key: {key}, Data: {data}",verbose,debug)
              keys_array.append(key)
              values.append(data)
              
    if "Measurement_Date" not in keys_array:
    	  key = "MEASUREMENT_DATE"
    	  keys_array.append(key)
    	  values.append(today_str)

    dict(zip(keys_array, values))

        # Print the object as a dictionary
        # print(measurement.Originator)
        # print(measurement.to_dict())
    return  dict(zip(keys_array, values))


def extract_keys_and_data(file_path_local,measurement_uuid):
    keys_local = []
    data_local = []
    is_parsing_keys = False
    is_parsing_data = False

    with open(file_path_local, 'r') as file:
        # JSON data will have the sma Measurement_ID ss the database table record

        for line in file:
            line = line.strip()
            # Detect and parse keys and data using a switch statement
            match line:
                case "BEGIN_DATA_FORMAT":
                    is_parsing_keys = True
                case "END_DATA_FORMAT":
                    is_parsing_keys = False
                case "BEGIN_DATA":
                    is_parsing_data = True
                case "END_DATA":
                    is_parsing_data = False
                case _ if is_parsing_keys:
                    line = "MEASUREMENT_ID: " + line
                    keys_local = line.split()
                case _ if is_parsing_data:
                    line = measurement_uuid + " " + line
                    values = line.split()
                    record = convert_values(keys_local, values)
                    log(f"Value: {values}",verbose,debug)
                    
                    data_local.append(record)

    return keys_local, data_local


def save_to_json(data_local, output_path):
    with open(output_path, 'w') as f:
        json.dump(data_local, f, indent=4)

    log(f"JSON data saved to {output_path}",verbose,debug)
    
    
def main():
    # Global Variables
    global debug
    debug = False
    global verbose
    verbose = False

    parser = argparse.ArgumentParser(description="Process input and output files with a path.")
    parser.add_argument("--path", default="/Users/aps/Docs/TestData/", help="The folder path (default: current directory)")
    parser.add_argument("--input_file", default="printerTest.cie.txt", help="The input file name (default: input.txt)")
    parser.add_argument("--output_file", default="printerTest.output.json", help="The output file name (default: output.txt)")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()
    
    if args.debug:
        debug = True
        print("Debug mode is ON")
    
    if args.verbose:
        verbose = True
        print("Verbose output enabled")
        

    path = args.path
    input_file = path + args.input_file
    output_file = path + args.output_file
    
    measurement_uuid = str(uuid.uuid4())  # Generate a unique GUID
    log(f"measurement_uuid: {measurement_uuid}",verbose,debug)
    
    # parse descriptive lines
    descriptive_data = parse_descriptive_lines(input_file,measurement_uuid)

    # Extract keys and data
    keys, measurement_data = extract_keys_and_data(input_file,measurement_uuid)

    data = {
        "descriptive_data": descriptive_data,
        "measurement_data": measurement_data
    }

    # Save the data to JSON
    # output_file = path + 'printerTest.output.json'
    save_to_json(data, output_file)
    # Save the data to JSON
    
    
    log("End of Program",verbose,debug)


if __name__ == '__main__':
    main()
    

# End of script