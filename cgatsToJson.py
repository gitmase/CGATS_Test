import json
import uuid

class MeasurementData:
    # Measurement data class which will be stored as fields in a database table
    def __init__(self,
                 Measurement_ID=None,
                 Measurement_Name=None,
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
                 Measurement_Geometry_Choice=None,
                 Measurement_Illumination=None,
                 Measurement_Angle=None,
                 Measurement_Filter=None,
                 Measurement_Polarization=None,
                 Sample_Backing=None,
                 Software=None,
                 Heat_Press_Name=None):
        self.Measurement_ID = Measurement_ID
        self.Measurement_Name = Measurement_Name
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
        self.Measurement_Geometry_Choice = Measurement_Geometry_Choice
        self.Measurement_Illumination = Measurement_Illumination
        self.Measurement_Angle = Measurement_Angle
        self.Measurement_Filter = Measurement_Filter
        self.Measurement_Polarization = Measurement_Polarization
        self.Sample_Backing = Sample_Backing
        self.Software = Software
        self.Heat_Press_Name = Heat_Press_Name

    def to_dict(self):
        """Convert the object to a dictionary."""
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


def parse_descriptive_lines(file_path_local):
    # print(f"Reading file: {file_path_local}")
    descriptive_lines = []
    # Generate a GUID for Measurement_ID

    with open(file_path_local, 'r') as file:
        for line in file:
            line = line.strip().upper()
            if line.startswith("BEGIN"):
                break
            try:
                if remove_first_word(line) is None:
                    continue
            except IndexError:
                # print(f"Error processing line: {line}")
                continue

        match True:
            case _ if line.startswith("CREATOR") or line.startswith("ORIGINATOR"):
                measurement.Originator = remove_first_word(line)
            case _ if line.startswith("CREATION_DATE") or line.startswith("CREATED"):
                measurement.Creation_Date = remove_first_word(line)
            case _ if line.startswith("TARGET") or line.startswith("TARGET_NAME"):
                measurement.Target_ID = remove_first_word(line)
            case _ if line.startswith("MANUFACTURER"):
                measurement.Media_Manufacturer = remove_first_word(line)
            case _ if line.startswith("MATERIAL"):
                measurement.Media_Material = remove_first_word(line)
            case _ if line.startswith("PROD_DATE") or line.startswith("CREATED"):
                measurement.Media_Prod_Date = remove_first_word(line)
            case _ if line.startswith("SERIAL"):
                measurement.Device_Serial_Number = remove_first_word(line)
            case _ if line.startswith("INSTRUMENTATION") or line.startswith("INSTRUMENT") or line.startswith("DEVICE"):
                measurement.Device_Model = remove_first_word(line)
            case _ if line.startswith("SOFTWARE"):
                measurement.Software = remove_first_word(line)
            case _ if line.startswith("COMMENT"):
                measurement.Comment = remove_first_word(line)
            case _ if line.startswith("JOB_NAME") or line.startswith("JOB_ID"):
                measurement.Measurement_Name = remove_first_word(line)
            case _ if line.startswith("DESCRIPTION"):
                measurement.Description = remove_first_word(line)
            case _ if line.startswith("HEAT_PRESS_NAME"):
                measurement.Heat_Press_Name = remove_first_word(line)
            case _ if line.startswith("HEAT_PRESS_TEMP"):
                measurement.Heat_Press_Temp = remove_first_word(line)
            case _ if line.startswith("HEAT_PRESS_TIME"):
                measurement.Heat_Press_Time = remove_first_word(line)
            case _ if line.startswith("PRINTER_NAME"):
                measurement.Printer_Name = remove_first_word(line)
            case _ if line.startswith("INK_MANUFACTURER"):
                measurement.Ink_Manufacturer = remove_first_word(line)
            case _ if line.startswith("INK_SET"):
                measurement.Ink_Set = remove_first_word(line)
            case _ if line.startswith("INK_TYPE"):
                measurement.Ink_Type = remove_first_word(line)
            case _ if line.startswith("SAMPLE_BACKING"):
                measurement.Sample_Backing = remove_first_word(line)
            case _ if line.startswith("ILLUMINANT") or line.startswith("ILLUMINATION_NAME"):
                measurement.Measurement_Illumination = remove_first_word(line)
            case _ if line.startswith("OBSERVER") or line.startswith("OBSERVER_ANGLE"):
                measurement.Measurement_Angle = remove_first_word(line)
            case _ if line.startswith("NUMBER_OF_FIELDS"):
                measurement.Number_Of_Fields = remove_first_word(line)
            case _ if line.startswith("NUMBER_OF_SETS"):
                measurement.Number_Of_Sets = remove_first_word(line)
            case _ if line.startswith("LGOROWLENGTH"):
                measurement.Row_Length = remove_first_word(line)         #
          

            # if line.startswith("CREATOR") or line.startswith("ORIGINATOR"):
            #     measurement.Originator = remove_first_word(line)
            # elif line.startswith("CREATION_DATE") or line.startswith("CREATED"):
            #     measurement.Creation_Date = remove_first_word(line)
            # elif line.startswith("TARGET") or line.startswith("TARGET_NAME"):
            #     measurement.Target_ID = remove_first_word(line)
            # elif line.startswith("MANUFACTURER"):
            #     measurement.Media_Manufacturer = remove_first_word(line)
            # elif line.startswith("MATERIAL"):
            #     measurement.Media_Material = remove_first_word(line)
            # elif line.startswith("PROD_DATE") or line.startswith("CREATED"):
            #     measurement.Media_Prod_Date = remove_first_word(line)
            # elif line.startswith("SERIAL"):
            #     measurement.Device_Serial_Number = remove_first_word(line)
            # elif line.startswith("INSTRUMENTATION") or line.startswith("INSTRUMENT") or line.startswith("DEVICE"):
            #     measurement.Device_Model = remove_first_word(line)
            # elif line.startswith("SOFTWARE"):
            #     measurement.Software = remove_first_word(line)
            # elif line.startswith("COMMENT"):
            #     measurement.Comment = remove_first_word(line)
            # elif line.startswith("JOB_NAME") or line.startswith("JOB_ID"):
            #     measurement.Measurement_Name = remove_first_word(line)
            # elif line.startswith("DESCRIPTION"):
            #     measurement.Description = remove_first_word(line)
            # elif line.startswith("HEAT_PRESS_NAME"):
            #     measurement.Heat_Press_Name = remove_first_word(line)
            # elif line.startswith("HEAT_PRESS_TEMP"):
            #     measurement.Heat_Press_Temp = remove_first_word(line)
            # elif line.startswith("HEAT_PRESS_TIME"):
            #     measurement.Heat_Press_Time = remove_first_word(line)
            # elif line.startswith("PRINTER_NAME"):
            #     measurement.Printer_Name = remove_first_word(line)
            # elif line.startswith("INK_MANUFACTURER"):
            #     measurement.Ink_Manufacturer = remove_first_word(line)
            # elif line.startswith("INK_SET"):
            #     measurement.Ink_Set = remove_first_word(line)
            # elif line.startswith("INK_TYPE"):
            #     measurement.Ink_Type = remove_first_word(line)
            # elif line.startswith("SAMPLE_BACKING"):
            #     measurement.Sample_Backing = remove_first_word(line)
            # elif line.startswith("ILLUMINANT") or line.startswith("ILLUMINATION_NAME"):
            #     measurement.Measurement_Illumination = remove_first_word(line)
            # elif line.startswith("OBSERVER") or line.startswith("OBSERVER_ANGLE"):
            #     measurement.Measurement_Angle = remove_first_word(line)
            # elif line.startswith("NUMBER_OF_FIELDS"):
            #     measurement.Number_Of_Fields = remove_first_word(line)
            # elif line.startswith("NUMBER_OF_SETS"):
            #     measurement.Number_Of_Sets = remove_first_word(line)
            # elif line.startswith("LGOROWLENGTH"):
            #     measurement.Row_Length = remove_first_word(line)

            descriptive_lines.append(line)

        # Print the object as a dictionary
        print(measurement.Originator)
        print(measurement.to_dict())
    return descriptive_lines


def extract_keys_and_data(file_path_local):
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
                    line = "Measurement_ID " + line
                    keys_local = line.split()
                case _ if is_parsing_data:
                    line = measurement.Measurement_ID + " " + line
                    values = line.split()
                    record = dict(zip(keys_local, values))
                    data_local.append(record)

    return keys_local, data_local


def save_to_json(data_local, output_path):
    with open(output_path, 'w') as f:
        json.dump(data_local, f, indent=4)
    print(f"JSON data saved to {output_path}")


if __name__ == '__main__':
    # Path to the uploaded file
    path = '/Users/aps/Docs/TestData/'
    file_path = path + 'printerTest.cie.txt'

    measurement = MeasurementData(
        Measurement_ID=str(uuid.uuid4())  # Generate a unique GUID
    )

    # parse descriptive lines
    parse_descriptive_lines(file_path)

    # Extract keys and data
    keys, data = extract_keys_and_data(file_path)

    # Save the data to JSON
    output_file = path + 'printerTest.output.json'
    save_to_json(data, output_file)

    print(f"JSON data saved to {output_file}")
