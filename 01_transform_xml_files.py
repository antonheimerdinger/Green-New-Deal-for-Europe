import xml.etree.ElementTree as ET
import pandas as pd
import os

# Define the directories
input_dir = 'Gesamtdatenexport_20241001_24.1'

types = {
    'AnlagenEegSolar': 'AnlageEegSolar',
    'EinheitenSolar': 'EinheitSolar'
}


# Function to extract data from the XML
def xml_to_dict_list(root, type):
    data_list = []
    for einheit in root.findall(type):
        data = {}
        for elem in einheit:
            data[elem.tag] = elem.text
        data_list.append(data)
    return data_list


for type in types:
    output_dir = f'data/{types[type]}'

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # create file list
    file_list = [i for i in os.listdir(input_dir) if i.startswith(f'{i}_')]
    file_list = sorted(file_list, key=lambda x: int(x.split('_')[-1].split('.')[0]))

    # Iterate through all files in 'original_data'
    for file_name in file_list:
        file_path = os.path.join(input_dir, file_name)

        # Read the XML file and remove the XML declaration if present
        with open(file_path, 'r', encoding='utf-16') as file:
            content = file.read()

        # Remove the XML declaration line (if present)
        if content.startswith("<?xml"):
            content = content.split("?>", 1)[1]

        # Parse the remaining content
        root = ET.fromstring(content)

        # Convert the XML data into a list of dictionaries
        data_list = xml_to_dict_list(root, type=types[type])

        # Convert the list of dictionaries into a pandas DataFrame
        df = pd.DataFrame(data_list)

        # Save the DataFrame as a CSV in the 'data' directory
        output_file_path = os.path.join(output_dir, file_name.replace('.xml', '.parquet'))
        df.to_parquet(output_file_path, index=False)

        print(f"Processed and saved: {output_file_path}")
