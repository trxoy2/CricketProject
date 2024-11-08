import json

# Function to convert lists and dictionaries to JSON strings in a DataFrame
def convert_to_jsonstring(df):
    def convert_element(element):
        if isinstance(element, dict) or isinstance(element, list):
            return json.dumps(element)
        else:
            return element

    for col in df.columns:
        df[col] = df[col].apply(convert_element)
    return df