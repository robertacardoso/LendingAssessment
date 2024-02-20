import requests

def download_csv(url, filename):
    """
    Download a CSV file from the specified URL and save it locally.

    Args:
    - url (str): URL of the CSV file.
    - filename (str): Name to save the CSV file locally.

    Returns:
    - str: Local file path of the downloaded CSV file.
    """
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)
    return filename
