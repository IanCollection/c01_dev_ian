import requests

url = "http://106.14.88.25:8000/pdf_parse?return_content_list=true&is_json_md_dump=true&return_layout=true&return_info=true&return_images=true&pdf_path=s3://dj-storage/dj-docs/41d7199401da700dd23e59c75914003c69ce5ac5.pdf"

payload = {}
files={}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)