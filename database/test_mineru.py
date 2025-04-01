import requests

url = "http://106.14.88.25:8000/pdf_parse?return_content_list=true&is_json_md_dump=true&return_layout=true&return_info=true&return_images=true&pdf_path=s3://dj-storage/dj-docs/4a1881ce8f6a513cf285de70757f77df6ec3bfa4.pdf"

payload = {}
files={}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)