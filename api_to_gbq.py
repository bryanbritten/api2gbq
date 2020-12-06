from google.cloud import bigquery
import json
import requests
from lxml import etree

bfr = ""

def xml_to_dict(record):
    root = etree.fromstring(record)
    d = {}
    cols = [
        "id",
        "time",
        "duration",
        "src_comp",
        "src_port",
        "dst_comp",
        "dst_port",
        "protocol",
        "packet_count",
        "byte_count"
    ]
    for col in cols:
        value = root.find(f".//{col}").text
        if col in ["id", "time", "duration", "protocol", "packet_count", "byte_count"]:
            d[col] = int(value) if value is not None else None
        else:
            d[col] = value
    return d

def process_json_buffer():
    global bfr
    try:
        idx = bfr.index("\n")
        record = bfr[0:idx]
        bfr = bfr[(idx + 1):]
        d = json.loads(record)
        return d
    except ValueError:
        None

def process_xml_buffer():
    global bfr
    try:
        start = bfr.index("<record>")
        end = bfr.index("</record>")
        record = bfr[start:(end + 9)]
        bfr = bfr[(end + 9):]
        return xml_to_dict(record)
    except ValueError:
        None
    
def format_schema(schema):
    formatted_schema = []
    for var in schema:
        field = bigquery.SchemaField(var["name"], var["type"], var["mode"])
        formatted_schema.append(field)
    return formatted_schema

def config_job():
    table_schema = (
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "time",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "duration",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "src_comp",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "src_port",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "dst_comp",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "dst_port",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "protocol",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "packet_count",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "byte_count",
            "type": "INTEGER",
            "mode": "REQUIRED"
        }
    )
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DEMLIMITED_JSON
    job_config.schema = format_schema(table_schema)
    return job_config

def main():
    global bfr
    url = "http://api-service/api/v1/stream/flows"
    params = {
        "limit": 1000000,
        "format": "xml"
    }
    job_config = config_job()
    client = bigquery.Client(project="docker-api")
    table = client.dataset("LANL").table("flows")
    records = []
    for chunk in requests.request("GET", url, params=params):
        bfr += chunk.decode("utf-8")
        data = []
        while data is not None:
            data = process_xml_buffer()
            if data:
                records.append(data)
                if len(records) == 1000:
                    job = client.load_table_from_json(records, table, job_config=job_config)
                    records = []

if __name__ == "__main__":
    main()