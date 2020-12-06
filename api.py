from flask import Flask, request, Response
import sqlite3
import os
import json

basedir = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__)

def format_record(record, fmt):
    if fmt == "json":
        return json.dumps({
            "id": record[0],
            "time": record[1],
            "duration": record[2],
            "src_comp": record[3],
            "src_port": record[4],
            "dst_comp": record[5],
            "dst_port": record[6],
            "protocol": record[7],
            "packet_count": record[8],
            "byte_count": record[9],
        })
    else:
        s = """
        <record>
            <id>%d</id>
            <time>%d</time>
            <duration>%d</duration
            <src_comp>%s</src_comp>
            <src_port>%s</src_port>
            <dst_comp>%s</dst_comp>
            <dst_port>%s</dst_port>
            <protocol>%d</protocol>
            <packet_count>%d</packet_count>
            <byte_count>%d</byte_count>
        </record>
        """ % record
        return s.strip().replace("\n", "").replace(" ", "")

def stream(conn, records, fmt):
    try:
        prev_record = next(records)
    except StopIteration:
        if fmt == "json":
            yield "{}"
        else:
            yield "<?xml version='1.0' encoding='UTF-8'?><records/>"
    
    if fmt == "xml":
        yield "<?xml version='1.0' encoding='UTF-8'?><records>"
    for record in records:
        yield format_record(prev_record, fmt) + "\n"
        prev_record = record
    
    conn.close()
    yield format_record(prev_record, fmt)
    if fmt == "xml":
        yield "</records>"

@app.route("/api/v1/stream/flows", methods=["GET"])
def query_flows():
    conn = sqlite3.connect("http://database")
    c = conn.cursor()

    start = request.args.get("scanned_after", 0)
    end = request.args.get("scanned_before", 5011300)
    args = (start, end)
    q = "SELECT * FROM flows WHERE time > ? AND time <= ?"

    limit = request.args.get("limit", None)
    if limit is not None:
        q += " LIMIT ?"
        args = (start, end, limit)
    
    c.execute(q, args)
    fmt = request.args.get("format", "json")
    content_type = f"application/{fmt}"
    return Response(stream(conn, c, fmt), mimetype=content_type)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)

