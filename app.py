import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from flask import Flask, jsonify, render_template, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

S3_BUCKET    = os.environ["S3_BUCKET_NAME"]
SQS_URL      = os.environ["SQS_QUEUE_URL"]
DYNAMO_TABLE = os.environ["DYNAMODB_TABLE_NAME"]
AWS_REGION   = os.environ.get("AWS_REGION", "ap-south-1")


def s3():
    return boto3.client("s3", region_name=AWS_REGION)

def sqs():
    return boto3.client("sqs", region_name=AWS_REGION)

def dynamo():
    return boto3.resource("dynamodb", region_name=AWS_REGION)


# ── UI ───────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ── Upload: S3 + SQS ─────────────────────────────────────────────────────────

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "no file provided"}), 400

    file_id  = uuid.uuid4().hex
    filename = file.filename or f"file-{file_id}"
    s3_key   = f"uploads/{file_id}/{filename}"
    ts       = datetime.now(timezone.utc).isoformat()

    # 1. Upload to S3
    s3().upload_fileobj(file, S3_BUCKET, s3_key)
    logger.info("S3 upload bucket=%s key=%s", S3_BUCKET, s3_key)

    # 2. Send message to SQS
    message = {
        "id":        file_id,
        "file_name": filename,
        "bucket":    S3_BUCKET,
        "s3_key":    s3_key,
        "timestamp": ts,
    }
    sqs().send_message(QueueUrl=SQS_URL, MessageBody=json.dumps(message))
    logger.info("SQS send file_id=%s", file_id)

    return jsonify({"file_id": file_id, "file_name": filename, "s3_key": s3_key, "status": "queued"})


# ── Process: SQS → DynamoDB ───────────────────────────────────────────────────

@app.route("/process", methods=["POST"])
def process():
    resp = sqs().receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
    )
    messages = resp.get("Messages", [])
    if not messages:
        return jsonify({"processed": 0, "message": "queue is empty"})

    table = dynamo().Table(DYNAMO_TABLE)
    count = 0
    for msg in messages:
        body = json.loads(msg["Body"])
        item = {
            "id":         body.get("id", uuid.uuid4().hex),
            "file_name":  body.get("file_name", ""),
            "bucket":     body.get("bucket", ""),
            "s3_key":     body.get("s3_key", ""),
            "status":     "received",
            "created_at": body.get("timestamp", datetime.now(timezone.utc).isoformat()),
        }
        table.put_item(Item=item)
        sqs().delete_message(QueueUrl=SQS_URL, ReceiptHandle=msg["ReceiptHandle"])
        logger.info("DynamoDB write id=%s file=%s", item["id"], item["file_name"])
        count += 1

    return jsonify({"processed": count})


# ── Logs: list DynamoDB records ───────────────────────────────────────────────

@app.route("/logs")
def logs():
    table  = dynamo().Table(DYNAMO_TABLE)
    result = table.scan()
    items  = sorted(result.get("Items", []), key=lambda x: x.get("created_at", ""), reverse=True)
    return jsonify({"logs": items})


# ── File: presigned URL for viewing ──────────────────────────────────────────

@app.route("/file/<file_id>")
def view_file(file_id):
    table  = dynamo().Table(DYNAMO_TABLE)
    result = table.get_item(Key={"id": file_id})
    item   = result.get("Item")
    if not item:
        return jsonify({"error": "not found"}), 404

    s3_key = item.get("s3_key")
    if not s3_key:
        return jsonify({"error": "no s3 key"}), 404

    url = s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=300,
    )
    from flask import redirect
    return redirect(url)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
