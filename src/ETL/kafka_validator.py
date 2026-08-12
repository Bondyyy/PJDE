import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from config.database_config import get_kafka_config


RAW_TOPIC = "BondyPJDE_raw"
VALIDATED_TOPIC = "BondyPJDE_validated"
DLQ_TOPIC = "BondyPJDE_dlq"

REQUIRED_FIELDS = [
    "log_id",
    "user_id",
    "login",
    "gravatar_id",
    "url",
    "avatar_url",
    "state",
    "log_timestamp",
]
VALID_STATES = {"INSERT", "UPDATE", "DELETE"}


def validate_message(original_message):
    try:
        # Kiểm tra xem có phải utf8 không
        message_text = original_message.decode("utf-8")
    except UnicodeDecodeError:
        readable_message = original_message.decode("utf-8", errors="replace")
        return readable_message, "Message is not valid UTF-8"

    try:
        # Kiểm tra xem có phải JSON không
        data = json.loads(message_text)
    except json.JSONDecodeError as error:
        return message_text, f"Invalid JSON: {error.msg}"

    #bắt buộc là json object
    if not isinstance(data, dict):
        return data, "Message must be a JSON object"

    #kiểm tra các trường bắt buộc và kiểu dữ liệu
    missing_fields = [field for field in REQUIRED_FIELDS if field not in data]
    if missing_fields:
        return data, f"Missing required fields: {', '.join(missing_fields)}"

    if type(data["log_id"]) is not int:
        return data, "log_id must be a non-null integer"

    if type(data["user_id"]) is not int:
        return data, "user_id must be a non-null integer"

    if not isinstance(data["login"], str):
        return data, "login must be a non-null string"

    if not isinstance(data["state"], str) or data["state"] not in VALID_STATES:
        return data, "state must be INSERT, UPDATE, or DELETE"

    if not isinstance(data["log_timestamp"], str):
        return data, "log_timestamp must be a non-null string"

    # These fields are optional values, but must follow the source schema.
    for field in ("gravatar_id", "url", "avatar_url"):
        if data[field] is not None and not isinstance(data[field], str):
            return data, f"{field} must be a string or null"

    return data, None


def validate_log_sequence(current_log_id, last_log_id):
    # Kiểm tra xem log_id có tăng dần không
    if last_log_id is None:
        return current_log_id

    if current_log_id > last_log_id + 1:
        logging.warning(
            "log_id gap detected. Possible missing range: %s-%s",
            last_log_id + 1,
            current_log_id - 1,
        )
        return current_log_id

    if current_log_id <= last_log_id:
        logging.warning(
            "Duplicate or old message detected: current log_id=%s, last log_id=%s",
            current_log_id,
            last_log_id,
        )
        return last_log_id

    return current_log_id


def kafka_validator():
    config_kafka = get_kafka_config()

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=config_kafka["bootstrap_servers"],
        group_id="BondyPJDE_validator",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=config_kafka["bootstrap_servers"],
        value_serializer=lambda x: json.dumps(
            x,
            default=str
        ).encode("utf-8")
    )

    last_log_id = None
    logging.info("Validator is listening to topic %s", RAW_TOPIC)

    try:
        for message in consumer:
            data, error = validate_message(message.value)

            # Sequence can be checked whenever a usable log_id is available.
            if isinstance(data, dict) and type(data.get("log_id")) is int:
                last_log_id = validate_log_sequence(
                    data["log_id"],
                    last_log_id,
                )

            if error is None:
                future = producer.send(VALIDATED_TOPIC, data)
                destination = VALIDATED_TOPIC
            else:
                future = producer.send(
                    DLQ_TOPIC,
                    {"data": data, "error": error},
                )
                destination = DLQ_TOPIC

            # Wait for Kafka acknowledgement before committing this offset.
            future.get(timeout=30)
            consumer.commit()
            logging.info(
                "Processed raw offset %s -> %s",
                message.offset,
                destination,
            )
    finally:
        producer.close()
        consumer.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        kafka_validator()
    except KeyboardInterrupt:
        logging.info("Kafka validator stopped")


if __name__ == "__main__":
    main()
