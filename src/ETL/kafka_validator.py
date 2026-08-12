import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from config.database_config import get_kafka_config


RAW_TOPIC = "BondyPJDE_raw"
VALIDATED_TOPIC = "BondyPJDE_validated"
DLQ_TOPIC = "BondyPJDE_dlq"

COMMON_REQUIRED_FIELDS = ["entity", "log_id", "state", "log_timestamp"]
USER_REQUIRED_FIELDS = ["user_id", "login", "gravatar_id", "url", "avatar_url"]
REPOSITORY_REQUIRED_FIELDS = ["repo_id", "name", "url"]
VALID_ENTITIES = {"users", "repositories"}
VALID_STATES = {"INSERT", "UPDATE", "DELETE"}


def validate_user(data):
    # kiểm tra các trường bắt buộc cho người dùng
    missing_fields = [
        field for field in USER_REQUIRED_FIELDS if field not in data
    ]
    if missing_fields:
        return f"Missing required user fields: {', '.join(missing_fields)}"

    if type(data["user_id"]) is not int:
        return "user_id must be a non-null integer"

    if not isinstance(data["login"], str):
        return "login must be a non-null string"

    for field in ("gravatar_id", "url", "avatar_url"):
        if data[field] is not None and not isinstance(data[field], str):
            return f"{field} must be a string or null"

    return None


def validate_repository(data):
    # Kiểm tra các trường bắt buộc cho repository
    missing_fields = [
        field for field in REPOSITORY_REQUIRED_FIELDS if field not in data
    ]
    if missing_fields:
        return f"Missing required repository fields: {', '.join(missing_fields)}"

    if type(data["repo_id"]) is not int:
        return "repo_id must be a non-null integer"

    if not isinstance(data["name"], str):
        return "name must be a non-null string"

    if data["url"] is not None and not isinstance(data["url"], str):
        return "url must be a string or null"

    return None


def validate_message(original_message):
    try:
        # Chuyển đổi bytes sang string UTF-8
        message_text = original_message.decode("utf-8")
    except UnicodeDecodeError:
        readable_message = original_message.decode("utf-8", errors="replace")
        return readable_message, "Message is not valid UTF-8"

    try:
        data = json.loads(message_text)
    except json.JSONDecodeError as error:
        return message_text, f"Invalid JSON: {error.msg}"

    # nếu data không phải là json object, trả về lỗi
    if not isinstance(data, dict):
        return data, "Message must be a JSON object"

    missing_fields = [
        field for field in COMMON_REQUIRED_FIELDS if field not in data
    ]
    # nếu thiếu trường bắt buộc, trả về lỗi
    if missing_fields:
        return data, f"Missing required fields: {', '.join(missing_fields)}"
    # nếu entity không hợp lệ, trả về lỗi
    if (
        not isinstance(data["entity"], str)
        or data["entity"] not in VALID_ENTITIES
    ):
        return data, "entity must be users or repositories"
    # nếu log_id không phải là int, trả về lỗi
    if type(data["log_id"]) is not int:
        return data, "log_id must be a non-null integer"
    # nếu state không hợp lệ, trả về lỗi
    if (
        not isinstance(data["state"], str)
        or data["state"] not in VALID_STATES
    ):
        return data, "state must be INSERT, UPDATE, or DELETE"
    # nếu log_timestamp không phải là string, trả về lỗi
    if not isinstance(data["log_timestamp"], str):
        return data, "log_timestamp must be a non-null string"

    if data["entity"] == "users":
        error = validate_user(data)
    else:
        error = validate_repository(data)

    return data, error


def validate_log_sequence(entity, current_log_id, last_log_id):
    if last_log_id is None:
        return current_log_id
    # nếu log_id hiện tại lớn hơn log_id cuối cùng + 1
    if current_log_id > last_log_id + 1:
        logging.warning(
            "%s log_id gap detected. Possible missing range: %s-%s",
            entity,
            last_log_id + 1,
            current_log_id - 1,
        )
        return current_log_id

    if current_log_id <= last_log_id:
        logging.warning(
            "%s duplicate or old message: current log_id=%s, last log_id=%s",
            entity,
            current_log_id,
            last_log_id,
        )
        return last_log_id

    return current_log_id


def kafka_validator():
    config_kafka = get_kafka_config()
    
    # Khởi tạo Kafka consumer và producer
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

    last_log_ids = {
        "users": None,
        "repositories": None,
    }
    logging.info("Validator is listening to topic %s", RAW_TOPIC)

    try:
        for message in consumer:
            data, error = validate_message(message.value)

            # Theo dõi sequence riêng cho từng entity.
            if isinstance(data, dict):
                entity = data.get("entity")
                log_id = data.get("log_id")
                if entity in last_log_ids and type(log_id) is int:
                    last_log_ids[entity] = validate_log_sequence(
                        entity,
                        log_id,
                        last_log_ids[entity],
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

            # Đợi cho đến khi tin nhắn được gửi thành công hoặc timeout
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
