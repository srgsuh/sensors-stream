SNS_EVENT_SOURCE = "aws:sns"
SQS_EVENT_SOURCE = "aws:sqs"

def get_records(event: dict) -> list[dict]:
    return event.get("Records", [])

def get_message_id_from_record(record: dict) -> str | None:
    event_source = record.get("EventSource")
    if event_source == SNS_EVENT_SOURCE:
        return record.get("Sns", {}).get("MessageId")
    elif event_source == SQS_EVENT_SOURCE:
        return record.get("MessageId")
    
    raise ValueError(f"Unsupported event source: {event_source}")

def get_message_from_record(record: dict) -> str | None:
    event_source = record.get("EventSource")
    if event_source == SNS_EVENT_SOURCE:
        return record.get("Sns", {}).get("Message")
    elif event_source == SQS_EVENT_SOURCE:
        return record.get("Body")

    raise ValueError(f"Unsupported event source: {event_source}")