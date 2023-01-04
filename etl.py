import localstack_client.session as boto3
import json
from datetime import datetime
from encoding import load_key, encrypt, decrypt, public_file, private_file
from sqlalchemy import create_engine


def load_from_sqs():
    sqs = boto3.resource("sqs")
    queue_name = "login-queue"
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    max_queue_messages = 10
    message_bodies = []

    batch_size = 0
    while True:
        messages_to_delete = []
        for message in queue.receive_messages(MaxNumberOfMessages=max_queue_messages):
            # process message body
            body = json.loads(message.body)
            message_bodies.append(body)
            # add message to delete
            messages_to_delete.append(
                {"Id": message.message_id, "ReceiptHandle": message.receipt_handle}
            )

        # if you don't receive any notifications then messages_to_delete list will be empty
        batch_size += max_queue_messages
        if len(messages_to_delete) == 0 or batch_size >= chunk_size:
            print("Finished parsing")
            break

        else:
            # delete messages to remove them from SQS queue handle any errors
            print(f"Processed {max_queue_messages} messages")
            delete_response = queue.delete_messages(Entries=messages_to_delete)
    return message_bodies


def write_to_psql(message_bodies):

    # process message bodies to insert values
    print(f"current batch size is {len(message_bodies)}")
    today = str(datetime.now().date())
    lst = []
    for i in message_bodies:
        try:
            val = [
                i["user_id"],
                i["device_type"],
                encrypt(i["ip"], public),
                encrypt(i["device_id"], public),
                i["locale"],
                int(i["app_version"].replace(".", ""))
                if i["app_version"] is not None
                else "",
                today,
            ]
            val = tuple(["" if k is None else k for k in val])
            lst.append(str(val))
        except:
            print("One error message detected and will not write to psql", i)

    # write to psql
    db_name = "postgres"
    db_user = "postgres"
    db_pass = "postgres"
    db_host = "localhost"
    db_port = "5432"

    # Connecto to the database
    db_string = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        db_user, db_pass, db_host, db_port, db_name
    )
    conn = create_engine(db_string)

    insert_values = ",".join(lst)
    conn.execute(f"insert into user_logins values {insert_values};")


if __name__ == "__main__":
    chunk_size = 100  # you can define size of chunk write to psql here
    public, private = load_key(public_file, private_file)
    message_bodies = load_from_sqs()
    write_to_psql(message_bodies)
