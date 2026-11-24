import logging

from .codecs import DECODERS


def read_all_from_topic(driver, topic, consumer, commit=False, detailed=False):
    """Read messages within the partition offsets captured before opening the reader."""
    info = driver.topic_client.describe_topic(topic, include_stats=True)
    total_messages = 0
    pending_partitions = {}
    for partition in info.partitions:
        stats = partition.partition_stats
        total_messages += stats.partition_end - stats.partition_start
        if stats.partition_start < stats.partition_end:
            pending_partitions[partition.partition_id] = (stats.partition_start, stats.partition_end)
    logging.info("Starting reading (MessagesToRead: %d, Topic: %s, Consumer: %s)", total_messages, topic, consumer)
    got_messages = dict()
    message_count = 0

    with driver.topic_client.reader(topic, consumer, decoders=DECODERS) as reader:
        logging.info("Created reader (Topic: %s, Consumer: %s)", topic, consumer)
        # New heartbeats in one partition must not displace the captured tail of another partition.
        while pending_partitions:
            message = reader.receive_message()
            partition_id = message.partition_id
            bounds = pending_partitions.get(partition_id)
            if bounds is None:
                continue
            start, end = bounds
            if message.offset >= end - 1:
                del pending_partitions[partition_id]
            if not start <= message.offset < end:
                continue
            if partition_id in got_messages:
                got_messages[partition_id].append(message)
            else:
                got_messages[partition_id] = [message]

            logging.info(
                "Read message (MessageNumber: %d, MessagesToRead: %d, PartitionId: %d, MessageOffset: %d, Topic: %s)",
                message_count,
                total_messages,
                partition_id,
                message.offset,
                topic,
            )
            message_count += 1

        logging.info("Finished reading (MessageCount: %d, Topic: %s)", message_count, topic)

        if commit:
            for messages in got_messages.values():
                for message in messages:
                    reader.commit_with_ack(message)
            logging.info("Finished committing messages that were recently read")

    if not detailed:
        data = []
        for messages in got_messages.values():
            for message in messages:
                data.append(message.data)
        return data
    else:
        return got_messages
