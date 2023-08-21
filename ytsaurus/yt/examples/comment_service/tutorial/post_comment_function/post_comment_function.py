# -*- coding: utf-8 -*-
import yt.wrapper as yt
import os
import json
import time
from datetime import datetime


# Вспомогательная функция для получения количества комментариев в топике из таблицы topic_id
# В случае отсутствия заданного топика возвращается None
def get_topic_size(client, table_path, topic_id):
    topic_info = list(client.lookup_rows(
        "{}/topics".format(table_path), [{"topic_id": topic_id}],
    ))
    if not topic_info:
        return None
    assert(len(topic_info) == 1)
    return topic_info[0]["comment_count"]


def post_comment(client, table_path, user, content, topic_id=None, parent_id=None):
    # Необходимо обрабатывать исключение YtResponseError, возникающее если выполнить операцию не удается
    try:
        # Добавление комментария включает в себя несколько запросов разных типов,
        # поэтому требуется собрать их в одну транзакицию, чтобы обеспечить атомарность
        with client.Transaction(type="tablet"):
            new_topic = not topic_id
            if new_topic:
                topic_id = yt.common.generate_uuid()
                comment_id = 0
                parent_id = 0
                parent_path = "0"
            else:
                # Поле comment_id задается равным порядковому номеру комментария в топике
                # Этот номер совпадает с текущим размером топика
                comment_id = get_topic_size(topic_id)
                if not comment_id:
                    return json.dumps({"error": "There is no topic with id {}".format(topic_id)})

                parent_info = find_comment(topic_id, parent_path, client, table_path)
                if not parent_info:
                    return json.dumps({"error" : "There is no comment {} in topic {}".format(parent_id, topic_id)})
                parent_id = parent_info[0]["comment_id"]
                parent_path = "{}/{}".format(parent_path, comment_id)

            creation_time = int(time.mktime(datetime.now().timetuple()))
            insert_comments([{
                "topic_id": topic_id,
                "comment_id": comment_id,
                "parent_id": parent_id,
                "parent_path": parent_path,
                "user": user,
                "creation_time": creation_time,
                "update_time": creation_time,
                "content": content,
                "views_count": 0,
                "deleted": False,
            }], client, table_path)

            result = {"comment_id" : comment_id, "new_topic" : new_topic, "parent_path": parent_path}
            if new_topic:
                result["topic_id"] = topic_id
            return json.dumps(result)
    except yt.YtResponseError as error:
        # У yt.YtResponseError определен метод __str__, возвращающий подробное сообщение об ошибке
        json.dumps({"error" : str(error)})

