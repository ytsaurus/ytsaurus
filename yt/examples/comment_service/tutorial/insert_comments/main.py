# -*- coding: utf-8 -*-
import yt.wrapper as yt
import os

# В параметре comments_info передаются значения, соответствующие колонкам в таблице topic_comments
# В параметре client передается клиент, через которого происходит обращение к кластеру
# В параметре table_path передается путь к директории, в которой лежат таблицы
def insert_comments(comments_info, client, table_path):
    # Необходимо указать параметр aggregate=True, без него запись в агрегирующие колонки
    # будет происходить так же, как и в обычные
    client.insert_rows("{}/topic_comments".format(table_path), comments_info, aggregate=True)
    # При добавлении в таблицу user_comments колонку hash задавать не требуется, она вычислится сама
    client.insert_rows(
        "{}/user_comments".format(table_path), [{
            "user":  comment_info["user"],
            "topic_id":  comment_info["topic_id"],
            "parent_path": comment_info["parent_path"],
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
    )
    client.insert_rows(
        "{}/topics".format(table_path), [{
            "topic_id":  comment_info["topic_id"],
            "comment_count": 1,
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
        aggregate=True,
    )


def main():
    # Параметры передаются в переменных окружения
    table_path = os.environ["TABLE_PATH"]
    cluster_name = os.environ["CLUSTER"]
    client = yt.YtClient(cluster_name, config={"backend": "rpc"})

    # Добавление комментария (по форме это первый комментарий в топике)
    insert_comments([{
        "topic_id": "1dd64501-4131025-562332a3-40507acc",
        "comment_id": 0,
        "parent_id": 0,
        "parent_path": "0",
        "user": "abc",
        "creation_time": 100000,
        "update_time": 100000,
        "content": "Some comment text",
        "views_count": 0,
        "deleted": False,
    }], client, table_path)


if __name__ == "__main__":
    main()
