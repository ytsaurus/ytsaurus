# -*- coding: utf-8 -*-
import yt.wrapper as yt
import json


def get_last_user_comments(client, table_path, user, limit=10, from_time=0):
    try:
        # В качестве основной таблицы используется user_comments, позволяющая фильтровать записи по полю user
        # Через join подключается дополнительная таблица topic_comments,
        # которая используется для получения полной информации про комментарий
        comments_info = list(client.select_rows(
            """
            topic_comments.topic_id as topic_id,
            topic_comments.comment_id as comment_id,
            topic_comments.content as content,
            topic_comments.user as user,
            topic_comments.views_count as views_count,
            topic_comments.update_time as update_time
            from [{0}/user_comments] as user_comments join [{0}/topic_comments] as topic_comments
            on (user_comments.topic_id, user_comments.parent_path) =
            (topic_comments.topic_id, topic_comments.parent_path)
            where user_comments.user = '{1}' and user_comments.update_time >= {2}
            order by user_comments.update_time desc
            limit {3}""".format(table_path, user, from_time, limit)
        ))
        return json.dumps(comments_info, indent=4)
    except yt.YtResponseError as error:
        return json.dumps({"error": str(error)})
