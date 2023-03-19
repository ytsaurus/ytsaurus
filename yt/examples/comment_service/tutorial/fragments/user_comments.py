# Форма для метода user_comments
class UserCommentsForm(Form):
    user = get_required_field("user")
    from_time = IntegerField("from_time", [validators.Optional()], default=0)
    limit = IntegerField("limit", [validators.Optional()], default=10)


@api.route("/user_comments/", methods=["GET"])
def get_last_user_comments():
    # В GET-запросе для передачи параметров используется объект типа flask.request.args
    form = UserCommentsForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    user = form.user.data
    limit = form.limit.data
    from_time = form.from_time.data
    try:
        # В качестве основной таблицы используется таблица user_comments, записи в которой можно фильтровать по полю user
        # Через join подключается дополнительная таблица topic_comments,
        # которая используется для получения полной информации про комментарий
        comments_info = list(g.client.select_rows(
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
            limit {3}""".format(g.table_path, user, from_time, limit)
        ))
        log_request_completion("Returned {} comments by user {}".format(len(comments_info), user), 200)
        return Response(json.dumps(comments_info, indent=4), status=200, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))
