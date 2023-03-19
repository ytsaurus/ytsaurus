# Функция, формирующая поле в форме WTForms для guid топика, в котором валидируется корректность guid
# а также при необходимости это поле объявляется обязательным
# Поля в WTForms должны создаваться со списком специальных объектов-валидаторов
def get_guid_field(name, optional=True):
    opt_req_validator = validators.Optional()
    if not optional:
        opt_req_validator = validators.InputRequired(
            "Parameter {} must be specified".format(name)
        )
    return StringField(name, [opt_req_validator, validators.Regexp(
        "-".join("([0-9a-f]{1,8})" for _ in range(4)) + "$",
        message="Guid in parameter {} is incorrect".format(name),
    )])


# Функция, формирующая обязательное поле в форме WTForms
def get_required_field(name):
    return StringField(name, [validators.InputRequired(
        "Parameter {} must be specified".format(name)
    )])


# Форма для метода post_comment
# В WTForms каждая форма должна быть классом, унаследованным от класса wtforms.Form
# Поля формы задаются как атрибуты этого класса
class PostCommentForm(Form):
    topic_id = get_guid_field("topic_id")
    parent_path = StringField("parent_path")
    user = get_required_field("user")
    content = get_required_field("content")

    def validate(self):
        # Для валидации формы используется метод validate, по умолчанию пробегающий по валидаторам всех полей,
        # и записывающий возникающие ошибки в поле errors этих полей
        # В данном случае этот метод переопределяется, чтобы реализовать чуть более сложную логику:
        # если поле topic_id задано (то есть новый топик не создается), то должен быть задан параметр
        # parent_path чтобы знать, где создавать комментарий
        if not Form.validate(self):
            return False
        if self.topic_id.data and self.parent_path.data == "":
            self.parent_path.errors.append("Parameter parent_path must be specified")
            return False
        return True


# Вспомогательная функция для проверки валидности формы,
# и формирования полного сообщения об ошибке при необходимости
def check_parameters(form):
    if not form.validate():
        errors = [
            error for error_messages in form.errors.values()
            for error in error_messages
        ]
        return make_error_response("; ".join(errors), 400)


# Вспомогательная функция для получения количества комментариев в топике из таблицы topic_id
# В случае отсутствия заданного топика возвращается None
def get_topic_size(topic_id):
    topic_info = list(g.client.lookup_rows(
        "{}/topics".format(g.table_path), [{"topic_id": topic_id}],
    ))
    if not topic_info:
        return None
    assert(len(topic_info) == 1)
    return topic_info[0]["comment_count"]


# Реализация метода post_comment
# Для создания методов API используется декоратор route, в котором можно задается URL, запускающий функцию и методы запроса
@api.route("/post_comment/", methods=["POST"])
def post_comment():
    # В POST-запросе для передачи параметров используется объект типа flask.request.form
    form = PostCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        # Теперь параметры передаются как атрибуты специального объекта g,
        # который настраивается при создании приложения
        with g.client.Transaction(type="tablet"):
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
                    make_error_response("There is no topic with id {}".format(topic_id), 404)

                parent_info = find_comment(topic_id, parent_path)
                if not parent_info:
                    return make_error_response(
                        "There is no comment with parent_path '{}' in topic {}".format(
                            parent_path, topic_id,
                        ), 404,
                    )
                parent_id = parent_info[0]["comment_id"]
                parent_path = "{}/{}".format(parent_path, comment_id)

            creation_time = int(time.mktime(datetime.now().timetuple()))
            insert_comments([{
                "topic_id": topic_id,
                "comment_id": comment_id,
                "parent_id": parent_id,
                "parent_path": parent_path,
                "user": form.user.data,
                "creation_time": creation_time,
                "update_time": creation_time,
                "content": form.content.data,
                "views_count": 0,
                "deleted": False,
            }])
            log_request_completion("Added comment {} in topic {}".format(comment_id, topic_id), 201)

            # Теперь результатом работы метода является объект flask.Response, в котором помимо
            # самого json-результата задается HTTP код возврата и тип содержимого
            result = {"comment_id" : comment_id, "new_topic" : new_topic, "parent_path": parent_path}
            if new_topic:
                result["topic_id"] = topic_id
            return Response(json.dumps(result), status=201, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))



