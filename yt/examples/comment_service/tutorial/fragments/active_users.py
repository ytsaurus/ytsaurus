from yql.api.v1.client import YqlClient

# Форма для метода active_users
class ActiveUsersForm(LastTopicsForm):
    limit = IntegerField("limit", [validators.Optional()], default=10)


# Реализация метода active_users
@api.route("/active_users/", methods=["GET"])
def get_active_users():
    form = ActiveUsersForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response
    limit = form.limit.data

    # К реплицированной таблице нельзя применять map_reduce операции,
    # так что вместо этого используется одна из реплик
    req = g.yql_client.query(
        """
        select user, count(user) as activity from [{}/user_comments_replica]
         group by user order by activity desc limit {}
        """.format("//home/dt_tutorial/comment_service", 3)
    )
    req.run()

    result = []
    for table in req.get_results():
        table.fetch_full_data()

        for row in table.rows:
            cells = list(row)
            result.append({"user": cells[0], "activity": cells[1]})

    return Response(json.dumps(result, indent=4), status=200, mimetype="application/json")
