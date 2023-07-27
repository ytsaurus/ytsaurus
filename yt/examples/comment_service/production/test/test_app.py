from lib.create_tables import create_database
from lib.run_application import api, log_request_start
from mapreduce.yt.python.yt_stuff import YtStuff, YtConfig
from flask import Flask, g, request
from random import randint
import pytest
import json


def create_test_database(yt_stuff):
    client = yt_stuff.get_yt_client()
    path = "//home"

    def create_test_table(name, schema):
        table_path = "{}/{}".format(path, name)
        client.create("table", table_path, attributes={
            "schema": schema, "dynamic": True,
            "in_memory_mode": "compressed",
        })
        client.mount_table(table_path, sync=True)

    create_database(create_test_table)


def make_test_app(yt_stuff):
    app = Flask(__name__)
    app.register_blueprint(api)

    @app.before_request
    def before_request():
        g.client = yt_stuff.get_yt_client()
        g.table_path = "//home"
        g.request_id = randint(0, 1000000)
        log_request_start(request, g.request_id)

    return app


@pytest.fixture(scope="module")
def app():
    config = YtConfig(wait_tablet_cell_initialization=True)
    yt_stuff = YtStuff(config)

    yt_stuff.start_local_yt()
    create_test_database(yt_stuff)
    yield make_test_app(yt_stuff)


def test_good_requests(app):
    with app.test_client() as c:
        for i in range(5):
            rv = c.post("/post_comment/", data={"user": "test_user", "content": "content"})
            assert rv.status_code == 201

        rv = c.get("/user_comments/?user=test_user&limit=5")
        assert rv.status_code == 200
        assert len(json.loads(rv.data)) == 5

        rv = c.get("/last_topics/?limit=5")
        assert rv.status_code == 200
        assert len(json.loads(rv.data)) == 5


def test_bad_requests(app):
    with app.test_client() as c:
        rv = c.post("/post_comment/", data={
            "topic_id": "1000-1000-1000-1000", "parent_id": "1000-1000-1000-1000", "parent_path": "0",
            "user": "test_user", "content": "content",
        })
        assert rv.status_code == 404
        assert "error" in json.loads(rv.data)

        rv = c.post("/post_comment/", data={"user": "test_user"})
        assert rv.status_code == 400
        assert "error" in json.loads(rv.data)

        rv = c.get("/user_comments/?limit=5")
        assert rv.status_code == 400
        assert "error" in json.loads(rv.data)

        rv = c.get("/topic_comments/?parent_id=1000-1000-1000-1000")
        assert rv.status_code == 400
        assert "error" in json.loads(rv.data)
