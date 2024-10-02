class TestClientBaseTypeHandler:
    def test_builtin_object(self, example_env, regular_user1):
        result = example_env.client.get_object(
            "user",
            str(regular_user1.id),
            ["/meta/ultimate_question_of_life"],
        )[0]
        assert result == 42

    def test_regular_object(self, example_env):
        author_key = example_env.client.create_object("author", request_meta_response=True)
        result = example_env.client.get_object(
            "author",
            str(author_key),
            ["/meta/ultimate_question_of_life"],
        )[0]
        assert result == 42

    def test_emit_opaque_as_entity(self, example_env):
        author_key = example_env.client.create_object("author", request_meta_response=True)
        result = example_env.client.get_object(
            "author",
            str(author_key),
            ["/meta"],
        )[0]
        assert "ultimate_question_of_life" not in result
