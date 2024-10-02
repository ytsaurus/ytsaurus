class TestStorageType:
    def test_storage_type(self, example_env):
        id = example_env.create_object(
            "cat",
            spec={
                "mood": 1,
                "mood_in_previous_days": [2, "playful"],
                "health_condition": "healthy",
                "health_condition_in_previous_days": [1, "ill"],
                "eye_color_with_default_yson_storage_type": "blue",
                "eye_color_of_friend_cats_with_default_yson_storage_type": [1, "orange"],
                "friend_cats_to_their_eyes_color": {"Oliver": 1, "Bella": "hazel", "Simba": 3},
            },
        )
        rows = example_env.yt_client.lookup_rows(
            "//home/example/db/cats",
            [{"meta.id": int(id)}],
            column_names=[
                "spec.mood_state",
                "spec.health_condition",
                "spec.eye_color_with_default_yson_storage_type",
                "spec.etc"])
        row = [row for row in rows][0]

        assert row["spec.mood_state"] == 1
        assert row["spec.etc"]["mood_in_previous_days"] == [2, 4]
        assert row["spec.health_condition"] == "healthy"
        assert row["spec.etc"]["health_condition_in_previous_days"] == ["ill", "ill"]
        assert row["spec.eye_color_with_default_yson_storage_type"] == 3
        assert row["spec.etc"]["eye_color_of_friend_cats_with_default_yson_storage_type"] == [1, 4]
        assert row["spec.etc"]["friend_cats_to_their_eyes_color"] == {"Oliver": 1, "Bella": 1, "Simba": 3}
