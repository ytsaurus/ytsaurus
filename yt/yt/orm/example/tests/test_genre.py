class TestGenre:
    def test_genre_substring(self, example_env):
        genre_name = "Horrors"
        genre = example_env.create_genre(genre_name)

        for name_substr in ["Hor", "ror", "Adven"]:
            result = example_env.client.get_object(
                "genre",
                genre,
                selectors=["/spec/is_name_substring/{}".format(name_substr)]
            )[0]
            assert result == (name_substr in genre_name)

    def test_filter(self, example_env):
        genre_keys = []
        filtered_keys = [[], []]
        substrings = ["r", "ram"]
        result_lens = [60, 20]
        genres = ["Horror", "Drama", "Thriller", "Comedy", "Action"]

        for i in range(100):
            genre_keys.append(
                example_env.create_genre(genres[i % 5] + str(i))
            )
            if i % 5 < 3:
                filtered_keys[0].append(genre_keys[-1])
                if i % 5 == 1:
                    filtered_keys[1].append(genre_keys[-1])

        for i in range(2):
            select_result = example_env.client.select_objects(
                "genre",
                filter="[/spec/is_name_substring/{}]".format(substrings[i]),
                selectors=["/meta/key"],
                common_options=dict(fetch_performance_statistics=True),
                enable_structured_response=True,
            )
            results = select_result["results"]

            assert result_lens[i] == len(results)
            assert list(sorted([result[0]["value"] for result in results])) == list(sorted(filtered_keys[i]))
