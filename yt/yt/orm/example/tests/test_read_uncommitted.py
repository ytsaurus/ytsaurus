from .conftest import ExampleTestEnvironment


class TestReadUncommitted:
    def test_create_book(self, example_env: ExampleTestEnvironment):
        transaction_id = example_env.client.start_transaction()
        book = example_env.create_book(spec={"name": "Some Guide"}, transaction_id=transaction_id)
        result = example_env.client.get_object("book", str(book), ["/spec/name"], transaction_id=transaction_id)
        assert "Some Guide" == result[0]

        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            timestamp_by_transaction_id=transaction_id,
        )
        assert result is None
        example_env.client.commit_transaction(transaction_id)

    def test_remove_book(self, example_env: ExampleTestEnvironment):
        book = example_env.create_book(spec={"name": "Some Book"})
        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object("book", str(book), transaction_id=transaction_id)

        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            timestamp_by_transaction_id=transaction_id,
        )
        assert "Some Book" == result[0]

        result = example_env.client.get_object(
            "book", str(book), ["/spec/name"], options={"skip_nonexistent": True}, transaction_id=transaction_id
        )
        assert result is None
        example_env.client.commit_transaction(transaction_id)

    def test_create_remove_book(self, example_env: ExampleTestEnvironment):
        transaction_id = example_env.client.start_transaction()
        book = example_env.create_book(spec={"name": "Some Guide"}, transaction_id=transaction_id)
        example_env.client.remove_object("book", str(book), transaction_id=transaction_id)
        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            transaction_id=transaction_id,
        )
        assert result is None

        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            timestamp_by_transaction_id=transaction_id,
        )
        assert result is None
        example_env.client.commit_transaction(transaction_id)

    def test_remove_create_book(self, example_env: ExampleTestEnvironment):
        publisher = example_env.create_publisher()
        book = example_env.create_book(spec={"name": "Some Old Book"}, publisher_id=publisher)

        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object("book", str(book), transaction_id=transaction_id)
        example_env.create_book(
            spec={"name": "Some New Book"}, book_id=book, publisher_id=publisher, transaction_id=transaction_id
        )
        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            transaction_id=transaction_id,
        )
        assert "Some New Book" == result[0]
        example_env.client.commit_transaction(transaction_id)

    def test_update_book(self, example_env: ExampleTestEnvironment):
        book = example_env.create_book(spec={"name": "Volume 1"})
        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "book", str(book), set_updates=[{"path": "/spec/name", "value": "Volume 2"}], transaction_id=transaction_id
        )

        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            transaction_id=transaction_id,
        )
        assert "Volume 2" == result[0]

        result = example_env.client.get_object(
            "book",
            str(book),
            ["/spec/name"],
            options={"skip_nonexistent": True},
            timestamp_by_transaction_id=transaction_id,
        )
        assert "Volume 1" == result[0]

        example_env.client.commit_transaction(transaction_id)
