from yt.testlib import authors

from yt.wrapper.docker_yandex import _Image, prepare_image_on_cluster, _TransportFailed

import pytest
from unittest.mock import patch


class TestImage:
    @authors("danila-shutov")
    @pytest.mark.parametrize("image_str,expected_registry,expected_image,expected_tag", [
        ("registry.yandex.net/my/image:v1.0", "registry.yandex.net", "my/image", "v1.0"),
        ("ubuntu:20.04", "registry.yandex.net", "ubuntu", "20.04"),
        ("nginx", "registry.yandex.net", "nginx", "latest"),
        ("library/ubuntu:latest", "registry.yandex.net", "library/ubuntu", "latest"),
    ])
    def test_from_str(self, image_str, expected_registry, expected_image, expected_tag):
        img = _Image.from_str(image_str)
        assert img.registry == expected_registry
        assert img.image == expected_image
        assert img.tag == expected_tag

    @authors("danila-shutov")
    @pytest.mark.parametrize("registry,image,tag,expected_str", [
        ("reg", "img", "tag", "reg/img:tag"),
        ("reg", "prefix/img", "latest", "reg/prefix/img:latest"),
    ])
    def test_str_representation(self, registry, image, tag, expected_str):
        img = _Image(registry=registry, image=image, tag=tag)
        assert str(img) == expected_str

    @authors("danila-shutov")
    @pytest.mark.parametrize(
        "invalid_input",
        [
            "",
            ":",
            "/",
            "//",
            "registry/",
            ":tag",
            "registry/image:",
        ],
    )
    def test_invalid_formats(self, invalid_input):
        with pytest.raises(ValueError):
            _Image.from_str(invalid_input)


class TestPrepareImageOnCluster:
    @authors("danila-shutov")
    @pytest.mark.parametrize(
        "image,expected",
        [
            ("registry.test.yt.yandex-team.ru/library/nginx:latest", "registry.test.yt.yandex-team.ru/library/nginx:latest"),
            ("registry.yandex.net/my/image:v1", "registry.test.yt.yandex-team.ru/home/qe/nirvana/production/docker_images/my/image:v1"),
            ("random_image:123", "random_image:123")
        ],
    )
    def test_prepare_image(self, image, expected):
        with patch("yt.wrapper.docker_yandex._Transporter"):
            result = prepare_image_on_cluster(
                image=image,
                yt_cluster_name="test",
                yt_token="not_a_token",
            )
            assert result == expected

    @authors("danila-shutov")
    def test_transport_failure(self):
        with patch("yt.wrapper.docker_yandex._Transporter") as mock_transporter:
            mock_transporter.side_effect = _TransportFailed("test error")

            with pytest.raises(_TransportFailed):
                _ = prepare_image_on_cluster(
                    image="registry.yandex.net/my/image:v1",
                    yt_cluster_name="test",
                    yt_token="not_a_token",
                )
