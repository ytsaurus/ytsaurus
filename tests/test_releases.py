from . import templates

from yp.common import YtResponseError

import pytest

@pytest.mark.usefixtures("yp_env")
class TestReleases(object):

    def test_update_spec(self, yp_env):
        templates.update_spec_test_template(
            yp_env.yp_client,
            "release",
            initial_spec={
                'docker': {
                    'image_name': 'some_image_name',
                    'image_tag': 'some_image_tag',
                    'image_hash': 'some_image_hash',
                    'release_type': 'stable'
                },
                "description": "desc"
            },
            update_path="/spec/description",
            update_value="desc1"
        )

    def test_release_id_validation_failure(self, yp_env):
        yp_client = yp_env.yp_client
        valid_spec = {
            'docker': {
                'image_name': 'some_image_name',
                'image_tag': 'some_image_tag',
                'image_hash': 'some_image_hash',
                'release_type': 'stable'
            }
        }
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release",
                attributes={"meta": {"id": "inv*"}, "spec": valid_spec}
            )

    def test_release_sandbox_validation(self, yp_env):
        yp_client = yp_env.yp_client
        valid_resource = {
            'type': 'some_resource_type',
            'skynet_id': 'some-id'
        }
        invalid_resource_no_type = {'skynet_id': 'some-id'}

        invalid_spec_no_task_id = {
            'sandbox': {
                'task_type': 'some_task_type',
                'release_type': 'stable',
                'resources': [valid_resource]
            }
        }

        invalid_spec_no_resources = {
            'sandbox': {
                'task_id': 'some_task_id',
                'task_type': 'some_task_type',
                'release_type': 'stable',
                'resources': []
            }
        }

        invalid_spec_invalid_resource = {
            'sandbox': {
                'task_id': 'some_task_id',
                'task_type': 'some_task_type',
                'release_type': 'stable',
                'resources': [invalid_resource_no_type]
            }
        }

        valid_spec = {
            'sandbox': {
                'task_id': 'some_task_id',
                'task_type': 'some_task_type',
                'release_type': 'stable',
                'resources': [valid_resource]
            }
        }

        # Case 1: no task_id in sandbox release
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_no_task_id}
            )

        # Case 2: no resouces in sandbox release
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_no_resources}
            )

        # Case 3: no resouce type in sandbox release
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_invalid_resource}
            )

        # Case 4: all is ok
        release_id = yp_client.create_object(
            "release",
            attributes={"meta": {"id": "val"}, "spec": valid_spec}
        )

        # Case 5: task_id cannot be empty
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "release",
                release_id,
                set_updates=[{"path": "/spec/sandbox/task_id", "value": ""}]
            )

    def test_release_docker_validation(self, yp_env):
        yp_client = yp_env.yp_client
        valid_spec = {
            'docker': {
                'image_name': 'some_image_name',
                'image_tag': 'some_image_tag',
                'image_hash': 'some_image_hash',
                'release_type': 'stable'
            }
        }
        invalid_spec_no_image_name = {
            'docker': {
                'image_tag': 'some_image_tag',
                'image_hash': 'some_image_hash',
                'release_type': 'stable'
            }
        }

        # Case 1: no image_name in docker release
        with pytest.raises(YtResponseError):
            yp_client.create_object(
                "release",
                attributes={"meta": {"id": "val"}, "spec": invalid_spec_no_image_name}
            )

        # Case 2: all is ok
        release_id = yp_client.create_object(
            "release",
            attributes={"meta": {"id": "val"}, "spec": valid_spec}
        )

        # Case 3: image_name cannot be empty
        with pytest.raises(YtResponseError):
            yp_client.update_object(
                "release",
                release_id,
                set_updates=[{"path": "/spec/docker/image_name", "value": ""}]
            )
