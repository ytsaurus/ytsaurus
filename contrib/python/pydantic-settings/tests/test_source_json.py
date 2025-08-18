"""
Test pydantic_settings.JsonConfigSettingsSource.
"""

import json
from pathlib import Path
from typing import Union

from pydantic import BaseModel

from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def test_repr() -> None:
    source = JsonConfigSettingsSource(BaseSettings, Path('config.json'))
    assert repr(source) == 'JsonConfigSettingsSource(json_file=config.json)'


def test_json_file(tmp_path):
    p = tmp_path / '.env'
    p.write_text(
        """
    {"foobar": "Hello", "nested": {"nested_field": "world!"}, "null_field": null}
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(json_file=p)
        foobar: str
        nested: Nested
        null_field: Union[str, None]

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


def test_json_no_file():
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(json_file=None)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.model_dump() == {}


def test_multiple_file_json(tmp_path):
    p5 = tmp_path / '.env.json5'
    p6 = tmp_path / '.env.json6'

    with open(p5, 'w') as f5:
        json.dump({'json5': 5}, f5)
    with open(p6, 'w') as f6:
        json.dump({'json6': 6}, f6)

    class Settings(BaseSettings):
        json5: int
        json6: int

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls, json_file=[p5, p6]),)

    s = Settings()
    assert s.model_dump() == {'json5': 5, 'json6': 6}
