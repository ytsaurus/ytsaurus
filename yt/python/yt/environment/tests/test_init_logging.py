import pytest

from yt.environment.api import LogLevel, LocalYtConfig
from yt.environment.configs_provider import init_logging, _init_logging


def _get_rules(config):
    return config.get("rules", [])


def _get_writer_names(config):
    return set(config.get("writers", {}).keys())


def _rule_min_levels(config):
    return [r["min_level"] for r in _get_rules(config)]


class TestLogLevel:
    def test_from_str(self):
        assert LogLevel.from_str("info") == LogLevel.INFO
        assert LogLevel.from_str("INFO") == LogLevel.INFO
        assert LogLevel.from_str("debug") == LogLevel.DEBUG
        assert LogLevel.from_str("warning") == LogLevel.WARNING
        assert LogLevel.from_str("trace") == LogLevel.TRACE

    def test_from_str_invalid(self):
        with pytest.raises(ValueError):
            LogLevel.from_str("unknown")

    def test_to_str(self):
        assert LogLevel.INFO.to_str() == "info"
        assert LogLevel.DEBUG.to_str() == "debug"
        assert LogLevel.WARNING.to_str() == "warning"
        assert LogLevel.TRACE.to_str() == "trace"

    def test_ordering(self):
        assert LogLevel.TRACE < LogLevel.DEBUG
        assert LogLevel.DEBUG < LogLevel.INFO
        assert LogLevel.INFO < LogLevel.WARNING
        assert LogLevel.WARNING < LogLevel.ERROR

    def test_min_max(self):
        assert min(LogLevel.DEBUG, LogLevel.INFO) == LogLevel.DEBUG
        assert max(LogLevel.DEBUG, LogLevel.INFO) == LogLevel.INFO
        assert min(LogLevel.WARNING, LogLevel.INFO) == LogLevel.INFO


class TestInitLogging:
    def test_info_level_default(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.INFO, use_name_in_writer_name=False)
        assert "info" in _get_writer_names(config)
        assert "debug" not in _get_writer_names(config)
        rules = _get_rules(config)
        assert any(r["min_level"] == "info" for r in rules)
        assert not any(r["min_level"] in ("debug", "trace") for r in rules)

    def test_warning_level(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.WARNING, use_name_in_writer_name=False)
        assert "warning" in _get_writer_names(config)
        assert "info" not in _get_writer_names(config)
        assert "debug" not in _get_writer_names(config)
        assert "warning" in _rule_min_levels(config)

    def test_debug_level(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.DEBUG, use_name_in_writer_name=False)
        # Main log should still be "info" (capped at INFO from below)
        assert "info" in _get_writer_names(config)
        # Debug log should exist
        assert "debug" in _get_writer_names(config)
        assert "debug" in _rule_min_levels(config)

    def test_trace_level(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.TRACE, use_name_in_writer_name=False)
        # Main log capped at info
        assert "info" in _get_writer_names(config)
        # Debug log uses trace min level
        assert "debug" in _get_writer_names(config)
        assert "trace" in _rule_min_levels(config)

    def test_writer_names_include_service_name(self):
        config = init_logging("/logs", "master-0", log_level=LogLevel.DEBUG)
        assert "info-master-0" in _get_writer_names(config)
        assert "debug-master-0" in _get_writer_names(config)

    def test_log_file_paths(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.DEBUG, use_name_in_writer_name=False)
        assert config["writers"]["info"]["file_name"] == "/logs/myservice.log"
        assert config["writers"]["debug"]["file_name"] == "/logs/myservice.debug.log"

    def test_log_file_paths_warning(self):
        config = init_logging("/logs", "myservice", log_level=LogLevel.WARNING, use_name_in_writer_name=False)
        assert config["writers"]["warning"]["file_name"] == "/logs/myservice.log"

    def test_rules_and_writers_consistent(self):
        for level in (LogLevel.TRACE, LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARNING):
            config = init_logging("/logs", "svc", log_level=level, use_name_in_writer_name=False)
            writer_names = _get_writer_names(config)
            for rule in _get_rules(config):
                for wname in rule["writers"]:
                    assert wname in writer_names, f"Rule references unknown writer '{wname}' for level {level}"


class TestLocalYtConfigLogLevel:
    """Tests for LocalYtConfig.log_level converter (accepts both LogLevel and strings)."""

    def test_default_is_info(self):
        config = LocalYtConfig()
        assert config.log_level == LogLevel.INFO

    def test_accepts_loglevel_enum(self):
        config = LocalYtConfig(log_level=LogLevel.DEBUG)
        assert config.log_level == LogLevel.DEBUG

    def test_accepts_string(self):
        config = LocalYtConfig(log_level="warning")
        assert config.log_level == LogLevel.WARNING

    def test_accepts_uppercase_string(self):
        config = LocalYtConfig(log_level="DEBUG")
        assert config.log_level == LogLevel.DEBUG

    def test_rejects_invalid_string(self):
        with pytest.raises(ValueError):
            LocalYtConfig(log_level="verbose")


class TestInitLoggingWithConfig:
    """Tests for _init_logging using LocalYtConfig, covering backwards compat."""

    def _call(self, yt_config, **kwargs):
        return _init_logging("/logs", "svc", {}, yt_config,
                             use_name_in_writer_name=False, **kwargs)

    # --- --log-level only (enable_debug_logging=False) ---

    def test_log_level_info_only(self):
        """--log-level info, no --enable-debug-logging"""
        cfg = LocalYtConfig(log_level=LogLevel.INFO, enable_debug_logging=False)
        config = self._call(cfg)
        assert "info" in _get_writer_names(config)
        assert "debug" not in _get_writer_names(config)
        assert "info" in _rule_min_levels(config)

    def test_log_level_debug_only(self):
        """--log-level debug, no --enable-debug-logging"""
        cfg = LocalYtConfig(log_level=LogLevel.DEBUG, enable_debug_logging=False)
        config = self._call(cfg)
        assert "info" in _get_writer_names(config)
        assert "debug" in _get_writer_names(config)
        assert "debug" in _rule_min_levels(config)

    def test_log_level_warning_only(self):
        """--log-level warning, no --enable-debug-logging"""
        cfg = LocalYtConfig(log_level=LogLevel.WARNING, enable_debug_logging=False)
        config = self._call(cfg)
        assert "warning" in _get_writer_names(config)
        assert "info" not in _get_writer_names(config)
        assert "debug" not in _get_writer_names(config)

    def test_log_level_trace_only(self):
        """--log-level trace, no --enable-debug-logging"""
        cfg = LocalYtConfig(log_level=LogLevel.TRACE, enable_debug_logging=False)
        config = self._call(cfg)
        assert "info" in _get_writer_names(config)
        assert "debug" in _get_writer_names(config)
        assert "trace" in _rule_min_levels(config)

    # --- --enable-debug-logging only (log_level stays INFO default) ---

    def test_enable_debug_logging_only(self):
        """--enable-debug-logging, no --log-level (deprecated path, like the old CLI)"""
        cfg = LocalYtConfig(log_level=LogLevel.INFO, enable_debug_logging=True)
        config = self._call(cfg)
        # backwards compat: enable_debug_logging=True overrides log_level to DEBUG
        assert "info" in _get_writer_names(config)
        assert "debug" in _get_writer_names(config)
        assert "debug" in _rule_min_levels(config)

    # --- both flags set ---

    def test_log_level_warning_with_enable_debug_logging(self):
        """--log-level warning --enable-debug-logging: enable_debug_logging wins (min wins)"""
        cfg = LocalYtConfig(log_level=LogLevel.WARNING, enable_debug_logging=True)
        config = self._call(cfg)
        assert "debug" in _get_writer_names(config)
        assert "debug" in _rule_min_levels(config)

    def test_log_level_debug_with_enable_debug_logging(self):
        """--log-level debug --enable-debug-logging: both say DEBUG, result is DEBUG"""
        cfg = LocalYtConfig(log_level=LogLevel.DEBUG, enable_debug_logging=True)
        config = self._call(cfg)
        assert "debug" in _get_writer_names(config)
        assert "debug" in _rule_min_levels(config)

    def test_log_level_trace_with_enable_debug_logging(self):
        """--log-level trace --enable-debug-logging: log_level wins (TRACE < DEBUG)"""
        cfg = LocalYtConfig(log_level=LogLevel.TRACE, enable_debug_logging=True)
        config = self._call(cfg)
        assert "debug" in _get_writer_names(config)
        assert "trace" in _rule_min_levels(config)

    # --- override via _init_logging's log_level parameter (used for controller agent) ---

    def test_explicit_log_level_overrides_config(self):
        """_init_logging log_level=TRACE forces trace even if config says INFO"""
        cfg = LocalYtConfig(log_level=LogLevel.INFO, enable_debug_logging=False)
        config = self._call(cfg, log_level=LogLevel.TRACE)
        assert "debug" in _get_writer_names(config)
        assert "trace" in _rule_min_levels(config)

    def test_explicit_log_level_does_not_raise_level(self):
        """_init_logging log_level=INFO does not raise a TRACE config to INFO"""
        cfg = LocalYtConfig(log_level=LogLevel.TRACE, enable_debug_logging=False)
        config = self._call(cfg, log_level=LogLevel.INFO)
        # min(TRACE, INFO) = TRACE, so trace should still be in effect
        assert "trace" in _rule_min_levels(config)
