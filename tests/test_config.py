"""Tests for Config validation and env var parsing."""

import pytest

from langsmith_migrator.utils.config import Config


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure migration-related env vars don't leak between tests."""
    for var in (
        "LANGSMITH_OLD_API_KEY",
        "LANGSMITH_NEW_API_KEY",
        "LANGSMITH_OLD_BASE_URL",
        "LANGSMITH_NEW_BASE_URL",
        "LANGSMITH_VERIFY_SSL",
        "MIGRATION_BATCH_SIZE",
        "MIGRATION_WORKERS",
        "MIGRATION_CHUNK_SIZE",
        "MIGRATION_RATE_LIMIT_DELAY",
        "MIGRATION_DRY_RUN",
        "MIGRATION_VERBOSE",
        "MIGRATION_SKIP_EXISTING",
        "MIGRATION_STREAM_EXAMPLES",
    ):
        monkeypatch.delenv(var, raising=False)


class TestConfigValidation:
    """Tests for Config.validate()."""

    def test_valid_config(self, sample_config):
        valid, errors = sample_config.validate()
        assert valid
        assert errors == []

    def test_missing_source_api_key(self):
        config = Config(dest_api_key="dest-key")
        assert config.source.api_key == ""
        valid, errors = config.validate()
        assert not valid
        assert any("Source API key" in e for e in errors)

    def test_missing_dest_api_key(self):
        config = Config(source_api_key="source-key")
        assert config.destination.api_key == ""
        valid, errors = config.validate()
        assert not valid
        assert any("Destination API key" in e for e in errors)

    def test_missing_both_api_keys(self):
        config = Config()
        valid, errors = config.validate()
        assert not valid
        assert len(errors) >= 2

    def test_invalid_source_url_no_scheme(self):
        config = Config(
            source_api_key="key",
            dest_api_key="key",
            source_url="example.com",
        )
        valid, errors = config.validate()
        assert not valid
        assert any("source" in e.lower() for e in errors)

    def test_invalid_dest_url_no_scheme(self):
        config = Config(
            source_api_key="key",
            dest_api_key="key",
            dest_url="example.com",
        )
        valid, errors = config.validate()
        assert not valid
        assert any("destination" in e.lower() for e in errors)

    def test_batch_size_negative(self):
        config = Config(
            source_api_key="key",
            dest_api_key="key",
            batch_size=-1,
        )
        valid, errors = config.validate()
        assert not valid

    def test_batch_size_too_large(self):
        config = Config(
            source_api_key="key",
            dest_api_key="key",
            batch_size=2000,
        )
        valid, errors = config.validate()
        assert not valid
        assert any("1000" in e for e in errors)

    def test_workers_too_many(self):
        config = Config(
            source_api_key="key",
            dest_api_key="key",
            concurrent_workers=15,
        )
        valid, errors = config.validate()
        assert not valid

    def test_chunk_size_zero(self):
        config = Config(source_api_key="key", dest_api_key="key")
        config.migration.chunk_size = 0
        valid, errors = config.validate()
        assert not valid
        assert any("Chunk size must be positive" in e for e in errors)

    def test_rate_limit_negative(self):
        config = Config(source_api_key="key", dest_api_key="key")
        config.migration.rate_limit_delay = -1.0
        valid, errors = config.validate()
        assert not valid
        assert any("Rate limit delay cannot be negative" in e for e in errors)

    def test_timeout_zero(self):
        config = Config(source_api_key="key", dest_api_key="key")
        config.source.timeout = 0
        valid, errors = config.validate()
        assert not valid
        assert any("Source timeout must be positive" in e for e in errors)

        config2 = Config(source_api_key="key", dest_api_key="key")
        config2.destination.timeout = -5
        valid2, errors2 = config2.validate()
        assert not valid2
        assert any("Destination timeout must be positive" in e for e in errors2)


class TestConfigEnvFallback:
    """Tests for environment variable fallback behavior."""

    def test_cli_args_override_env(self, monkeypatch):
        monkeypatch.setenv("LANGSMITH_OLD_API_KEY", "env-source-key")
        monkeypatch.setenv("LANGSMITH_NEW_API_KEY", "env-dest-key")
        config = Config(source_api_key="cli-source-key", dest_api_key="cli-dest-key")
        assert config.source.api_key == "cli-source-key"
        assert config.destination.api_key == "cli-dest-key"

    def test_env_used_when_cli_absent(self, monkeypatch):
        monkeypatch.setenv("LANGSMITH_OLD_API_KEY", "env-source-key")
        monkeypatch.setenv("LANGSMITH_NEW_API_KEY", "env-dest-key")
        config = Config()
        assert config.source.api_key == "env-source-key"
        assert config.destination.api_key == "env-dest-key"

    def test_batch_size_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_BATCH_SIZE", "200")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.batch_size == 200

    def test_invalid_batch_size_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_BATCH_SIZE", "not_a_number")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.batch_size == 100

    def test_workers_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_WORKERS", "8")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.concurrent_workers == 8

    def test_invalid_workers_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_WORKERS", "abc")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.concurrent_workers == 4

    def test_rate_limit_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_RATE_LIMIT_DELAY", "0.5")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.rate_limit_delay == 0.5

    def test_invalid_rate_limit_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_RATE_LIMIT_DELAY", "bad")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.rate_limit_delay == 0.1

    def test_ssl_verify_from_env(self, monkeypatch):
        monkeypatch.setenv("LANGSMITH_VERIFY_SSL", "false")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.source.verify_ssl is False

    def test_ssl_verify_cli_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LANGSMITH_VERIFY_SSL", "false")
        config = Config(source_api_key="k", dest_api_key="k", verify_ssl=True)
        assert config.source.verify_ssl is True

    def test_dry_run_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_DRY_RUN", "true")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.dry_run is True

    def test_verbose_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_VERBOSE", "true")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.verbose is True

    def test_skip_existing_from_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_SKIP_EXISTING", "true")
        config = Config(source_api_key="k", dest_api_key="k")
        assert config.migration.skip_existing is True

    def test_skip_existing_cli_overrides_env(self, monkeypatch):
        monkeypatch.setenv("MIGRATION_SKIP_EXISTING", "true")
        config = Config(source_api_key="k", dest_api_key="k", skip_existing=False)
        assert config.migration.skip_existing is False

    def test_batch_size_zero_explicit(self):
        """Config(batch_size=0) should set 0, not fall through to env/default."""
        config = Config(source_api_key="k", dest_api_key="k", batch_size=0)
        assert config.migration.batch_size == 0
