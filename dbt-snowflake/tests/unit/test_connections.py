import os
import pytest
from importlib import reload
from unittest.mock import Mock, patch
import multiprocessing
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt_common.exceptions import DbtConfigError
import dbt.adapters.snowflake.connections as connections
import dbt.adapters.events.logging


def test_connections_sets_logs_in_response_to_env_var(monkeypatch):
    """Test that setting the DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING environment variable happens on import"""
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    monkeypatch.setattr(os, "environ", {"DBT_SNOWFLAKE_CONNECTOR_DEBUG_LOGGING": "true"})
    reload(connections)

    assert log_mock.debug.call_count == 3
    assert log_mock.set_adapter_dependency_log_level.call_count == 3


def test_connections_does_not_set_logs_in_response_to_env_var(monkeypatch):
    log_mock = Mock()
    monkeypatch.setattr(dbt.adapters.events.logging, "AdapterLogger", Mock(return_value=log_mock))
    reload(connections)

    assert log_mock.debug.call_count == 0
    assert log_mock.set_adapter_dependency_log_level.call_count == 0


def test_connnections_credentials_replaces_underscores_with_hyphens():
    credentials = {
        "account": "account_id_with_underscores",
        "user": "user",
        "password": "password",
        "database": "database",
        "warehouse": "warehouse",
        "schema": "schema",
    }
    creds = connections.SnowflakeCredentials(**credentials)
    assert creds.account == "account-id-with-underscores"


def test_snowflake_oauth_expired_token_raises_error():
    credentials = {
        "account": "test_account",
        "user": "test_user",
        "authenticator": "oauth",
        "token": "expired_or_incorrect_token",
        "database": "database",
        "schema": "schema",
    }

    mp_context = multiprocessing.get_context("spawn")
    mock_credentials = connections.SnowflakeCredentials(**credentials)

    with patch.object(
        connections.SnowflakeConnectionManager,
        "open",
        side_effect=FailedToConnectError(
            "This error occurs when authentication has expired. "
            "Please reauth with your auth provider."
        ),
    ):

        adapter = connections.SnowflakeConnectionManager(mock_credentials, mp_context)

        with pytest.raises(FailedToConnectError):
            adapter.open()


def test_missing_account_raises_error():
    """Test that missing account (neither in profile nor environment) raises an error"""
    credentials = {
        "user": "test_user",
        "password": "test_password",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(Exception):  # Should raise error for missing account
            connections.SnowflakeCredentials(**credentials)


def test_missing_user_for_password_auth_raises_error():
    """Test that missing user for password auth raises an error (non-default authenticator)"""
    credentials = {
        "account": "test_account",
        "password": "test_password",
        "authenticator": "externalbrowser",  # Non-default authenticator
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(DbtConfigError, match=r"'user' is required for this authenticator"):
            connections.SnowflakeCredentials(**credentials)


def test_missing_user_for_oauth_auth_is_allowed():
    """Test that missing user for oauth auth is allowed"""
    credentials = {
        "account": "test_account",
        "authenticator": "oauth",
        "token": "test_token",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        creds = connections.SnowflakeCredentials(**credentials)
        assert creds.user is None  # Should be None for oauth auth


def test_missing_user_for_jwt_auth_is_allowed():
    """Test that missing user for jwt auth is allowed"""
    credentials = {
        "account": "test_account",
        "authenticator": "jwt",
        "token": "test_token",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        creds = connections.SnowflakeCredentials(**credentials)
        assert creds.user is None  # Should be None for jwt auth


def test_missing_account_for_default_authenticator_is_allowed():
    """Test that missing account is allowed only for default authenticator (None)"""
    credentials = {
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
        "role": "test_role",
        # No authenticator specified, so it defaults to None
    }

    with patch.dict(os.environ, {}, clear=True):
        creds = connections.SnowflakeCredentials(**credentials)
        assert creds.account is None  # Should be None when not provided
        assert creds.authenticator is None  # Default authenticator
        assert creds.unique_field == "default"  # Should use default for unique_field


def test_missing_account_for_oauth_raises_error():
    """Test that missing account for oauth raises an error"""
    credentials = {
        "authenticator": "oauth",
        "token": "test_token",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(DbtConfigError, match=r"'account' is required when using oauth or jwt"):
            connections.SnowflakeCredentials(**credentials)


def test_missing_account_for_jwt_raises_error():
    """Test that missing account for jwt raises an error"""
    credentials = {
        "authenticator": "jwt",
        "token": "test_token",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(DbtConfigError, match=r"'account' is required when using oauth or jwt"):
            connections.SnowflakeCredentials(**credentials)


def test_missing_account_and_user_allowed_for_default_auth_without_password():
    """Test that both account and user can be omitted for default authenticator without password (user's scenario)"""
    credentials = {
        "database": "NJOGI_DB",
        "schema": "PUBLIC",
        "role": "PUBLIC",
        "warehouse": "TEST_WAREHOUSE_DBT",
        # No authenticator (defaults to None) and no password
    }

    with patch.dict(os.environ, {}, clear=True):
        creds = connections.SnowflakeCredentials(**credentials)
        assert creds.account is None
        assert creds.user is None
        assert creds.authenticator is None  # Default authenticator
        assert creds.password is None  # No password provided
        assert creds.unique_field == "default"


def test_missing_user_for_default_auth_with_password_raises_error():
    """Test that missing user for default authenticator WITH password raises an error"""
    credentials = {
        "account": "test_account",
        "password": "test_password",
        # No authenticator specified, defaults to None (password auth)
        "database": "test_database",
        "warehouse": "test_warehouse",
        "schema": "test_schema",
    }

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(DbtConfigError, match=r"'user' is required for this authenticator"):
            connections.SnowflakeCredentials(**credentials)
