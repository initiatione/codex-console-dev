from src.core.register import (
    TOKEN_COMPLETENESS_COMPLETE,
    TOKEN_COMPLETENESS_ONLY_ACCESS,
    build_token_completeness_metadata,
)
from src.web.routes import registration


def test_build_token_completeness_metadata_marks_only_access_token_profile():
    metadata = build_token_completeness_metadata(
        access_token="access-token",
        account_id="acct-1",
        id_token="",
        refresh_token="",
    )

    assert metadata["has_access_token"] is True
    assert metadata["has_account_id"] is True
    assert metadata["has_id_token"] is False
    assert metadata["has_refresh_token"] is False
    assert metadata["token_completeness"] == TOKEN_COMPLETENESS_ONLY_ACCESS


def test_build_token_completeness_metadata_marks_complete_profile():
    metadata = build_token_completeness_metadata(
        access_token="access-token",
        account_id="acct-1",
        id_token="id-token",
        refresh_token="refresh-token",
    )

    assert metadata["has_access_token"] is True
    assert metadata["has_account_id"] is True
    assert metadata["has_id_token"] is True
    assert metadata["has_refresh_token"] is True
    assert metadata["token_completeness"] == TOKEN_COMPLETENESS_COMPLETE


def test_should_skip_auto_upload_for_only_access_token_profile():
    token_metadata = {"token_completeness": TOKEN_COMPLETENESS_ONLY_ACCESS}

    assert registration._should_skip_auto_upload_for_token_profile(token_metadata, True) is True
    assert registration._should_skip_auto_upload_for_token_profile(token_metadata, False) is False


def test_build_token_profile_summary_includes_complete_ratio():
    stats = registration._build_empty_token_profile_stats()
    registration._record_token_profile_stat(stats, TOKEN_COMPLETENESS_COMPLETE)
    registration._record_token_profile_stat(stats, TOKEN_COMPLETENESS_COMPLETE)
    registration._record_token_profile_stat(stats, TOKEN_COMPLETENESS_ONLY_ACCESS)

    summary = registration._build_token_profile_summary(stats, 3)

    assert "完整 2" in summary
    assert "仅 access_token 1" in summary
    assert "完整率 66.7%" in summary
