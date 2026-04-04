from __future__ import annotations

from py_earnings_calls.runtime_output import render_summary_block


def test_quiet_mode_includes_next_step_only_when_incomplete():
    success = render_summary_block(
        "Done",
        {
            "requested_count": 2,
            "fetched_count": 2,
            "failed_count": 0,
            "next_step": "Should be hidden on success",
        },
        mode="quiet",
    )
    assert "next_step" not in success

    incomplete = render_summary_block(
        "Done",
        {
            "requested_count": 2,
            "fetched_count": 1,
            "failed_count": 1,
            "next_step": "Retry failures",
        },
        mode="quiet",
    )
    assert "next_step" in incomplete


def test_verbose_mode_truncates_long_collections():
    output = render_summary_block(
        "Verbose",
        {
            "artifact_paths": [f"/tmp/{i}" for i in range(50)],
        },
        mode="verbose",
    )
    assert "len=50" in output
