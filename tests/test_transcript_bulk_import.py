import pandas as pd

from py_earnings_calls.adapters.transcripts_local_tabular import LocalTabularTranscriptAdapter
from py_earnings_calls.adapters.transcripts_motley_fool_pickle import MotleyFoolPickleTranscriptAdapter
from py_earnings_calls.config import AppConfig
from py_earnings_calls.pipelines.transcript_import import run_transcript_bulk_import
from py_earnings_calls.storage.paths import normalized_path


def test_local_tabular_adapter_supports_csv_and_jsonl(tmp_path):
    rows = [
        {
            "symbol": "aapl",
            "title": "Apple (AAPL) Q1",
            "date": "2026-01-30",
            "transcript": "Operator: Welcome.",
        }
    ]

    csv_path = tmp_path / "calls.csv"
    jsonl_path = tmp_path / "calls.jsonl"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    pd.DataFrame(rows).to_json(jsonl_path, orient="records", lines=True)

    adapter = LocalTabularTranscriptAdapter()
    csv_docs = adapter.load_documents(str(csv_path))
    jsonl_docs = adapter.load_documents(str(jsonl_path))

    assert len(csv_docs) == 1
    assert len(jsonl_docs) == 1
    assert csv_docs[0].symbol == "AAPL"
    assert jsonl_docs[0].symbol == "AAPL"
    assert csv_docs[0].provider == "local_tabular"


def test_motley_fool_pickle_adapter_requires_pickle_extension(tmp_path):
    path = tmp_path / "calls.csv"
    pd.DataFrame([{"transcript": "x"}]).to_csv(path, index=False)

    adapter = MotleyFoolPickleTranscriptAdapter()
    try:
        adapter.load_documents(str(path))
    except ValueError as exc:
        assert ".pkl" in str(exc)
    else:
        raise AssertionError("Expected ValueError for non-pickle path")


def test_motley_fool_pickle_adapter_loads_known_dataframe_pickle(tmp_path):
    path = tmp_path / "motley-fool-data.pkl"
    pd.DataFrame(
        [
            {
                "title": "Brunswick (BC 0.66%) Q4 2018",
                "date": "Aug 27, 2020, 9:00 p.m. ET",
                "transcript": "Operator: Good evening.",
            }
        ]
    ).to_pickle(path)

    docs = MotleyFoolPickleTranscriptAdapter().load_documents(str(path))

    assert len(docs) == 1
    assert docs[0].symbol == "BC"
    assert docs[0].provider == "motley_fool_pickle"
    assert docs[0].call_datetime is not None
    assert docs[0].call_datetime.hour == 21


def test_transcript_import_is_idempotent_and_uses_identity_fallback(tmp_path):
    dataset = tmp_path / "calls.csv"
    pd.DataFrame(
        [
            {
                "title": "Brunswick (BC 0.66%) Q4 2018",
                "date": "Jan. 31, 2019 11:00 a.m. ET",
                "transcript": "Operator: Welcome everyone.",
            },
            {
                "title": "Brunswick (BC 0.66%) Q4 2018",
                "date": "Jan. 31, 2019 11:00 a.m. ET",
                "transcript": "Operator: Welcome everyone.",
            },
        ]
    ).to_csv(dataset, index=False)

    config = AppConfig.from_project_root(tmp_path)

    first = run_transcript_bulk_import(config, str(dataset), adapter_name="local_tabular")
    second = run_transcript_bulk_import(config, str(dataset), adapter_name="local_tabular")

    calls_path = normalized_path(config, "transcript_calls")
    artifacts_path = normalized_path(config, "transcript_artifacts")
    calls_df = pd.read_parquet(calls_path)
    artifacts_df = pd.read_parquet(artifacts_path)

    assert first["document_count"] == 2
    assert second["document_count"] == 2
    assert len(calls_df.index) == 1
    assert len(artifacts_df.index) == 2

    row = calls_df.iloc[0].to_dict()
    assert row["provider"] == "local_tabular"
    assert row["provider_call_id"].startswith("fallback|symbol=BC|call_datetime=2019-01-31T11:00:00|title=Brunswick (BC 0.66%) Q4 2018")
    assert len(str(row["call_id"])) == 16
