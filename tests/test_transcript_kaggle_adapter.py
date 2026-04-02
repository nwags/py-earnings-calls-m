import pandas as pd

from py_earnings_calls.adapters.transcripts_kaggle import KaggleMotleyFoolTranscriptAdapter


def test_kaggle_adapter_loads_basic_csv(tmp_path):
    df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "date": "2026-01-30",
                "title": "Apple Q1 2026 Earnings Call Transcript",
                "transcript": "Operator: Welcome everyone.",
            }
        ]
    )
    dataset = tmp_path / "calls.csv"
    df.to_csv(dataset, index=False)

    adapter = KaggleMotleyFoolTranscriptAdapter()
    docs = adapter.load_documents(str(dataset))

    assert len(docs) == 1
    assert docs[0].symbol == "AAPL"
    assert "Welcome" in docs[0].transcript_text


def test_kaggle_adapter_supports_column_variants_and_provider_id(tmp_path):
    df = pd.DataFrame(
        [
            {
                "Ticker": "msft",
                "Content": "Prepared remarks and Q&A.",
                "published_at": "2020-08-27T21:00:00",
                "headline": "Microsoft Earnings Call",
                "id": "abc-123",
            }
        ]
    )
    dataset = tmp_path / "calls.csv"
    df.to_csv(dataset, index=False)

    docs = KaggleMotleyFoolTranscriptAdapter().load_documents(str(dataset))

    assert len(docs) == 1
    assert docs[0].symbol == "MSFT"
    assert docs[0].provider_call_id == "abc-123"
    assert docs[0].call_datetime is not None
    assert docs[0].call_datetime.year == 2020


def test_kaggle_adapter_infers_symbol_and_parses_motley_fool_datetime(tmp_path):
    df = pd.DataFrame(
        [
            {
                "title": "Brunswick (BC 0.66%) Q4 2018",
                "date": "Jan. 31, 2019 11:00 a.m. ET",
                "transcript": "Operator: Good morning and welcome.",
            }
        ]
    )
    dataset = tmp_path / "calls.csv"
    df.to_csv(dataset, index=False)

    docs = KaggleMotleyFoolTranscriptAdapter().load_documents(str(dataset))

    assert len(docs) == 1
    assert docs[0].symbol == "BC"
    assert docs[0].provider_call_id is not None
    assert docs[0].provider_call_id.startswith("fallback|symbol=BC")
    assert docs[0].call_datetime is not None
    assert docs[0].call_datetime.year == 2019
    assert docs[0].call_datetime.hour == 11
