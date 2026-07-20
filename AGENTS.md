# StockTradebyZ engineering guidance

## Goal

Maintain a reproducible, point-in-time stock research system for CN, HK, and US equities. The system produces research evidence and ranked candidates; it must not present model output as guaranteed investment advice.

## Non-negotiable data rules

- Use canonical ids `CN:600519`, `HK:00700`, and `US:AAPL` at provider boundaries.
- Preserve immutable provider snapshots under `data/lake/raw`; write normalized latest bars under `data/lake/curated`.
- Every fundamental observation used in a historical run must have `available_at <= pick_date`. Reject data without a reliable publication timestamp from backtests.
- Never silently mix raw, forward-adjusted, and backward-adjusted prices. Record source and adjustment mode.
- Treat AKShare and yfinance as research fallbacks and cross-checks, not sole production truth sources.
- Keep market calendars, currencies, volume units, price limits, and trading rules market-specific.

## Model rules

- The deterministic pipeline computes indicators, weighted totals, vetoes, and verdicts. A model may provide bounded dimension scores and evidence, but may not override risk rules.
- Send both daily and weekly charts plus the structured evidence packet.
- Use Pydantic Structured Outputs. Do not recover malformed JSON with regex in the OpenAI path.
- Never ask for or persist hidden chain-of-thought. Store concise evidence summaries, uncertainties, input hashes, prompt hashes, model id, and response id.
- Missing or stale fundamentals must remain explicit; never infer them from a chart.
- Keep GLM, Kimi, OpenAI, Gemini, and local-model adapters independently testable. Model changes require the same frozen evaluation set.

## Verification

Run before handoff:

```bash
python -m unittest discover -s tests -v
python -m py_compile agent/*.py pipeline/market_data/*.py pipeline/fetch_market.py pipeline/fetch_fundamentals.py run_all.py
git diff --check
```

For strategy or prompt changes, additionally run a purged walk-forward evaluation and report top-k excess return, drawdown, turnover, false-positive risk flags, JSON validity, latency, and cost by market regime.
