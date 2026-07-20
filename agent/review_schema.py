from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


Score = Literal[1, 2, 3, 4, 5]


class DimensionAssessment(BaseModel):
    score: Score
    evidence: list[str] = Field(min_length=1, max_length=4)
    uncertainty: str = ""


class FundamentalAssessment(BaseModel):
    status: Literal["verified", "missing", "stale", "risk"]
    evidence: list[str] = Field(default_factory=list, max_length=5)
    risk_flags: list[str] = Field(default_factory=list, max_length=5)


class ReviewModelOutput(BaseModel):
    trend: DimensionAssessment
    price_position: DimensionAssessment
    volume_behavior: DimensionAssessment
    previous_abnormal_move: DimensionAssessment
    fundamentals: FundamentalAssessment
    signal_type: Literal["trend_start", "rebound", "distribution_risk"]
    invalidation_conditions: list[str] = Field(min_length=1, max_length=4)
    positive_evidence: list[str] = Field(default_factory=list, max_length=4)
    risk_flags: list[str] = Field(default_factory=list, max_length=6)
    hard_risk_flags: list[
        Literal[
            "delisting_or_listing_status",
            "financial_fraud_or_audit",
            "liquidity_or_debt_crisis",
            "earnings_collapse",
            "data_quality_failure",
        ]
    ] = Field(default_factory=list, max_length=5)
    comment: str = Field(min_length=8, max_length=220)


WEIGHTS = {
    "trend_structure": 0.20,
    "price_position": 0.20,
    "volume_behavior": 0.30,
    "previous_abnormal_move": 0.30,
}


def finalize_review(parsed: ReviewModelOutput, *, code: str, metadata: dict) -> dict:
    scores = {
        "trend_structure": parsed.trend.score,
        "price_position": parsed.price_position.score,
        "volume_behavior": parsed.volume_behavior.score,
        "previous_abnormal_move": parsed.previous_abnormal_move.score,
    }
    total = round(sum(scores[key] * weight for key, weight in WEIGHTS.items()), 2)
    hard_fail = (
        scores["volume_behavior"] == 1
        or parsed.signal_type == "distribution_risk"
        or parsed.fundamentals.status == "risk"
        or bool(parsed.hard_risk_flags)
    )
    if hard_fail or total < 3.2:
        verdict = "FAIL"
    elif total < 4.0:
        verdict = "WATCH"
    else:
        verdict = "PASS"

    return {
        "code": code,
        "trend_reasoning": "；".join(parsed.trend.evidence),
        "position_reasoning": "；".join(parsed.price_position.evidence),
        "volume_reasoning": "；".join(parsed.volume_behavior.evidence),
        "abnormal_move_reasoning": "；".join(parsed.previous_abnormal_move.evidence),
        "signal_reasoning": f"信号={parsed.signal_type}；风险={';'.join(parsed.risk_flags) or '未发现明确硬伤'}",
        "scores": scores,
        "total_score": total,
        "signal_type": parsed.signal_type,
        "verdict": verdict,
        "comment": parsed.comment,
        "fundamentals": parsed.fundamentals.model_dump(),
        "positive_evidence": parsed.positive_evidence,
        "risk_flags": parsed.risk_flags,
        "hard_risk_flags": parsed.hard_risk_flags,
        "invalidation_conditions": parsed.invalidation_conditions,
        "assessment": {
            "trend": parsed.trend.model_dump(),
            "price_position": parsed.price_position.model_dump(),
            "volume_behavior": parsed.volume_behavior.model_dump(),
            "previous_abnormal_move": parsed.previous_abnormal_move.model_dump(),
        },
        "metadata": metadata,
    }
