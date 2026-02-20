from typing import Optional
from pydantic import BaseModel, Field

# Incoming payload for POST /predict
class Transaction(BaseModel):
    timestamp:        str = Field(..., example="2025-07-12 13:45:00")
    approved:         int = Field(..., ge=0)
    denied:           int = Field(..., ge=0)
    failed:           int = Field(..., ge=0)
    reversed:         int = Field(..., ge=0)
    backend_reversed: int = Field(..., ge=0)
    refunded:         int = Field(..., ge=0)

# Response shape for GET /transactions
# One aggregated row per time window
class TransactionRow(BaseModel):
    date_hour:        str
    approved:         int
    denied:           int
    failed:           int
    refunded:         int
    reversed:         int
    backend_reversed: int
    total_transactions: int

# Anomaly detail payload
# ensemble_score is None for rule-based detections (no ML score)
class AnomalyResponse(BaseModel):
    date_hour:      str
    main_feature:   str
    details:        str
    ensemble_score: Optional[float] = None

# Top-level response for POST /predict
# body is None when no anomaly is detected
class PredictionResponse(BaseModel):
    message:        str
    is_anomaly:     bool
    body:           Optional[AnomalyResponse] = None