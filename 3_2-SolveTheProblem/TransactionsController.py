import pandas as pd
import numpy as np
from typing import List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from DatabaseConnection import DatabaseConnection
from AnomalyDetectionModel import AnomalyDetectionModel
from TransactionsEntities import Transaction, TransactionRow, AnomalyResponse, PredictionResponse
from SlackNotifier import SlackNotifier

dbc   = DatabaseConnection()
model = AnomalyDetectionModel(dbc)
notifier = SlackNotifier()

# Min z-score for a risk feature to trigger an alert / appear in log detail (ML layer only)
ANOMALY_ALERT_THRESHOLD = 0
ANOMALY_LOG_THRESHOLD = 1.5

# Only these features generate alerts
# approved / approved_rate are excluded intentionally
ALERT_FEATURES = {
    "failed",
    "failed_rate",
    "reversed",
    "reversed_rate",
    "backend_reversed",
    "backend_reversed_rate",
    "denied",
    "denied_rate",
    "refunded",
    "refunded_rate",
}

def get_response_body():
    return PredictionResponse(
        message="No anomaly detected.",
        is_anomaly=False,
        body=None,
    )

# Returns "does any risk feature exceed the alert threshold"
# Used to suppress false positives
def should_alert(triggers):
    return any(
        t["feature"] in ALERT_FEATURES and t["z_score"] > ANOMALY_ALERT_THRESHOLD
        for t in triggers
    )

def send_anomaly_alert(body):
    notifier.send_anomaly_alert(
        date_hour      = body.date_hour,
        main_feature   = body.main_feature,
        details        = body.details,
        ensemble_score = body.ensemble_score
    )

# Collects the top risk feature name and a human-readable summary from the explain output
def get_anomaly_details(triggers):
    main_feature = None
    message = ""
    for t in triggers:
        if t["feature"] in ALERT_FEATURES and t["z_score"] > ANOMALY_LOG_THRESHOLD:
            if main_feature is None:
                main_feature = t["feature"]
            message += f"{t['feature']} has a z-score of {t['z_score']}. This value is higher than historical average.\n"
    return main_feature, message

def get_rule_response(transaction, result):
    body = AnomalyResponse(
            date_hour      = transaction.timestamp,
            main_feature   = result["main_feature"],
            details        = result["details"],
            ensemble_score = None
        )

    response = PredictionResponse(
        message="Anomaly detected.",
        is_anomaly=True,
        body=body,
    )

    anomaly_data = {
        "main_feature":   body.main_feature,
        "anomaly_message": body.details,
        "ensemble_score": None
    }

    dbc.insertAnomaly(body.date_hour, anomaly_data)
    send_anomaly_alert(body)

    return response


def get_model_response(df, transaction, result):
    # explain() returns per-feature z-scores
    # Used to identify which metric drove the anomaly
    triggers = model.explain(df).to_dict("records")

    if not should_alert(triggers):
        return get_response_body()

    main_feature, anomaly_details = get_anomaly_details(triggers)

    body = AnomalyResponse(
            date_hour      = transaction.timestamp,
            main_feature   = main_feature,
            details        = anomaly_details,
            ensemble_score = round(float(result["ensemble_score"]), 4)
        )

    response = PredictionResponse(
        message="Anomaly detected.",
        is_anomaly=True,
        body=body,
    )

    anomaly_data = {
        "main_feature":   main_feature,
        "anomaly_message": anomaly_details,
        "ensemble_score": body.ensemble_score
    }

    dbc.insertAnomaly(body.date_hour, anomaly_data)
    send_anomaly_alert(body)

    return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    model.start()
    yield

app = FastAPI(
    title="Anomaly Detection API",
    version="0.0.1",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/predict", response_model=PredictionResponse)
def predict(transaction: Transaction):
    try:
        df = pd.DataFrame([{
            "date_hour":          pd.to_datetime(transaction.timestamp),
            "approved":           transaction.approved,
            "denied":             transaction.denied,
            "failed":             transaction.failed,
            "refunded":           transaction.refunded,
            "reversed":           transaction.reversed,
            "backend_reversed":   transaction.backend_reversed,
            "total_transactions": (
                transaction.approved +
                transaction.denied +
                transaction.failed +
                transaction.reversed +
                transaction.backend_reversed +
                transaction.refunded
            ),
        }])

        # Drop total_transactions before persisting to avoid registering it as a status in the database
        insertData = df.drop(columns=["date_hour", "total_transactions"]).to_dict("records")[0]
        dbc.insertTransactions(df["date_hour"][0], insertData)

        result = model.predict(df).iloc[0]

        if not result["is_anomaly"]:
            return get_response_body()

        # Rule layer fires first; if triggered, skip the more expensive ML inference
        if not result["is_model"]:
            return get_rule_response(transaction, result)

        return get_model_response(df, transaction, result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/transactions", response_model=List[TransactionRow])
def get_transactions(start_date = None, end_date = None):
    try:
        df = dbc.getTransactionsByMinute(start_date, end_date)
        df["date_hour"] = df["date_hour"].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/anomalies")
def get_anomalies() -> List[dict]:
    return dbc.getAnomalies()