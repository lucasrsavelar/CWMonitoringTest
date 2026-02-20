import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from pyod.models.iforest import IForest
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from DatabaseConnection import DatabaseConnection

OUTPUT_DIR           = Path("models")
CONTAMINATION        = 0.02  # expected fraction of anomalies in training data
THRESHOLD_PERCENTILE = 90   # ensemble score percentile used as the anomaly cut-off

ALL_STATUSES = [
    "approved",
    "denied",
    "failed",
    "refunded",
    "reversed",
    "backend_reversed",
]

# Metrics checked against historical max before the ML model runs (deterministic layer)
ALL_TRESHOLDS = [
    "denied",
    "failed",
    "refunded",
    "reversed",
    "backend_reversed",
    "denied_rate",
    "failed_rate",
    "refunded_rate",
    "reversed_rate",
    "backend_reversed_rate",
]

# Required files for loading the model
# They must be all present in the models directory for the model to load successfully
REQUIRED_FILES = [
    "iforest.pkl",
    "copod.pkl",
    "ecod.pkl",
    "scaler.pkl",
    "metadata.pkl",
]

# Weighted average: IForest carries more weight as it is more stable in this context
MODEL_WEIGHTS = {
    "iforest": 0.4,
    "copod":   0.3,
    "ecod":    0.3,
}

class AnomalyDetectionModel:

    def __init__(self, dbc: DatabaseConnection):
        self._dbc       = dbc or DatabaseConnection()
        self._scaler    = None
        self._models    = {}
        self._threshold = None
        self._feature_names = []
        self._score_ranges = {}
        self._rules_threshold = {}
        self._is_trained = False

    # Fetch the historical maximum for each risk metric and store as lookup dict
    def _get_rules_threshold(self):
        rules_threshold = {}

        max_values = self._dbc.getMaxValueByStatus()
        max_rates = self._dbc.getMaxRateByStatus()

        for _, row in max_values.iterrows():
            rules_threshold[row["status"]] = row["max_amount"]

        for col in max_rates.columns:
            value = float(max_rates.iloc[0][col])
            rules_threshold[col] = round(value, 4)

        return rules_threshold

    def _prepare_data(self, df = None):
        if df is None:
            df = self._dbc.getTransactionsByMinute()

        df = df.copy()

        # Replace 0 totals with NaN so division produces 0-rate instead of inf
        total = df["total_transactions"].replace(0, np.nan)
        for status in ALL_STATUSES:
            df[f"{status}_rate"] = (df[status] / total).fillna(0)

        dt = df['date_hour']
        df["hour"] = dt.dt.hour
        df["minute"] = dt.dt.minute

        # Hour/minute encoded as sine/cosine to preserve cyclical continuity (e.g. 23h next to 0h)
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["minute_sin"] = np.sin(2 * np.pi * df["minute"] / 60)
        df["minute_cos"] = np.cos(2 * np.pi * df["minute"] / 60)

        df = df.drop(columns=["date_hour", "hour", "minute"])

        return df

    # Min-max normalization
    # Returns zeros if all scores are identical
    def _normalize(self, arr, mn, mx):
        return (arr - mn) / (mx - mn) if mx > mn else np.zeros_like(arr, dtype=float)

    def _weighted_ensemble(self, scores):
        return sum(
            scores[f"score_{name}"] * weight
            for name, weight in MODEL_WEIGHTS.items()
        )

    # Build scores for each model and the ensemble
    def _build_scores(self, X_scaled):
        scores = pd.DataFrame()

        for name, model in self._models.items():
            arr      = model.decision_function(X_scaled)
            mn, mx   = self._score_ranges[name]
            # Clip to [0, 1] to guard against out-of-range values from unseen data
            normalized = np.clip(self._normalize(arr, mn, mx), 0.0, 1.0)
            scores[f"score_{name}"] = normalized

        scores["ensemble_score"] = self._weighted_ensemble(scores)
        return scores

    def _save_model(self):
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        for name, model in self._models.items():
            joblib.dump(model, OUTPUT_DIR / f"{name}.pkl")

        joblib.dump(self._scaler, OUTPUT_DIR / "scaler.pkl")
        joblib.dump(
            {
                "threshold":            self._threshold,
                "feature_names":        self._feature_names,
                "score_ranges":         self._score_ranges,
                "model_weights":        MODEL_WEIGHTS,
                "contamination":        CONTAMINATION,
                "threshold_percentile": THRESHOLD_PERCENTILE,
            },
            OUTPUT_DIR / "metadata.pkl",
        )

    def _load_model(self):
        try:
            meta = joblib.load(OUTPUT_DIR / "metadata.pkl")
            self._models = {
                "iforest": joblib.load(OUTPUT_DIR / "iforest.pkl"),
                "copod":   joblib.load(OUTPUT_DIR / "copod.pkl"),
                "ecod":    joblib.load(OUTPUT_DIR / "ecod.pkl"),
            }
            self._scaler        = joblib.load(OUTPUT_DIR / "scaler.pkl")
            self._threshold     = meta["threshold"]
            self._feature_names = meta["feature_names"]
            self._score_ranges  = meta["score_ranges"]
            self._is_trained    = True
        except FileNotFoundError as e:
            raise RuntimeError("Model not found. Run .train() first.") from e

    def train(self, contamination=CONTAMINATION, tresholdPercentile=THRESHOLD_PERCENTILE):
        df = self._prepare_data()

        self._feature_names = df.columns.tolist()
        self._scaler = StandardScaler()

        X = self._scaler.fit_transform(df)

        self._models = {}
        for name, weight in MODEL_WEIGHTS.items():
            if name == "iforest":
                clf = IForest(contamination=contamination, random_state=42, n_jobs=-1)
            elif name == "copod":
                clf = COPOD(contamination=contamination, n_jobs=-1)
            elif name == "ecod":
                clf = ECOD(contamination=contamination, n_jobs=-1)
            else:
                continue

            clf.fit(X)
            self._models[name] = clf

        # Normalize scores, apply weights and compute ensemble
        ensemble_scores = np.zeros(len(X))
        for name, clf in self._models.items():
            raw = clf.decision_scores_
            mn, mx = float(raw.min()), float(raw.max())
            self._score_ranges[name] = (mn, mx)
            normalized = self._normalize(raw, mn, mx)
            ensemble_scores += MODEL_WEIGHTS[name] * normalized

        self._threshold = np.percentile(ensemble_scores, tresholdPercentile)

        self._is_trained = True

        self._save_model()

    def predict(self, raw_df):
        if not self._is_trained:
            raise RuntimeError("Model must be trained before predicting.")

        # n_jobs forced to 1 at inference
        # The time to create n > 1 workers is higher than the time to just process the data
        self._models["iforest"].n_jobs = 1
        self._models["copod"].n_jobs = 1
        self._models["ecod"].n_jobs = 1

        df_dates = raw_df[["date_hour"]].copy()
        df = self._prepare_data(raw_df)

        rule_main_feature = None
        rule_message = ""

        # Rule-based approach
        # If any of the thresholds is exceeded, the feature is flagged as an anomaly
        for threshold in ALL_TRESHOLDS:
            value = float(df[threshold].iloc[0])
            if value >= self._rules_threshold[threshold]:
                if rule_main_feature is None:
                    rule_main_feature = threshold
                rule_message += f"{threshold} has a value of {value}. This value is higher than the maximum historical value for this metric.\n"

        if rule_main_feature is not None:
            return pd.DataFrame([{
                "is_anomaly":   1,
                "is_model":     0,
                "main_feature": rule_main_feature,
                "details":      rule_message,
            }])

        # Model-based approach
        X_scaled  = self._scaler.transform(df)
        scores_df = self._build_scores(X_scaled)

        # Ensemble score is the final score
        # If the ensemble score is higher than the threshold, the transaction is flagged as an anomaly
        model_result = df_dates.reset_index(drop=True)
        model_result = pd.concat([model_result, scores_df.reset_index(drop=True)], axis=1)
        model_result["is_anomaly"] = (model_result["ensemble_score"] >= self._threshold).astype(int)
        model_result["is_model"] = 1

        return model_result

    def explain(self, raw_df, top_n = 5):
        if not self._is_trained:
            raise RuntimeError("Model must be trained before predicting.")

        df_dates = raw_df[["date_hour"]].copy().reset_index(drop=True)
        df = self._prepare_data(raw_df)

        for col in self._feature_names:
            if col not in df.columns:
                df[col] = 0
        df = df[self._feature_names]

        X_scaled = self._scaler.transform(df)
        X_raw = df.values

        # Find the top n features with the highest z-score
        # These are the features that are most likely to be the cause of the anomaly
        records = []
        for i in range(len(X_scaled)):
            z_scores = np.abs(X_scaled[i])
            top_index = np.argsort(z_scores)[::-1][:top_n]

            for rank, idx in enumerate(top_index):
                records.append({
                "date_hour":       df_dates["date_hour"].iloc[i],
                "rank":            rank + 1,
                "feature":         self._feature_names[idx],
                "original_value":  round(float(X_raw[i][idx]), 4),
                "z_score":         round(float(X_scaled[i][idx]), 4),
            })

        return pd.DataFrame(records)

    def start(self):

        self._rules_threshold = self._get_rules_threshold()

        all_exist = all((OUTPUT_DIR / f).exists() for f in REQUIRED_FILES)
        if all_exist:
            self._load_model()
        else:
            self.train()