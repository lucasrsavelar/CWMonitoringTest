import sqlalchemy
import pandas as pd

class DatabaseConnection:

    # Default connection targets a local PostgreSQL instance, override via constructor
    def __init__(self, connection_string="postgresql://postgres:postgres@localhost:5432/cloudwalk"):
        self.engine = sqlalchemy.create_engine(connection_string)

    def getAllTransactions(self, start_date=None, end_date=None):
        conditions = []
        params = {}

        if start_date is not None:
            conditions.append("date_hour >= :start_date")
            params["start_date"] = start_date

        if end_date is not None:
            conditions.append("date_hour <= :end_date")
            params["end_date"] = end_date

        # Build WHERE clause dynamically only if date filters are provided
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        query = sqlalchemy.text(f"""
            SELECT *
            FROM transactions
            {where}
            ORDER BY date_hour ASC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params if params else None)

        df['date_hour'] = pd.to_datetime(df['date_hour'])
        return df

    def getTransactionsByMinute(self, start_date=None, end_date=None):
        conditions = []
        params = {}

        if start_date is not None:
            conditions.append("date_hour >= :start_date")
            params["start_date"] = start_date

        if end_date is not None:
            conditions.append("date_hour <= :end_date")
            params["end_date"] = end_date

        # Build WHERE clause dynamically only if date filters are provided
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # Pivot raw status rows into one column per status, summed per time window
        query = sqlalchemy.text(f"""
            SELECT
                date_hour,
                SUM(CASE WHEN status = 'approved' THEN amount ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'denied' THEN amount ELSE 0 END) AS denied,
                SUM(CASE WHEN status = 'failed' THEN amount ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END) AS refunded,
                SUM(CASE WHEN status = 'reversed' THEN amount ELSE 0 END) AS reversed,
                SUM(CASE WHEN status = 'backend_reversed' THEN amount ELSE 0 END) AS backend_reversed,
                SUM(amount) AS total_transactions
            FROM transactions
            {where}
            GROUP BY date_hour
            ORDER BY date_hour
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params if params else None)

        df['date_hour'] = pd.to_datetime(df['date_hour'])
        return df

    def insertTransactions(self, date_hour, amounts):
        # One row per status per timestamp
        rows = [
            {"date_hour": date_hour, "status": status, "amount": amount}
            for status, amount in amounts.items()
            if amount >= 0
        ]

        if not rows:
            return

        query = sqlalchemy.text("""
            INSERT INTO transactions (transaction_id, date_hour, status, amount)
            VALUES (nextval('transaction_id_seq'), :date_hour, :status, :amount)
        """)

        with self.engine.begin() as conn:
            conn.execute(query, rows)

    # Returns the historical peak amount for each non-approved status
    # Used for amount-based rules
    def getMaxValueByStatus(self):
        query = sqlalchemy.text("""
            SELECT
                status,
                MAX(amount) as max_amount
            FROM transactions
            WHERE status <> 'approved'
            GROUP BY status
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        return df

    # Computes the highest observed rate (status / total) across all timestamps
    # Used for rate-based rules
    def getMaxRateByStatus(self):
        query = sqlalchemy.text("""
            WITH aggregated AS (
                SELECT
                    date_hour,
                    SUM(CASE WHEN status = 'denied' THEN amount ELSE 0 END) AS denied,
                    SUM(CASE WHEN status = 'failed' THEN amount ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END) AS refunded,
                    SUM(CASE WHEN status = 'reversed' THEN amount ELSE 0 END) AS reversed,
                    SUM(CASE WHEN status = 'backend_reversed' THEN amount ELSE 0 END) AS backend_reversed,
                    SUM(amount) AS total
                FROM transactions
                GROUP BY date_hour
            )

            SELECT
                MAX(denied::float / NULLIF(total, 0)) AS denied_rate,
                MAX(failed::float / NULLIF(total, 0)) AS failed_rate,
                MAX(refunded::float / NULLIF(total, 0)) AS refunded_rate,
                MAX(reversed::float / NULLIF(total, 0)) AS reversed_rate,
                MAX(backend_reversed::float / NULLIF(total, 0)) AS backend_reversed_rate
            FROM aggregated;
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        return df

    def insertAnomaly(self, date_hour, anomaly_data):
        rows = [
            {
                "date_hour":      date_hour, 
                "main_feature":   anomaly_data["main_feature"], 
                "anomaly_message": anomaly_data["anomaly_message"], 
                "ensemble_score": anomaly_data["ensemble_score"]
            }
        ]

        query = sqlalchemy.text("""
            INSERT INTO anomalies (anomaly_id, date_hour, main_feature, anomaly_message, ensemble_score)
            VALUES (nextval('anomaly_id_seq'), :date_hour, :main_feature, :anomaly_message, :ensemble_score)
        """)

        with self.engine.begin() as conn:
            conn.execute(query, rows)

    def getAnomalies(self):
        query = sqlalchemy.text("""
            SELECT *
            FROM anomalies
            ORDER BY date_hour DESC
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        # anomaly_id is an internal PK, strip it before returning to callers
        df = df.drop(columns=["anomaly_id"])
        df = df.to_dict("records")

        return df