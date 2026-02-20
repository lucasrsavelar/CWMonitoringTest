import sqlalchemy
import pandas as pd

class DatabaseConnection:

    def __init__(self, connection_string="postgresql://postgres:postgres@localhost:5432/cloudwalk"):
        self.engine = sqlalchemy.create_engine(connection_string)

    def getAllSales(self):
        query = "SELECT * FROM sales"

        df = pd.read_sql(query, self.engine)

        df["hour"] = df["sale_time"].str.replace("h", "").astype(int)
        df = df.sort_values("hour")
        df = df.drop(columns=["sale_time"])

        return df

    def getSales(self, columns=None, conditions=None):
        query = "SELECT "

        if columns:
            query += columns
        else:
            query += "*"
            
        query += " FROM sales"

        if conditions:
            query += " WHERE " + conditions

        df = pd.read_sql(query, self.engine)

        df["hour"] = df["sale_time"].str.replace("h", "").astype(int)
        df = df.sort_values("hour")
        df = df.drop(columns=["sale_time"])

        return df