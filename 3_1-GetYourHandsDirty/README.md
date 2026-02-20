# Analyzing Sales Behavior

The goal of this task is to analyze a dataset of hourly sales across two distinct origins, aiming to identify possible anomalies that require attention. To accomplish this, tools are used to persist and retrieve data from databases and to generate graphs to assist in decision-making.

## Preparation

### 1. Data Processing

The provided .csv files were unified into a single file `unified.csv` (available for reference) with the addition of an `origin` column that indicates the source file (1 for `checkout_1.csv` and 2 for `checkout_2.csv`).

### 2. Database

A PostgreSQL database was created to store the sales data, with a `sales` table containing the following columns:

- `sale_id`: unique sale identifier
- `origin`: sale origin
- `sale_time`: time of the sale
- `today`: number of sales on the current day
- `yesterday`: number of sales on the previous day
- `same_day_last_week`: number of sales on the same day of the previous week
- `avg_last_week`: average sales for the previous week
- `avg_last_month`: average sales for the previous month

The `unified.csv` file was then imported into the database via the pgAdmin GUI, populating the `sales` table.

For more information about the database, refer to the `getyourhandsdirty.sql` file.

### 3. Python Scripts

Two Python scripts were created to assist in the data analysis:

- `DatabaseConnection.py`: script responsible for connecting to the database and retrieving sales data. It has two main methods:
    - `getAllSales()`: returns all sales
    ```sql
    SELECT * FROM sales
    ```
    - `getSales(columns, conditions)`: returns sales based on specified columns and conditions.
        ```sql
        SELECT columns FROM sales WHERE conditions
        ```
        - If no conditions are specified, returns all sales.
        ```sql
        SELECT columns FROM sales
        ```
        - If no columns are specified, returns all of them.
        ```sql
        SELECT * FROM sales WHERE conditions
        ```

- `Graphs.py`: script responsible for generating sales graphs. It has a main method `generate_graph(df, columns, name)` that generates and saves the sales graphs.
    - `df`: DataFrame with the sales data
    - `columns`: columns to be plotted
    - `name`: name of the file to be generated

## Analysis

The Graphs.py script was used to generate 5 date comparison graphs — one for each origin, plus a special graph comparing both origins:

- Sales Comparison (Today vs Same Day Last Week)
- Sales Comparison (Today vs Yesterday)
- Sales Comparison (Today vs Avg Last Week)
- Sales Comparison (Today vs Yesterday vs Avg Last Week)
- Sales Comparison (Today vs Avg Last Month)
- Today - Origin 1 vs Origin 2

After analyzing all graphs and identifying the main patterns, three notable potential anomalies were identified:

1. **Origin 2 sales today between 3 PM and 5 PM**: this is the most critical and urgent anomaly. The "Today - Origin 1 vs Origin 2" graph shows that Origin 2 sales dropped to zero between 3 PM and 5 PM, while Origin 1 sales remained considerably high. Analyzing the "Origin 2 - Sales Comparison (Today vs Same Day Last Week)" and "Origin 2 - Sales Comparison (Today vs Yesterday)" graphs confirms that this pattern is highly atypical for Origin 2 during this time window, as sales usually remain elevated. The evidence points to a severe operational failure that directly impacted Origin 2's sales processing capacity for a considerably long period, until normalizing at 6 PM. As this is a severe and prolonged incident, it is essential to investigate the root cause with priority and maintain heightened monitoring, especially during the affected time window.

2. **Origin 1 sales in the morning (7 AM – 9 AM)**: the "Today - Origin 1 vs Origin 2" graph also revealed that Origin 1 morning sales, particularly between 7 AM and 9 AM, were considerably lower compared to the same period for Origin 2, dropping to zero at 8 AM. Looking solely at the today-vs-yesterday comparison for Origin 1 via the "Origin 1 - Sales Comparison (Today vs Yesterday)" graph, this difference might appear normal, since Origin 1's sales volume yesterday was also quite low. However, when analyzing longer-term graphs such as "Origin 1 - Sales Comparison (Today vs Same Day Last Week)", "Origin 1 - Sales Comparison (Today vs Avg Last Week)", and "Origin 1 - Sales Comparison (Today vs Avg Last Month)", it becomes clear that Origin 1's sales volume both yesterday and today were significantly lower than historical reference values for the same period. Disregarding external factors beyond the provided data (such as seasonality), this is indicative of a recent issue, possibly introduced by the latest system changes, that is affecting Origin 1's performance in the morning. It is important to investigate the root cause and evaluate whether recent changes may have contributed to this decline.

3. **Origin 2 sales yesterday in the morning (7 AM – 9 AM)**: the "Origin 2 - Sales Comparison (Today vs Yesterday)" graph shows that Origin 2's sales yesterday morning, particularly between 7 AM and 9 AM, were considerably lower compared to historical values for the same period, dropping to zero at 8 AM. This is the same behavior observed in the previous anomaly for Origin 1, but in this case the sales volume for the same period today on Origin 2 appeared to have normalized. Although this anomaly is lower in priority compared to the others identified, attention is warranted to detect if this pattern repeats in the future and, most importantly, whether it may be related to the incident reported in item 2.