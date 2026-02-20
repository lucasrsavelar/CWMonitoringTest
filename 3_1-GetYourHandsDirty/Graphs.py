import DatabaseConnection
import matplotlib.pyplot as plt
from pathlib import Path

OUTPUT_DIR = Path("graphs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

db = DatabaseConnection.DatabaseConnection()

df_origin1 = db.getSales(conditions="origin = 1")
df_origin2 = db.getSales(conditions="origin = 2")

def generate_graph(df, columns, name):
    
    plt.figure()

    for column in columns:
        plt.plot(df["hour"], df[column], marker='o')

    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Sales")
    plt.title(name)
    plt.xticks(df["hour"])
    plt.legend(columns)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / (name + ".png")) 
    plt.close() 

name = "Origin 1 - Sales Comparison (Today vs Same Day Last Week)"
generate_graph(df_origin1, ["today", "same_day_last_week"], name)

name = "Origin 2 - Sales Comparison (Today vs Same Day Last Week)"
generate_graph(df_origin2, ["today", "same_day_last_week"], name)

name = "Origin 1 - Sales Comparison (Today vs Yesterday)"
generate_graph(df_origin1, ["today", "yesterday"], name)

name = "Origin 2 - Sales Comparison (Today vs Yesterday)"
generate_graph(df_origin2, ["today", "yesterday"], name)

name = "Origin 1 - Sales Comparison (Today vs Avg Last Week)"
generate_graph(df_origin1, ["today", "avg_last_week"], name)

name = "Origin 2 - Sales Comparison (Today vs Avg Last Week)"
generate_graph(df_origin2, ["today", "avg_last_week"], name)

name = "Origin 1 - Sales Comparison (Today vs Yesterday vs Avg Last Week)"
generate_graph(df_origin1, ["today", "yesterday", "avg_last_week"], name)

name = "Origin 2 - Sales Comparison (Today vs Yesterday vs Avg Last Week)"
generate_graph(df_origin2, ["today", "yesterday", "avg_last_week"], name)

name = "Origin 1 - Sales Comparison (Today vs Avg Last Month)"
generate_graph(df_origin1, ["today", "avg_last_month"], name)

name = "Origin 2 - Sales Comparison (Today vs Avg Last Month)"
generate_graph(df_origin2, ["today", "avg_last_month"], name)

# Special graph comparing both origins
name = "Today - Origin 1 vs Origin 2"
plt.figure()
plt.plot(df_origin1["hour"], df_origin1["today"], marker='o')
plt.plot(df_origin2["hour"], df_origin2["today"], marker='o')
plt.xlabel("Hour of Day")
plt.ylabel("Number of Sales")
plt.title(name)
plt.xticks(df_origin1["hour"])
plt.legend(["Origin 1", "Origin 2"])
plt.tight_layout()
plt.savefig(OUTPUT_DIR / (name + ".png")) 
plt.close()