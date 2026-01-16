from google.cloud import bigquery
client = bigquery.Client()
table_id = "my-673-project.bus673_compustat.annual_data2000_2025"
table = client.get_table(table_id)

query = """
SELECT fyear, SUM(sale) AS total_sales, AVG(sale) AS avg_sales
FROM my-673-project.bus673_compustat.annual_data2000_2025
GROUP BY fyear ORDER BY fyear;
"""
df = client.query(query).to_dataframe()

print(df)

import matplotlib.pyplot as plt # type: ignore
plt.plot(df.fyear, df.avg_sales)
plt.title("Fiscal Year vs. Average Sales")
plt.xlabel("Fiscal Year")
plt.ylabel("Average Sales")
plt.show()