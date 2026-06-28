Credit Card Fraud Detection Pipeline (PySpark)

An end-to-end PySpark pipeline that analyzes ~555,000 credit card transactions to identify where fraud concentrates — by merchant category, transaction amount, time of day, and geographic location.

What it does

The pipeline reads raw transaction data, cleans it, and produces five breakdowns of fraud rate:


By category — which types of merchants (shopping, grocery, gas, etc.) see the highest fraud rate, with a global rank attached to each category.
By amount range — whether high-value transactions are disproportionately fraudulent (bucketed into 0–100, 100–500, 500–1000, 1000+).
By amount range, within each state — the same amount-range question, but answered separately per state, so regional patterns aren't hidden by the national average.
By hour of day — whether fraud spikes at certain times.
By state — overall fraud rate per state.


Key findings


High-value transactions are dramatically riskier: the 1000+ and 500-1000 buckets sit around 17% fraud rate, versus 0.11% for transactions under $100.
This pattern isn't uniform across states — for example, some states' riskiest bucket is 500-1000, others' is 1000+, and a few low-volume states show 100% fraud rate in their top bucket purely because they only had one or two transactions in that range (see Limitations).
Early morning hours (12am–3am) show a noticeably higher fraud rate (~0.9–1.1%) than the middle of the day (~0.05–0.09%).


Tech stack


PySpark (SparkSession, DataFrame API, window functions)
Pandas (final conversion to CSV for output)
Input: CSV (fraudTest.csv), ~555K rows, 23 columns