"""
This script runs a prediction on the daily order volume using SARIMA.

modal run ml/forecast.py --forecast-file forecast.parquet --html-report forecast.html --gcs-bucket kestraio --nr-days-fcst 90 --color-history blue --color-prediction red
"""
import json
import os
import modal
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

app = modal.App(
    "dbt-modal-order-forecast",
    secrets=[
        modal.Secret.from_local_environ(
            env_keys=[
                "CPU",
                "MEMORY",
            ]
        )
    ],
)

# Use statsmodels for SARIMA and Plotly for visualization
image = modal.Image.debian_slim().pip_install(
    "pandas",
    "pandas-gbq",
    "pyarrow",
    "google-cloud-storage",
    "google-cloud-bigquery",
    "google-cloud-bigquery-storage",
    "plotly",
    "kestra",
    "statsmodels",
)


@app.function(
    secrets=[modal.Secret.from_name("gcp_creds")],
    image=image,
    cpu=float(os.getenv("CPU", 0.25)),
    memory=int(os.getenv("MEMORY", 256)),
)
def predict_order_volume(
    forecast_file: str,
    html_report: str,
    gcs_bucket: str,
    nr_days_fcst: int,
    color_history: str,
    color_prediction: str,
):
    from google.cloud import bigquery
    from google.cloud import storage
    from google.oauth2 import service_account
    from kestra import Kestra
    import pandas as pd
    import plotly.graph_objs as go
    import datetime
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    # ==================== EXTRACT =================
    service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )
    client = bigquery.Client(credentials=credentials)

    QUERY = """
    SELECT
      DATE(ordered_at) AS ds,  -- Group by day
      SUM(order_total) AS y    -- Sum the order totals per day
    FROM
      `geller.dwh.orders`
    GROUP BY
      ds
    ORDER BY
      ds
    """

    df = client.query(QUERY).to_dataframe()
    initial_nr_rows = len(df)
    print(f"Number of rows in the dataset: {initial_nr_rows}")

    # ==================== TRANSFORM ====================
    df["ds"] = pd.to_datetime(df["ds"])
    df.set_index("ds", inplace=True)
    df = df.asfreq("D")  # Ensuring daily frequency
    nr_rows_daily = len(df)

    # ==================== TRAIN SARIMA MODEL ====================
    model = SARIMAX(df["y"], order=(1, 1, 1), seasonal_order=(1, 1, 1, 7))
    sarima_fit = model.fit(disp=False)

    # ==================== PREDICT ====================
    future = sarima_fit.get_forecast(steps=nr_days_fcst)
    forecast = future.summary_frame()

    # Create future dates
    future_dates = pd.date_range(
        df.index.max() + datetime.timedelta(days=1), periods=nr_days_fcst
    )
    forecast_df = pd.DataFrame({"ds": future_dates, "yhat": forecast["mean"]})
    forecast_df.to_parquet(forecast_file)

    # ==================== VISUALIZE WITH PLOTLY ====================
    forecast_fig = go.Figure()
    forecast_fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["y"],
            mode="lines",
            name="Historical Order Volume",
            line=dict(color=color_history),
        )
    )
    forecast_fig.add_trace(
        go.Scatter(
            x=forecast_df["ds"],
            y=forecast_df["yhat"],
            mode="lines",
            name="Predicted Order Volume",
            line=dict(color=color_prediction),
        )
    )

    forecast_fig.update_layout(
        title=f"Order Volume Prediction for the Next {nr_days_fcst} Days",
        xaxis_title="Date",
        yaxis_title="Order Total",
        legend_title="Legend",
        xaxis=dict(showgrid=True),
        yaxis=dict(showgrid=True),
    )

    forecast_fig.write_html(html_report)

    # ==================== UPLOAD TO GCS ====================
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(gcs_bucket)
    files_to_upload = [html_report, forecast_file]
    for file_name in files_to_upload:
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_name)
        print(f"File {file_name} uploaded to {gcs_bucket}.")

    Kestra.outputs(
        dict(
            initial_nr_rows=initial_nr_rows,
            nr_rows_daily=nr_rows_daily,
            forecast_file=forecast_file,
            html_report=html_report,
        )
    )
    return forecast_file, html_report


@app.local_entrypoint()
def generate_and_predict(
    forecast_file: str,
    html_report: str,
    gcs_bucket: str,
    nr_days_fcst: int,
    color_history: str,
    color_prediction: str,
) -> None:
    results = predict_order_volume.remote(
        forecast_file,
        html_report,
        gcs_bucket,
        nr_days_fcst,
        color_history,
        color_prediction,
    )
    print(f"Forecast file: {results[0]}, HTML report: {results[1]}")
