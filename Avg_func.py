import pandas as pd


def average_probabilities(poly: pd.DataFrame, kalshi: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the daily average probability across Polymarket and Kalshi.

    Parameters
    ----------
    df_poly   : DataFrame with columns ``t`` (datetime) and ``p`` (float, 0–100 %)
                as produced by 1_Polymarket.py
    df_kalshi : DataFrame with columns ``Date`` (datetime) and ``Close (¢)`` (float, 0–100 ¢)
                as produced by 2_Kalshi.py

    Returns
    -------
    DataFrame with columns ``Date`` (date) and ``Average (%)`` (float, 0–100)
    """
    if len(poly) == 0 or len(kalshi) == 0:
        return pd.DataFrame(columns=["Date", "Average (%)"])

    poly = poly[["t", "p"]].copy()
    poly["Date"] = pd.to_datetime(poly["t"]).dt.normalize()
    poly = poly.groupby("Date", as_index=False)["p"].mean().rename(columns={"p": "polymarket"})

    kalshi = kalshi[["Date", "Close (¢)"]].copy()
    kalshi["Date"] = pd.to_datetime(kalshi["Date"]).dt.normalize()
    kalshi = kalshi.groupby("Date", as_index=False)["Close (¢)"].mean().rename(columns={"Close (¢)": "kalshi"})

    merged = poly.merge(kalshi, on="Date", how="inner")
    merged["Average (%)"] = (merged["polymarket"] + merged["kalshi"]) / 2
    return merged[["Date", "Average (%)"]]
