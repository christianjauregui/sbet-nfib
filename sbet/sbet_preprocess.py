import sys
import datetime as dt
from typing import List, Union, Optional
import requests
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NFIB_URL = "https://api.nfib-sbet.org:443/rest/sbetdb/_proc/getQtrMonthTrends2"
DEFAULT_QUESTIONS = [
    # --- General ---
    "bus_health",
    "bus_type",
    "top_issue",
    "supply_chain_disrupt",

    # --- Employment ---
    "emp_count",
    "emp_count_change",
    "emp_count_change_expect",
    "emp_comp_change",
    "emp_comp_change_expect",
    "job_opening_unfilled",
    "qualified_appl",
    "sales_add_empl",

    # --- Expansion & Capex ---
    "expand_good",
    "expand_good_why",
    "expand_good_why_no",
    "expand_good_why_yes",
    "expand_good_why_un",
    "cap_ex_total",
    "cap_ex_total_2",
    "cap_ex_expect",
    "cap_purch_vehicles",
    "cap_lease_vehicles",
    "cap_purch_equip",
    "cap_lease_equip",
    "cap_purch_furn",
    "cap_lease_furn",
    "cap_purch_add_build",
    "cap_lease_add_build",
    "cap_purch_imp_build",
    "cap_lease_imp_build",

    # --- Financials, Sales, Earnings ---
    "sales_actual",
    "sales_change",
    "sales_expect",
    "earn_change",
    "earn_change_reason",
    "earn_change_reason_down",
    "earn_change_reason_up",
    "price_change",
    "price_change_pct",
    "price_change_pct_up",
    "price_change_pct_down",
    "price_change_plan",
    "price_change_plan_pct",
    "price_change_plan_pct_up",
    "price_change_plan_pct_down",

    # --- Inventory ---
    "inventory_change", 
    "inventory_current",
    "inventory_expect",

    # --- Credit & Rates ---
    "credit_access",
    "credit_access_expect",
    "rate_change",
    "rate_change_2",
    "bill_pay_rate",
    "receivables",
    "trade_credit",
    
    # --- Expectations ---
    "bus_cond_expect",
]

def fetch_nfib_data(
    start_date: dt.date,
    end_date: dt.date,
    questions: Union[List[str], str],
    industry: str = "",
    state_code: str = "",
    employee_size: str = ""
) -> pd.DataFrame:
    """
    Fetches trend data from the NFIB API.
    """
    q_str = ",".join(questions) if isinstance(questions, list) else questions

    raw_params = [
        ("minYear", start_date.year),
        ("minMonth", start_date.month),
        ("maxYear", end_date.year),
        ("maxMonth", end_date.month),
        ("questions", q_str),
        ("industry", industry),
        ("statev", state_code),
        ("employee", employee_size),
    ]

    payload = {"app_name": "sbet"}
    for i, (name, value) in enumerate(raw_params):
        payload[f"params[{i}][name]"] = name
        payload[f"params[{i}][param_type]"] = "IN"
        payload[f"params[{i}][value]"] = value

    try:
        response = requests.post(NFIB_URL, data=payload, verify=False)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and "resource" in data:
            return pd.DataFrame(data["resource"])
        
        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error calling API: {e}", file=sys.stderr)
        return pd.DataFrame()


def densify_and_fill_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a complete grid of dimensions (Month x Question x Segments),
    merges with original data, and fills missing values.
    """
    if df.empty:
        return df

    df['monthyear'] = pd.to_datetime(df['monthyear'])

    dimensions = {
        'monthyear': df['monthyear'].unique(),
        'resp_q_short': df['resp_q_short'].unique(),
        'industry': df['industry'].unique(),
        'employee': df['employee'].unique(),
        'statev': df['statev'].unique()
    }

    grid_df = pd.MultiIndex.from_product(
        dimensions.values(),
        names=dimensions.keys()
    ).to_frame(index=False)

    df_merged = pd.merge(grid_df, df, on=list(dimensions.keys()), how='left')

    df_merged['totalcount'] = df_merged['totalcount'].fillna(0)
    df_merged['percent'] = df_merged['percent'].fillna(0)

    q_map = df[['resp_q_short', 'resp_q']].drop_duplicates().set_index('resp_q_short')['resp_q'].to_dict()
    
    df_merged['resp_q'] = df_merged['resp_q'].fillna(df_merged['resp_q_short'].map(q_map))
    df_merged['answer'] = df_merged['answer'].fillna('NO DATA')

    return df_merged.sort_values(by=['monthyear', 'resp_q_short']).reset_index(drop=True)

if __name__ == "__main__":
    START = dt.date(2018, 1, 1)
    END = dt.date(2025, 10, 31)
    OUTPUT_FILE = 'nfib_totalcount_percents.parquet'

    print("Fetching data from NFIB...")
    raw_df = fetch_nfib_data(START, END, DEFAULT_QUESTIONS)

    if raw_df.empty:
        print("No data retrieved. Exiting.")
        sys.exit(0)

    print(f"Retrieved {len(raw_df)} rows. Processing grid...")
    
    clean_df = densify_and_fill_data(raw_df)

    print(f"Saving {len(clean_df)} rows to {OUTPUT_FILE}...")
    clean_df.to_parquet(OUTPUT_FILE, index=False)
    print("Done.")
