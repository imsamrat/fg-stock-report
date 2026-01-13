import os
import xmlrpc.client
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ---------------- LOAD ENV ----------------
load_dotenv()

ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
ODOO_API_KEY = os.getenv("ODOO_API_KEY")

# ---------------- DYNAMIC DATE FILTER ----------------
# FROM_DATE = "2025-01-01 00:00:00"
FROM_DATE = datetime.now().replace(day=1).strftime("%Y-%m-%d 00:00:00")
# TO_DATE = "2025-12-31 23:59:59"
TO_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---------------- ODOO CONNECTION ----------------
common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY or ODOO_PASSWORD, {})
if not uid:
    raise Exception("❌ Failed to authenticate with Odoo. Check credentials.")

models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

# ---------------- FILTER AND FIELDS ----------------
order_domain = [
    ("next_operation", "=", "FG Packing"),
    ("state", "!=", "done"),
    ("state", "!=", "closed"),
    ("action_date", ">=", FROM_DATE),
    ("action_date", "<=", TO_DATE),
    ("company_id", "in", [1, 3])
    # ("work_center", "=", 7)
]

order_fields = [
    "action_date",
    "oa_id",
    "company_id",
    "qty",
    "final_price",
]

records = models.execute_kw(
    ODOO_DB,
    uid,
    ODOO_API_KEY or ODOO_PASSWORD,
    "operation.details",
    "search_read",
    [order_domain],
    {"fields": order_fields},
)

print(f"✅ {len(records)} records fetched from operation.details")
if not records:
    exit("⚠️ No records found for the given filters")


# ---------------- HELPER FUNCTIONS ----------------
def safe_field(value):
    if isinstance(value, list) and len(value) > 1:
        return value[1]
    return value or ""


def format_date(date_value):
    """Format date value to DD/MM/YYYY HH:MM:SS format with IST timezone conversion"""
    if not date_value:
        return ""
    if isinstance(date_value, str):
        try:
            # Parse the date string (assuming it's in UTC) and convert to IST
            parsed_date = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")
            # Add 5 hours 30 minutes for IST timezone
            ist_date = parsed_date + timedelta(hours=6, minutes=00)
            # Format to DD/MM/YYYY HH:MM:SS
            return ist_date.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                # Try parsing without time
                parsed_date = datetime.strptime(date_value, "%Y-%m-%d")
                ist_date = parsed_date + timedelta(hours=6, minutes=00)
                return ist_date.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return str(date_value)
    return str(date_value)





# ---------------- BUILD DATA ----------------
all_data = []
for rec in records:


    all_data.append(
        {
            "action_date": format_date(rec.get("action_date")),
            "oa": safe_field(rec.get("oa_id")),
            "company": safe_field(rec.get("company_id")),
            "qty": rec.get("qty") or 0.0,
            "pack_value": (rec.get("final_price") or 0.0) * (rec.get("qty") or 0.0),
        }
    )

# ---------------- EXPORT TO EXCEL ----------------
df = pd.DataFrame(all_data)

# Convert date columns to datetime format
date_columns = ["action_date"]
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

# ---------------- GROUP DATA ----------------
if not df.empty:
    # Fill NaN values for grouping columns to avoid data loss
    df["oa"] = df["oa"].fillna("")
    df["company"] = df["company"].fillna("")

    # Truncate action_date to just the date (YYYY-MM-DD) to merge same-day records
    df["action_date"] = df["action_date"].dt.date
    
    # Group by action_date, oa, company and sum qty, pack_value
    # Using as_index=False to keep the grouping columns
    df = df.groupby(["action_date", "oa", "company"], as_index=False)[["qty", "pack_value"]].sum()

    # Sort by action_date ASC, then oa ASC
    df = df.sort_values(by=["action_date", "oa"], ascending=[True, True])



# exported_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# output_file = f"5_FG_Stock__{exported_time}.xlsx"

# # Create Excel writer with datetime formatting
# with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
#     df.to_excel(writer, index=False, sheet_name="FG_Stock")

#     # Get the workbook and worksheet
#     workbook = writer.book
#     worksheet = writer.sheets["FG_Stock"]

#     # Format the date columns
#     for col_name in date_columns:
#         if col_name in df.columns:
#             col_idx = df.columns.get_loc(col_name) + 1  # +1 because Excel is 1-indexed
#             col_letter = chr(64 + col_idx)  # Convert to Excel column letter

#             # Apply datetime format to the entire column
#             for row in range(2, len(df) + 2):  # Start from row 2 (skip header)
#                 cell = worksheet[f"{col_letter}{row}"]
#                 if cell.value:
#                     cell.number_format = "YYYY-MM-DD HH:MM:SS"

# print(f"📂 Export complete! File saved as {output_file}")


# ---------------- GOOGLE SHEETS SYNC ----------------
try:
    import gspread
    from google.oauth2.service_account import Credentials

    print("\n🚀 Starting Google Sheets Sync...")
    
    # Configuration
    GSHEETS_CREDS = 'Credentials.json'
    SPREADSHEET_ID = '1loDazyGlqRnjv9SxYxd7yOTzF9eatCkjI0bsBHPTdpw'
    SHEET_NAME = 'Pack'
    
    # Authenticate
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GSHEETS_CREDS, scopes=scope)
    client = gspread.authorize(creds)
    
    # Open Sheet
    sheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = sheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        print(f"⚠️ Worksheet '{SHEET_NAME}' not found. Creating it...")
        worksheet = sheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)

    # Prepare Data
    # Replace NaN with empty string for JSON compliance
    df_clean = df.fillna('')
    
    # Convert datetime objects to string for JSON compliance
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)

    # Get headers and values
    # Get headers and values
    # headers = [df_clean.columns.values.tolist()] # No headers needed for appending
    values = df_clean.values.tolist()
    all_data_to_write = values
    
    num_rows = len(all_data_to_write)
    num_cols = len(df_clean.columns) if not df_clean.empty else 0
    
    # Calculate range, e.g., 'A34333:E...'
    # Function to convert col index to letter (0 -> A, 22 -> W)
    def col_to_letter(n):
        string = ""
        while n >= 0:
            string = chr(n % 26 + 65) + string
            n = n // 26 - 1
        return string

    last_col_letter = col_to_letter(num_cols - 1)
    
    # Target row from user request
    START_ROW = 33557
    target_range = f"A{START_ROW}:{last_col_letter}{START_ROW + num_rows}"
    
    print(f"Updating range {target_range} (Appended data)...")
    
    # Clear from START_ROW downwards to remove potential old data collision if needed
    # (Optional, but safe if we want to ensure "current month" replaces whatever was there from prev run)
    clear_range = f"A{START_ROW}:{last_col_letter}{worksheet.row_count}"
    worksheet.batch_clear([clear_range])
    
    # Update with new data
    worksheet.update(values=all_data_to_write, range_name=f"A{START_ROW}", value_input_option='USER_ENTERED')
    
    print("✅ Google Sheets update complete!")

except ImportError:
    print("⚠️ gspread library not found. Skipping Google Sheets sync.")
except Exception as e:
    print(f"❌ Google Sheets Sync Error: {e}")
