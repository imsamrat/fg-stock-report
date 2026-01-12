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
FROM_DATE = "2025-01-01 00:00:00"
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
    ("company_id", "in", [1, 3]),
    ("fg_balance", ">", 0)
    # ("work_center", "=", 7)
]

order_fields = [
    "write_date",
    "action_date",
    "fg_categ_type",
    "product_template_id",
    "oa_id",
    "shade",
    "sizcommon",
    "qty",
    "pack_qty",
    "finish",
    "slidercodesfg",
    "partner_id",
    "final_price",
    "company_id",
    "sales_person",
    "team_id",
    "buyer_name",
    "buyer_group",
    "fg_balance",
    "invoice_line_id",
    # "fg_stock_value",
    # "date_order",
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


# ---------------- BATCH FETCH PARTNER GROUPS ----------------
partner_ids = list({rec["partner_id"][0] for rec in records if rec.get("partner_id")})
partners = []
partner_map = {}
if partner_ids:
    partners = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_API_KEY or ODOO_PASSWORD,
        "res.partner",
        "read",
        [partner_ids],
        {"fields": ["id", "group"]},
    )
    partner_map = {
        p["id"]: {"customer_group": safe_field(p.get("group"))} for p in partners
    }

# ---------------- BATCH FETCH INVOICES ----------------
invoice_map = {}
try:
    # 1. Get all invoice_line_ids
    inv_line_ids = list({rec["invoice_line_id"][0] for rec in records if rec.get("invoice_line_id")})
    
    if inv_line_ids:
        print(f"🔍 Fetching {len(inv_line_ids)} invoice lines...")
        # 2. Fetch combine.invoice.line to get invoice_id
        # We need to read 'invoice_id' from 'combine.invoice.line'
        combine_lines = models.execute_kw(
            ODOO_DB, uid, ODOO_API_KEY or ODOO_PASSWORD,
            "combine.invoice.line", "read",
            [inv_line_ids],
            {"fields": ["invoice_id"]}
        )
        
        # 3. Collect invoice_ids
        # invoice_id is usually a tuple [id, name] or just id depending on read
        # In Odoo read result, many2one is (id, name)
        
        # We can map combine_line_id -> invoice_name directly if invoice_id has the name
        # But invoice_id name on combine line might be just "INV/2023/0001" which is perfect.
        # Let's check if we need to fetch account.move. 
        # Usually many2one tuple [id, display_name]. display_name of account.move IS the invoice number.
        
        for cl in combine_lines:
            if cl.get("invoice_id"):
                # cl["invoice_id"] is [id, name]
                invoice_map[cl["id"]] = cl["invoice_id"][1]
                
    print(f"✅ Mapped {len(invoice_map)} invoices")

except Exception as e:
    print(f"⚠️ Error fetching invoices: {e}")


# ---------------- BUILD DATA ----------------
all_data = []
for rec in records:
    partner_info = (
        partner_map.get(rec.get("partner_id")[0]) if rec.get("partner_id") else {}
    )

    # Get invoice number from map
    invoice_line_value = ""
    if rec.get("invoice_line_id"):
        inv_line_id = rec["invoice_line_id"][0]
        invoice_line_value = invoice_map.get(inv_line_id, "")

    all_data.append(
        {
            "last_updated": rec.get("write_date") or "",
            "action_date": format_date(rec.get("action_date")),
            "item": rec.get("fg_categ_type") or "",
            "product_name": safe_field(rec.get("product_template_id")),
            "oa": safe_field(rec.get("oa_id")),
            "shade": rec.get("shade") or "",
            "size": rec.get("sizcommon") or "",
            "qty": rec.get("qty") or "",
            "pack_qty": rec.get("pack_qty") or "",
            "finish": rec.get("finish") or "",
            "slider_code": rec.get("slidercodesfg") or "",
            "customer": safe_field(rec.get("partner_id")),
            "final_price": rec.get("final_price") or "",
            "company": safe_field(rec.get("company_id")),
            "salesperson": safe_field(rec.get("sales_person")),
            "team": safe_field(rec.get("team_id")),
            "invoice_no": invoice_line_value,
            "customer_group": partner_info.get("customer_group", ""),
            "buyer": safe_field(rec.get("buyer_name")),
            "buyer Group": safe_field(rec.get("buyer_group")),
            "pack_value": (rec.get("final_price") or 0.0) * (rec.get("qty") or 0.0),
            "fg_balance": rec.get("fg_balance") or "",
            "fg_stock_value": (rec.get("fg_balance") or 0.0) * (rec.get("final_price") or 0.0),
            # "oa_date": format_date(rec.get("date_order")),
        }
    )

# ---------------- EXPORT TO EXCEL ----------------
df = pd.DataFrame(all_data)

# Convert date columns to datetime format
date_columns = ["last_updated", "action_date"]
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

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
    SHEET_NAME = 'raw'
    
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
    headers = [df_clean.columns.values.tolist()]
    values = df_clean.values.tolist()
    all_data_to_write = headers + values
    
    num_rows = len(all_data_to_write)
    num_cols = len(headers[0])
    
    # Calculate range, e.g., 'A1:W500'
    # Function to convert col index to letter (0 -> A, 22 -> W)
    def col_to_letter(n):
        string = ""
        while n >= 0:
            string = chr(n % 26 + 65) + string
            n = n // 26 - 1
        return string

    last_col_letter = col_to_letter(num_cols - 1)
    target_range = f"A1:{last_col_letter}{num_rows}"
    
    print(f"Updating range {target_range} (preserving other columns)...")
    
    # Clear only the specific range we are updating
    # It's safer to clear the data content area first to remove old rows if the new data is shorter
    # But usually we just overwrite. To be safe against "leftover" rows if new data is shorter:
    # 1. Clear A1:W<MaxRows>
    # or just simple update if we assume data grows. 
    # Let's clear the specific columns A to W entirely to be safe?
    # No, clearing 'A:W' might be better.
    
    # Better approach:
    # 1. Clear just the columns we use.
    sheet_range = f"A1:{last_col_letter}{worksheet.row_count}"
    worksheet.batch_clear([sheet_range])
    
    # 2. Update with new data
    worksheet.update(values=all_data_to_write, range_name=f"A1")
    
    print("✅ Google Sheets update complete!")

except ImportError:
    print("⚠️ gspread library not found. Skipping Google Sheets sync.")
except Exception as e:
    print(f"❌ Google Sheets Sync Error: {e}")
