"""Generate safe synthetic E-commerce Orders CSV with Project 07's source headers.

No original buyer records are read. All values are fabricated. The output is
local-only by default and ignored by Git; never publish private Workshop data.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

SOURCE_COLUMNS = (
    "Order ID", "Order Status", "Order Substatus", "Cancelation/Return Type",
    "Normal or Pre-order", "SKU ID", "Seller SKU", "Product Name",
    "Variation", "Quantity", "Sku Quantity of return", "SKU Unit Original Price",
    "SKU Subtotal Before Discount", "SKU Platform Discount",
    "SKU Seller Discount", "SKU Subtotal After Discount",
    "Shipping Fee After Discount", "Original Shipping Fee",
    "Shipping Fee Seller Discount", "Shipping Fee Platform Discount",
    "Payment platform discount", "Taxes", "Order Amount",
    "Order Refund Amount", "Created Time", "Paid Time", "RTS Time",
    "Shipped Time", "Delivered Time", "Cancelled Time", "Cancel By",
    "Cancel Reason", "Fulfillment Type", "Warehouse Name", "Tracking ID",
    "Delivery Option", "Shipping Provider Name", "Buyer Message",
    "Buyer Username", "Recipient", "Phone #", "Zipcode", "Country",
    "Province", "District", "Districts", "Detail Address",
    "Additional address information", "Payment Method", "Weight(kg)",
    "Product Category", "Package ID", "Seller Note", "Checked Status",
    "Checked Marked by", "Order Channel", "Creator Handle",
    "Request Tax Invoice", "Tax Info - Buyer Tax ID", "Tax Info - Type",
    "Tax Info - Full Name of Buyer", "Tax Info - Email",
    "Tax Info - Phone Number", "Tax Info - Registered Address",
    "Tax Info - Address Type",
)

assert len(SOURCE_COLUMNS) == 65 and len(set(SOURCE_COLUMNS)) == 65
DEFAULT_FILE = Path(__file__).resolve().parents[1] / "data" / "synthetic_orders.csv"


def make_order(index: int) -> dict[str, str]:
    """Fabricate one consistent order line; names, emails and numbers are fake."""
    if type(index) is not int or index < 1:
        raise ValueError("index must be a positive integer")
    created = datetime(2026, 1, 1, 9, 0) + timedelta(minutes=index)
    amount = (index % 19 + 1) * 100
    qty = index % 4 + 1
    status = ("Completed", "Processing", "Cancelled")[index % 3]
    order_id = f"SYN-ORDER-{index:08d}"
    sku_id = f"SYN-SKU-{index % 53 + 1:04d}"
    row = {key: "" for key in SOURCE_COLUMNS}
    row.update({
        "Order ID": order_id,
        "Order Status": status,
        "Order Substatus": "Synthetic",
        "Normal or Pre-order": "Normal",
        "SKU ID": sku_id,
        "Seller SKU": f"DEMO-{index % 53 + 1:04d}",
        "Product Name": f"Demo Product {index % 53 + 1:04d}",
        "Variation": "Standard",
        "Quantity": str(qty),
        "Sku Quantity of return": "0",
        "SKU Unit Original Price": f"{amount / qty:.2f}",
        "SKU Subtotal Before Discount": f"{amount:.2f}",
        "SKU Platform Discount": "0.00",
        "SKU Seller Discount": "0.00",
        "SKU Subtotal After Discount": f"{amount:.2f}",
        "Shipping Fee After Discount": "0.00",
        "Original Shipping Fee": "0.00",
        "Shipping Fee Seller Discount": "0.00",
        "Shipping Fee Platform Discount": "0.00",
        "Payment platform discount": "0.00",
        "Taxes": "0.00",
        "Order Amount": f"{amount:.2f}",
        "Order Refund Amount": "0.00",
        "Created Time": created.strftime("%d/%m/%Y %H:%M:%S"),
        "Paid Time": (created + timedelta(minutes=5)).strftime("%d/%m/%Y %H:%M:%S")
                     if status == "Completed" else "",
        "RTS Time": (created + timedelta(hours=1)).strftime("%d/%m/%Y %H:%M:%S")
                    if status == "Completed" else "",
        "Shipped Time": (created + timedelta(hours=2)).strftime("%d/%m/%Y %H:%M:%S")
                        if status == "Completed" else "",
        "Delivered Time": (created + timedelta(days=2)).strftime("%d/%m/%Y %H:%M:%S")
                          if status == "Completed" else "",
        "Cancelled Time": (created + timedelta(minutes=15)).strftime("%d/%m/%Y %H:%M:%S")
                          if status == "Cancelled" else "",
        "Fulfillment Type": "Seller",
        "Warehouse Name": "Synthetic Warehouse",
        "Tracking ID": f"SYN-TRACK-{index:08d}",
        "Delivery Option": "Standard",
        "Shipping Provider Name": "Demo Carrier",
        "Buyer Username": f"synthetic_buyer_{index:08d}",
        "Recipient": f"Demo Recipient {index}",
        "Phone #": "",  # PII fields intentionally blank
        "Zipcode": "",
        "Country": "Demo Country",
        "Province": "Demo Province",
        "District": "Demo District",
        "Districts": "Demo Subdistrict",
        "Detail Address": "",  # PII fields intentionally blank
        "Payment Method": "Demo Payment",
        "Weight(kg)": "0.50",
        "Product Category": "Synthetic Product",
        "Package ID": f"SYN-PKG-{index:08d}",
        "Checked Status": "Demo",
        "Order Channel": "Demo Marketplace",
        "Request Tax Invoice": "No",
    })
    return row


def generate_orders(count: int, destination: Path) -> Path:
    if type(count) is not int or not 1 <= count <= 10000:
        raise ValueError("count must be 1-10000")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        for index in range(1, count + 1):
            writer.writerow(make_order(index))
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=5249)
    parser.add_argument("--output", type=Path, default=DEFAULT_FILE)
    args = parser.parse_args()
    path = generate_orders(args.rows, args.output)
    print(f"SYNTHETIC_DATA_CREATED={path}")
    print(f"DATA_ROWS={args.rows}")
    print(f"COLUMNS={len(SOURCE_COLUMNS)}")
    print("RAW_CUSTOMER_DATA_USED=False")


if __name__ == "__main__":
    main()
