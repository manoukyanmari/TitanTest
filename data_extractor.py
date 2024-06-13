import pickle
import datetime
import os
import csv
from dateutil.parser import parse

def ensure_str(value):
    """Ensure the value is treated as a string."""
    return str(value)


class DataExtractor:
    def __init__(self, filepath, expired_invoices_path):
        self.filepath = filepath
        self.expired_invoices_path = expired_invoices_path
        self.data = None
        self.expired_invoices = self.load_expired_invoices()

    def load_expired_invoices(self):
        """
        Load the expired invoices from a text file.
        """
        if not os.path.exists(self.expired_invoices_path):
            print(f"Expired invoices file not found at {self.expired_invoices_path}.")
            return set()

        with open(self.expired_invoices_path, 'r') as file:
            return set(line.strip() for line in file)

    def load_data(self):
        """
        Load the dataset from a pickle file.
        """
        with open(self.filepath, 'rb') as file:
            self.data = pickle.load(file)
        print(f"Data loaded successfully. Number of records: {len(self.data)}")

    def transform_data(self):
        """
        Transform the unstructured data into a flat structure with specified columns.

        Returns:
        list[dict]: The transformed data as a list of dictionaries.
        """
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        flat_data = []

        for record in self.data:
            invoice_id = ensure_str(record['id'])  # Ensure invoice_id is treated as string

            # Convert created_on with error handling
            try:
                created_on = parse(record['created_on']).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                print(
                    f"Invalid or missing date value: {record.get('created_on', 'None')} for invoice {invoice_id}. Assigning empty value.")
                created_on = ''

            is_expired = invoice_id in self.expired_invoices

            if 'items' not in record:
                print(f"No items found for invoice {invoice_id}")
                continue

            for item in record['items']:
                item_data = item['item']
                invoiceitem_id = ensure_str(item_data['id'])  # Ensure invoiceitem_id is treated as string
                invoiceitem_name = item_data['name']

                # Convert unit_price and quantity with error handling
                try:
                    unit_price = int(item_data['unit_price'])
                except ValueError:
                    print(f"Invalid unit_price value: {item_data['unit_price']} for invoice {invoice_id}")
                    continue

                item_type = type_conversion.get(item_data['type'], 'Other')

                try:
                    quantity = int(item['quantity'])
                except ValueError:
                    print(f"Invalid quantity value: {item['quantity']} for invoice {invoice_id}")
                    continue

                total_price = unit_price * quantity
                invoice_total = sum(int(i['item']['unit_price']) * int(i['quantity']) for i in record['items'] if
                                    str(i['quantity']).isdigit() and str(i['item']['unit_price']).isdigit())

                flat_record = {
                    'invoice_id': invoice_id,
                    'created_on': created_on,
                    'invoiceitem_id': invoiceitem_id,
                    'invoiceitem_name': invoiceitem_name,
                    'type': item_type,
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': total_price / invoice_total if invoice_total != 0 else 0,
                    'is_expired': is_expired
                }
                flat_data.append(flat_record)

        print(f"Data transformed successfully. Number of records: {len(flat_data)}")
        return sorted(flat_data, key=lambda x: (x['invoice_id'], x['invoiceitem_id']))

    def save_to_csv(self, data, output_file):
        """
        Save the transformed data to a CSV file.

        Args:
        data (list[dict]): The transformed data to save.
        output_file (str): The file path to save the data.
        """
        if not data:
            print("No data to save.")
            return

        # Ensure the directory exists
        directory = os.path.dirname(output_file)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory {directory} created.")

        # Write the data to the CSV file
        with open(output_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved to {output_file}")