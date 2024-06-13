from data_extractor import DataExtractor


# Usage example
pkl_file_path = './data/invoices_new.pkl'
expired_invoices_path = './data/expired_invoices.txt'  # Update this path accordingly

# Start Extraction Process
extractor = DataExtractor(pkl_file_path, expired_invoices_path)
extractor.load_data()
flat_data = extractor.transform_data()

# Save the transformed flat data to a new CSV file
output_csv_path = './data/transformed_invoices.csv'
extractor.save_to_csv(flat_data, output_csv_path)
