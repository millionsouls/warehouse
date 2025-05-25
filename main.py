import argparse
import sys
import os

from warehouse.loader import Loader
from warehouse.storage import Storage

class WarehouseCLI:
    def __init__(self, db_path="warehouse.db"):
        self.storage = Storage(db_path)

    def process_file_or_url(self):
        print(f"Current working directory: {os.getcwd()}")
        while True:
            source = input("Enter path to file or URL: ").strip()
            if source.startswith("http://") or source.startswith("https://"):
                df = Loader.load_from_url(source)
                break
            else:
                if not os.path.isfile(source):
                    print(f"File not found: {source}")
                    retry = input("Try again? (y/n): ").strip().lower()
                    if retry != "y":
                        return
                else:
                    df = Loader.load_from_file(source)
                    break

        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        if "arrest_date" in df.columns and "ofns_desc" in df.columns:
            df = df.dropna(subset=["arrest_date", "ofns_desc"])

        table_name = input("Enter table name to store data (default: arrests): ").strip() or "arrests"
        self.storage.write_table(table_name, df)
        print(f"Data loaded into {self.storage.db_path} (table: {table_name})")

    def view_data(self):
        tables = self.storage.list_tables()
        if not tables:
            print("No tables found in the database.")
            return
        print("Available tables:")
        for idx, t in enumerate(tables, 1):
            print(f"{idx}. {t}")
        try:
            choice = int(input("Select a table to view (number): "))
            if 1 <= choice <= len(tables):
                desc = self.storage.describe_table(tables[choice - 1])
                print(f"\nTable: {desc['table']}")
                print(f"Rows: {desc['rows']}")
                print(f"Columns: {desc['columns']}")
                print("Sample data:")
                print(desc['sample'])
                print()
            else:
                print("Invalid selection.")
        except ValueError:
            print("Invalid input.")

    def main_menu(self):
        while True:
            print("\nWarehouse CLI Options:")
            print("1. Process a file/URL")
            print("2. View data")
            print("3. Exit")
            choice = input("Select an option (1-3): ").strip()
            if choice == "1":
                self.process_file_or_url()
            elif choice == "2":
                self.view_data()
            elif choice == "3":
                print("Exiting.")
                break
            else:
                print("Invalid option. Please try again.")

def main():
    parser = argparse.ArgumentParser(description="Warehouse CLI")
    parser.add_argument("--cli", action="store_true", help="Start interactive CLI")
    args = parser.parse_args()

    if args.cli or len(sys.argv) == 1:
        WarehouseCLI().main_menu()
    else:
        print("For interactive mode, run: python3 main.py --cli")

if __name__ == "__main__":
    main()
