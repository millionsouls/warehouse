import argparse
import sys
import os
import matplotlib.pyplot as plt
import seaborn as sns

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

    def visualize_data(self):
        tables = self.storage.list_tables()
        if not tables:
            print("No tables found in the database.")
            return
        print("Available tables:")
        for idx, t in enumerate(tables, 1):
            print(f"{idx}. {t}")
        try:
            table_choice = int(input("Select a table to visualize (number): "))
            if 1 <= table_choice <= len(tables):
                table_name = tables[table_choice - 1]
                df = self.storage.read_table(table_name)
                print(f"Columns in '{table_name}':")
                for idx, col in enumerate(df.columns, 1):
                    print(f"{idx}. {col}")
                col1_idx = int(input("Select the first column (number): ")) - 1
                col2_idx = int(input("Select the second column (number): ")) - 1
                col1 = df.columns[col1_idx]
                col2 = df.columns[col2_idx]
                print(f"Selected: '{col1}' and '{col2}'.")

                # Suggest plot types based on data types
                numeric1 = df[col1].dtype.kind in 'biufc'
                numeric2 = df[col2].dtype.kind in 'biufc'

                print("\nChoose plot type:")
                plot_types = []
                if numeric1 and numeric2:
                    plot_types = ["scatter", "heatmap", "box"]
                elif numeric1 or numeric2:
                    plot_types = ["box", "bar"]
                else:
                    plot_types = ["bar"]

                for i, ptype in enumerate(plot_types, 1):
                    print(f"{i}. {ptype.capitalize()}")

                plot_choice = int(input("Select plot type (number): ")) - 1
                plot_type = plot_types[plot_choice]

                plt.figure(figsize=(10, 6))
                if plot_type == "scatter":
                    sns.scatterplot(data=df, x=col1, y=col2)
                    plt.title(f"Scatter: {col1} vs {col2}")
                elif plot_type == "box":
                    # Boxplot: categorical vs numeric
                    if numeric1 and not numeric2:
                        sns.boxplot(x=col2, y=col1, data=df)
                        plt.title(f"Boxplot: {col1} by {col2}")
                    else:
                        sns.boxplot(x=col1, y=col2, data=df)
                        plt.title(f"Boxplot: {col2} by {col1}")
                elif plot_type == "bar":
                    # Barplot: group by one column, aggregate the other
                    if numeric1 and not numeric2:
                        grouped = df.groupby(col2)[col1].mean().sort_values(ascending=False).head(20)
                        sns.barplot(x=grouped.index, y=grouped.values)
                        plt.xlabel(col2)
                        plt.ylabel(f"Mean {col1}")
                        plt.title(f"Mean {col1} by {col2}")
                    elif numeric2 and not numeric1:
                        grouped = df.groupby(col1)[col2].mean().sort_values(ascending=False).head(20)
                        sns.barplot(x=grouped.index, y=grouped.values)
                        plt.xlabel(col1)
                        plt.ylabel(f"Mean {col2}")
                        plt.title(f"Mean {col2} by {col1}")
                    else:
                        grouped = df.groupby(col1)[col2].count().sort_values(ascending=False).head(20)
                        sns.barplot(x=grouped.index, y=grouped.values)
                        plt.xlabel(col1)
                        plt.ylabel(f"Count of {col2}")
                        plt.title(f"Count of {col2} by {col1}")
                    plt.xticks(rotation=45, ha='right')
                elif plot_type == "heatmap":
                    # Heatmap: correlation or crosstab
                    pivot = pd.pivot_table(df, index=col1, columns=col2, aggfunc='size', fill_value=0)
                    sns.heatmap(pivot, cmap="YlGnBu")
                    plt.title(f"Heatmap: {col1} vs {col2}")

                plt.tight_layout()
                plt.show()
            else:
                print("Invalid selection.")
        except Exception as e:
            print(f"Error: {e}")

    def main_menu(self):
        while True:
            print("\nWarehouse CLI Options:")
            print("1. Process a file/URL")
            print("2. View data")
            print("3. Visualize data")
            print("4. Exit")
            choice = input("Select an option (1-4): ").strip()
            if choice == "1":
                self.process_file_or_url()
            elif choice == "2":
                self.view_data()
            elif choice == "3":
                self.visualize_data()
            elif choice == "4":
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
