import tkinter as tk
from tkinter import ttk, messagebox
from typing import List, Dict, Optional, Callable

from orion import execute_data_pipeline  #  Import the pipeline function

class OrionAuthPanel:
    def __init__(self, root: tk.Tk):
        """
        Initializes the Orion authentication and configuration panel.

        Args:
            root: The main Tkinter window.
        """

        self.root: tk.Tk = root  #  Explicitly type root
        self.root.title("Orion Data Pipeline Configuration")

        self.main_frame: ttk.Frame = ttk.Frame(self.root, padding="10")  #  Rename frame to main_frame
        self.main_frame.grid(row=0, column=0, sticky=(tk.W + tk.E + tk.N + tk.S))

        #  Configuration fields
        ttk.Label(self.main_frame, text="RACF:").grid(row=0, column=0, sticky=tk.W)
        self.racf_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)
        self.racf_entry.grid(row=0, column=1)

        ttk.Label(self.main_frame, text="Senha:").grid(row=1, column=0, sticky=tk.W)
        self.password_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30, show="*")  #  Rename to password_entry
        self.password_entry.grid(row=1, column=1)

        ttk.Label(self.main_frame, text="AWS Access Key:").grid(row=2, column=0, sticky=tk.W)
        self.aws_access_key_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)
        self.aws_access_key_entry.grid(row=2, column=1)

        ttk.Label(self.main_frame, text="AWS Secret Key:").grid(row=3, column=0, sticky=tk.W)
        self.aws_secret_key_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30, show="*")
        self.aws_secret_key_entry.grid(row=3, column=1)

        ttk.Label(self.main_frame, text="AWS Session Token:").grid(row=4, column=0, sticky=tk.W)
        self.aws_session_token_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)
        self.aws_session_token_entry.grid(row=4, column=1)

        ttk.Label(self.main_frame, text="AWS Region (default: sa-east-1):").grid(row=5, column=0, sticky=tk.W)
        self.aws_region_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)
        self.aws_region_entry.grid(row=5, column=1)

        ttk.Label(self.main_frame, text="Athena Database:").grid(row=6, column=0, sticky=tk.W)
        self.athena_database_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)  #  Rename to athena_database_entry
        self.athena_database_entry.grid(row=6, column=1)

        ttk.Label(self.main_frame, text="S3 Bucket:").grid(row=7, column=0, sticky=tk.W)
        self.s3_bucket_entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)  #  Rename to s3_bucket_entry
        self.s3_bucket_entry.grid(row=7, column=1)

        #  Dynamic table input
        ttk.Label(self.main_frame, text="Number of Tables:").grid(row=8, column=0, sticky=tk.W)
        self.num_tables_entry: ttk.Entry = ttk.Entry(self.main_frame, width=10)
        self.num_tables_entry.grid(row=8, column=1)

        self.generate_tables_button: ttk.Button = ttk.Button(
            self.main_frame, text="Generate Table Inputs", command=self.generate_table_entries
        )  #  Rename generate_tables_button
        self.generate_tables_button.grid(row=9, column=0, columnspan=2, pady=5)

        #  Logging area
        ttk.Label(self.main_frame, text="Logs:").grid(row=10, column=0, sticky=tk.W)
        self.log_text: tk.Text = tk.Text(self.main_frame, width=60, height=15, state=tk.DISABLED, wrap=tk.WORD)  #  Explicitly type log_text
        self.log_text.grid(row=11, column=0, columnspan=2, pady=5)

        #  Action buttons
        self.start_process_button: ttk.Button = ttk.Button(  #  Rename start_process_button
            self.main_frame, text="Start Data Pipeline", command=self.start_data_pipeline
        )
        self.start_process_button.grid(row=12, column=0, pady=10)

        self.close_panel_button: ttk.Button = ttk.Button(  #  Rename close_panel_button
            self.main_frame, text="Close Panel", command=self.close_panel
        )
        self.close_panel_button.grid(row=12, column=1, pady=10)

        self.table_entries: List[ttk.Entry] = []  #  List to hold table name entries
        self.pipeline_data: Optional[Dict[str, any]] = None  #  For storing collected data


    def generate_table_entries(self) -> None:
        """
        Generates input fields for table names based on the provided number.
        """

        try:
            num_tables: int = int(self.num_tables_entry.get())
            if num_tables <= 0:
                raise ValueError("Number of tables must be greater than zero.")

            #  Clear existing table entries
            for table_entry in self.table_entries:
                table_entry.destroy()
            self.table_entries.clear()

            #  Create new table entries
            for i in range(num_tables):
                label: ttk.Label = ttk.Label(self.main_frame, text=f"Table {i + 1}:")  #  Explicitly type label
                label.grid(row=13 + i, column=0, sticky=tk.W)
                entry: ttk.Entry = ttk.Entry(self.main_frame, width=30)  #  Explicitly type entry
                entry.grid(row=13 + i, column=1)
                self.table_entries.append(entry)

            #  Create "Start" button if it doesn't exist
            if not hasattr(self, 'start_button'):
                self.start_button: ttk.Button = ttk.Button(  #  Explicitly type start_button
                    self.main_frame, text="Start", command=self.start_data_pipeline
                )
                self.start_button.grid(row=13 + num_tables, column=0, columnspan=2, pady=10)

            self.update_idletasks() # Ensure UI update before messagebox (if any)


        except ValueError as e:
             messagebox.showerror("Input Error", str(e))
             self.log(str(e))


    def start_data_pipeline(self) -> None:
        """
        Collects user-provided data and initiates the data pipeline.
        """

        self.pipeline_data: Dict[str, any] = self.collect_user_data()  #  Explicitly type pipeline_data

        if self.pipeline_data:
            self.log("Data collected successfully. Starting pipeline...")
            self.log(str(self.pipeline_data))  #  Log the collected data for debugging
            try:
                execute_data_pipeline(self.pipeline_data, self.log)  #  Pass self.log to log
            except Exception as e:
                error_message = f"Error during pipeline execution: {str(e)}"
                self.log(error_message)
                messagebox.showerror("Pipeline Error", error_message)
        else:
             self.log("ERROR: Incomplete data. Please check input fields.")
             messagebox.showerror("Input Error", "Please ensure all fields are correctly filled.")

    def collect_user_data(self) -> Optional[Dict[str, any]]:
        """
        Collects data from input fields in the GUI panel.

        Returns:
            Dictionary containing user-provided data, or None if data is incomplete.
        """

        racf: str = self.racf_entry.get()
        password: str = self.password_entry.get()  #  Rename password
        aws_access_key: str = self.aws_access_key_entry.get()
        aws_secret_key: str = self.aws_secret_key_entry.get()
        aws_session_token: str = self.aws_session_token_entry.get()
        aws_region: str = self.aws_region_entry.get() or "sa-east-1"  #  Default region
        athena_database: str = self.athena_database_entry.get() # Rename athena_database
        s3_bucket: str = self.s3_bucket_entry.get()   #  Rename s3_bucket
        tables: List[str] = [entry.get() for entry in self.table_entries]

        if not all(
            [racf, password, aws_access_key, aws_secret_key, aws_session_token, aws_region, athena_database, s3_bucket]
        ) or not tables:  #  Check if tables list is empty
            return None  #  Indicate incomplete data