import pandas as pd
import psycopg2
import yaml

class TransactionAnalysis:
    def __init__(self, file_path, config_path):
        self.df = pd.read_excel(file_path)
        self.config = self.load_config(config_path)
        self.conn = self.connect_to_postgres()

    def load_config(self, file_path):
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config

    def connect_to_postgres(self):
        conn_params = self.config['postgres']
        conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            database=conn_params['database'],
            user=conn_params['user'],
            password=conn_params['password']
        )
        return conn

    def close_connection(self):
        self.conn.close()

    def table_exists(self, table_name):
        # Check if the table exist or not
        query = """
        SELECT EXISTS (
           SELECT 1
           FROM information_schema.tables
           WHERE table_name = %s
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            return cursor.fetchone()[0]

    def create_table_if_not_exists(self, table_config):
        # Create table if not exist
        table_name = table_config['table_name']
        if not self.table_exists(table_name):
            with self.conn.cursor() as cursor:
                # Define column name and number of columns
                columns = ', '.join([f'"{col["name"]}" {col.get("data_type", "VARCHAR")}' for col in table_config['columns']])
                unique_constraint = table_config.get('unique_constraint', None)
                if unique_constraint:
                    unique_constraint_query = f", CONSTRAINT {table_name}_unique_constraint UNIQUE ({unique_constraint})"
                else:
                    unique_constraint_query = ""
                create_table_query = f"CREATE TABLE {table_name} ({columns}{unique_constraint_query});"
                print(f"Success Create Table {table_name}")
                cursor.execute(create_table_query)

    def ensure_tables_exist(self):
        # Check if tables exist and create them if not.
        for table_config in self.config['schema']:
            self.create_table_if_not_exists(table_config)

    def clean_data(self):
        # Drop unused columns
        self.df = self.df.drop(['.', 'BALANCE AMT'], axis=1)
        
        # Standardize column names
        self.df.columns = (
            self.df.columns.str.lower()
            .str.replace(' ', '_')
            .str.replace('.', '_')
            .str.rstrip('_')
        )
        # Cleaning account_no 
        self.df['account_no'] = self.df['account_no'].str.replace("'", '')

        # Auto fill 0 to NaN rows and sort values based on account_no & date
        self.df['deposit_amt'].fillna(0, inplace=True)
        self.df['withdrawal_amt'].fillna(0, inplace=True)
        self.df.sort_values(by=['account_no', 'date'], ascending=[True, True])

        # Create cummulative sum based on account_no & date
        self.df['balance_amt'] = (
            self.df.groupby('account_no')['deposit_amt'].cumsum()
            - self.df.groupby('account_no')['withdrawal_amt'].cumsum()
        )
        
        # Adding updated_at to ensure updates data
        self.df['updated_at'] = pd.to_datetime('now')

    def analyze_transactions(self, table_config):
        # Create aggregated tables for analysis
        transactions_summary = self.df.groupby('account_no').agg({
            'date': 'max',
            'deposit_amt': 'sum',
            'withdrawal_amt': 'sum',
            'account_no': 'count',
            'balance_amt': 'median'
        })

        # Renaming Columns
        transactions_summary = transactions_summary.rename(columns={
            'date': 'max_date',
            'deposit_amt': 'total_deposit',
            'withdrawal_amt': 'total_withdrawal',
            'account_no': 'transaction_count',
            'balance_amt': 'median_balance'
        }).reset_index()

        # Create summary balance_amount
        transactions_summary['balance_amt'] = transactions_summary['total_deposit'] - transactions_summary['total_withdrawal']

        # Adding updated_at to ensure updates data
        transactions_summary['updated_at'] = pd.to_datetime('now')
        print(transactions_summary)

        return transactions_summary


    def upsert_to_postgres(self, table_config, unique_id):
        cursor = self.conn.cursor()
        print("Connected to the database....")

        # Define column name and number of columns
        columns = ', '.join([f'"{col["name"]}"' for col in table_config['columns']])
        placeholders = ', '.join(['%s' for _ in table_config['columns']])

        # Construct the ON CONFLICT clause based on unique_id constraint
        unique_constraint = unique_id
        conflict_clause = f"ON CONFLICT ({unique_constraint}) DO UPDATE SET "
        update_values = ', '.join([f'"{col["name"]}"=EXCLUDED."{col["name"]}"' for col in table_config['columns']])
        
        # Generate upsert query
        insert_query = f"INSERT INTO {table_config['table_name']} ({columns}) VALUES ({placeholders}) {conflict_clause} {update_values};"

        # Run the query
        for index, row in self.analyze_transactions(table_config).iterrows():
            values = [row[col["name"]] for col in table_config['columns']]
            cursor.execute(insert_query, values)

        print("Success Upsert Data: transactions_summary")

        self.conn.commit()
        cursor.close()
        print("== Connection closed == \n")

    def append_to_historical_transactions(self, table_config):
        cursor = self.conn.cursor()
        print("Connected to the database....")

        # Define column name and number of columns
        columns = ', '.join([f'"{col["name"]}"' for col in table_config['columns']])
        placeholders = ', '.join(['%s' for _ in table_config['columns']])

        # Generate append query
        insert_query = f"INSERT INTO {table_config['table_name']} ({columns}) VALUES ({placeholders});"

        # Run the query
        for index, row in self.df.iterrows():
            # Handle Nan Values to insert as Null in Postgres
            values = [row[col["name"]] if pd.notna(row[col["name"]]) else None for col in table_config['columns']]
            cursor.execute(insert_query, values)

        print("Success Upsert Data: historical_transactions")
    
        self.conn.commit()
        cursor.close()
        print("== Connection closed == \n")


if __name__ == "__main__":
    # Define path
    import os

    current_path = os.getcwd()
    print(f"The current working directory is: {current_path}")

    file_path = '/usr/local/airflow/dags/common/question/[Confidential] Mekari - Lead Data Engineer.xlsx'
    config_path = '/usr/local/airflow/dags/common/schema/config.yaml'

    # Define table_name & unique_id
    transactions_summary = 'transactions_summary'
    historical_transactions = 'historical_transactions'
    unique_id = 'account_no'

    # Run the cleaning data
    transaction_analysis = TransactionAnalysis(file_path, config_path)
    transaction_analysis.clean_data()
    
    # Ensure that the required tables exist
    transaction_analysis.ensure_tables_exist()

    # Access the tables schema in config.yaml
    transactions_summary_config = next((config for config in transaction_analysis.config['schema'] if config['table_name'] == transactions_summary), None)
    historical_transactions_config = next((config for config in transaction_analysis.config['schema'] if config['table_name'] == historical_transactions), None)

    # Throw Error if schema not found
    if transactions_summary_config is None or historical_transactions_config is None:
        raise ValueError(f"Table configuration not found for table_name")

    # Upsert to transactions_summary table
    summary_df = transaction_analysis.analyze_transactions(transactions_summary_config)
    transaction_analysis.upsert_to_postgres(transactions_summary_config, unique_id)
    
    # Append to historical_transactions table
    transaction_analysis.append_to_historical_transactions(historical_transactions_config)

    # Close the connections
    transaction_analysis.close_connection()
