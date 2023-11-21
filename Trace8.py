from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
import pandas as pd
import sqlite3
from os import listdir, path
from os.path import isfile, join, splitext

app = FastAPI()

templates = Jinja2Templates(directory="templates")

fixed_table_name = "csv_data_table"

class DatabaseManager:
    def __init__(self, database_file: str):
        self.conn = sqlite3.connect(database_file)

    def close_connection(self):
        if self.conn:
            self.conn.close()

    def execute_query(self, query: str, params: tuple = ()):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
            return cursor
        except Exception as e:
            print(f"Error executing query: {str(e)}")

def startup_event():
    database_file = 'Trace3.db'

    if not path.exists(database_file):
        print(f"Creating SQLite database: {database_file}")
        open(database_file, 'a').close()

    app.db_manager = DatabaseManager(database_file=database_file)


def shutdown_event():
    app.db_manager.close_connection()


app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)


def csv_to_sql(csv_files, database_manager):
    try:
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {fixed_table_name} (
                -- Add your column definitions here, for example:
                BARCODE TEXT,
                COLUMN1 TEXT,
                COLUMN2 INTEGER,
                -- Add more columns as needed
                Judgement TEXT,
                source_file TEXT
            );
        '''
        database_manager.execute_query(create_table_query)

        for csv_file in csv_files:
            print(f"Processing CSV file: {csv_file}")
            df = pd.read_csv(csv_file)
            print("DataFrame content:")
            print(df.head())
            df['source_file'] = splitext(csv_file)[0]

            existing_query = f'SELECT DISTINCT BARCODE FROM {fixed_table_name};'
            existing_barcodes = set(pd.read_sql_query(existing_query, database_manager.conn)['BARCODE'])

            df_to_append = df[~df['BARCODE'].isin(existing_barcodes)]

            if not df_to_append.empty:
                df_to_append.to_sql(fixed_table_name, database_manager.conn, index=False, if_exists='append')

    except Exception as e:
        print(f"Error in csv_to_sql: {str(e)}")


def get_row_by_barcode(database_manager, barcode_value):
    try:
        query = f'SELECT * FROM {fixed_table_name} WHERE BARCODE = ?;'
        cursor = database_manager.execute_query(query, params=(barcode_value,))
        df_row = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        return df_row

    except Exception as e:
        print(f"Error in get_row_by_barcode: {str(e)}")
        return pd.DataFrame()


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse('index.html', {"request": request})


@app.post("/result/")
async def read_item(request: Request, barcode: str = Form(...)):
    current_folder = 'C:/Users/amogh/Amogh N Kotha RNSIT/TIEI Internship/Trace3'
    csv_files = [join(current_folder, f) for f in listdir(current_folder) if
                 isfile(join(current_folder, f)) and f.endswith('.csv')]

    if not csv_files:
        return {"error": "No CSV files found in the specified folder."}

    try:
        csv_to_sql(csv_files, app.db_manager)
        df_row = get_row_by_barcode(app.db_manager, barcode)

        if not df_row.empty:
            df_row_html = df_row.to_html(index=False, escape=False, classes="styled-table")
            df_row_html = df_row_html.replace('<td>OK</td>', '<td style="background-color: green;">OK</td>')
            df_row_html = df_row_html.replace('<td>None</td>', '<td style="background-color: yellow;">None</td>')
            df_row_html = df_row_html.replace('<td>BB</td>', '<td style="background-color: red;">BB</td>')

            result_data = df_row_html
        else:
            result_data = f"No row found for BARCODE {barcode} or table is empty."

    except Exception as e:
        result_data = f"Error: {str(e)}"

    return templates.TemplateResponse("result.html", {"request": request, "result_data": result_data})
