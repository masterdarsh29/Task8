import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from sqlalchemy import create_engine, Integer, String, Column, Table, MetaData
from sqlalchemy.sql import text
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch data from a URL with retry logic
def fetch_data(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            if response.status_code == 429:
                logging.warning(f"Rate limit hit for URL {url}. Retrying after {delay} seconds...")
                time.sleep(delay)
                continue
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching data from URL {url}: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to fetch data from URL {url} after {retries} attempts.")

# Function to process and save data to the database
def process_and_save_data(symbol, symbol_to_name, engine):
    urls = [
        f'https://screener.in/company/{symbol}/consolidated/',
        f'https://screener.in/company/{symbol}/'
    ]

    data_fetched = False
    for url in urls:
        html_content = fetch_data(url)

        try:
            soup = bs(html_content, 'html.parser')
            profit_loss_section = soup.find('section', id="profit-loss")
            if not profit_loss_section:
                logging.warning(f"Failed to find the profit-loss section for symbol {symbol} at URL {url}.")
                continue

            table = profit_loss_section.find("table")
            if not table:
                logging.warning(f"Failed to find the table in the profit-loss section for symbol {symbol} at URL {url}.")
                continue

            table_data = []
            for row in table.find_all('tr'):
                row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
                table_data.append(row_data)

            df_table = pd.DataFrame(table_data)
            if df_table.empty:
                logging.warning(f"No data found for symbol {symbol}.")
                continue

            df_table.columns = df_table.iloc[0]
            df_table = df_table[1:]
            df_table.reset_index(drop=True, inplace=True)
            df_table.insert(0, 'id', range(1, len(df_table) + 1))
            df_table.insert(1, 'Narration', df_table.iloc[:, 1])
            df_table = df_table.drop(df_table.columns[2], axis=1)
            df_table.insert(2, 'company_name', symbol_to_name.get(symbol, 'Unknown Company'))

            columns = ['id'] + [col for col in df_table.columns if col != 'id']
            df_table = df_table[columns]

            company_name = symbol_to_name.get(symbol, 'Unknown Company')
            logging.info(f"Company: {company_name}")
            logging.info(df_table.head())

            if 'TTM' in df_table.columns:
                df_ttm = df_table[['Narration', 'TTM', 'company_name']].copy()
                df_ttm = df_ttm.reset_index(drop=True)
                df_table = df_table.drop(columns=['TTM'])
            else:
                logging.info(f"'TTM' column not found for symbol {symbol}. Skipping TTM extraction.")
                df_ttm = pd.DataFrame(columns=['Narration', 'TTM', 'company_name'])

            df_table = df_table.drop(df_table.columns[0], axis=1)
            df_melted = pd.melt(df_table, id_vars=['Narration', 'company_name'], var_name='Year', value_name='Value')
            df_melted = df_melted.sort_values(by=['Narration', 'Year']).reset_index(drop=True)

            consolidated_financials_table = Table('consolidated_financials', MetaData(),
                Column('id', Integer, primary_key=True),
                Column('Narration', String),
                Column('company_name', String),
                Column('Year', String),
                Column('Value', String)
            )

            ttm_financials_table = Table('ttm_financials', MetaData(),
                Column('id', Integer, primary_key=True),
                Column('Narration', String),
                Column('TTM', String),
                Column('company_name', String)
            )

            consolidated_financials_table.create(engine, checkfirst=True)
            ttm_financials_table.create(engine, checkfirst=True)

            df_melted.to_sql('consolidated_financials', con=engine, if_exists='append', index=False)
            df_ttm.to_sql('ttm_financials', con=engine, if_exists='append', index=False)

            logging.info("Data loaded successfully into SQLite database!")
            data_fetched = True
            break

        except Exception as e:
            logging.error(f"An error occurred for symbol {symbol} at URL {url}: {e}")

    if not data_fetched:
        logging.warning(f"Data for symbol {symbol} could not be fetched from any URL.")

def main():
    try:
        df_symbols = pd.read_csv('company.csv')
        logging.info("Column names in CSV file: %s", df_symbols.columns)

        if 'Symbol' in df_symbols.columns and 'Company Name' in df_symbols.columns:
            symbols = df_symbols['Symbol'].tolist()
            company_names = df_symbols['Company Name'].tolist()
        else:
            logging.error("Error: The 'Symbol' or 'Company Name' columns do not exist in the CSV file.")
            return

        symbol_to_name = dict(zip(symbols, company_names))

        # SQLite connection
        db_file = 'scrap.db'
        engine = create_engine(f'sqlite:///{db_file}')

        for symbol in symbols:
            process_and_save_data(symbol, symbol_to_name, engine)

        # Display ttm_financials data with company name
        with engine.connect() as connection:
            ttm_financials_data = pd.read_sql_table('ttm_financials', connection)
            print(ttm_financials_data)

    except FileNotFoundError:
        logging.error("Error: The file 'company.csv' does not exist.")
    except Exception as e:
        logging.error("An error occurred: %s", e)

if __name__ == "__main__":
    main()


