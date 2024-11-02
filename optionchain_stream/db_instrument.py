import psycopg2
import json
import dotenv
import os
dotenv.load_dotenv()
class InstrumentDumpFetchDB():
    def __init__(self):
        # Retrieve database connection details from environment variables
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        # Connection to TimescaleDB/PostgreSQL
        self.conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        self.cursor = self.conn.cursor()

        # Create the table structure if it doesn't exist
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS instruments (
                symbol VARCHAR(50) PRIMARY KEY,
                instrument_data JSONB
            );
            
            CREATE TABLE IF NOT EXISTS option_data (
                tradingsymbol VARCHAR(50),
                token INTEGER,
                option_data JSONB,
                PRIMARY KEY (tradingsymbol, token)
            );
        """)
        self.conn.commit()

    def data_dump(self, symbol, instrument_data):
        """
        Dump specific exchange complete instrument data into TimescaleDB
        """
        try:
            self.cursor.execute(
                "INSERT INTO instruments (symbol, instrument_data) VALUES (%s, %s) ON CONFLICT (symbol) DO UPDATE SET instrument_data = %s;",
                (symbol, json.dumps(instrument_data), json.dumps(instrument_data))
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error - {e}")

    def symbol_data(self, symbol):
        """
        Return instrument detail for required symbol
        """
        try:
            self.cursor.execute("SELECT instrument_data FROM instruments WHERE symbol = %s;", (symbol,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                raise Exception(f"Key not found - {symbol}")
        except Exception as e:
            raise Exception(f"Error - {e}")

    def fetch_token(self, token):
        """
        Fetch contract name for requested instrument token
        """
        try:
            self.cursor.execute("SELECT option_data FROM option_data WHERE token = %s;", (token,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                raise Exception(f"Token not found - {token}")
        except Exception as e:
            raise Exception(f"Error - {e}")

    def store_optiondata(self, tradingsymbol, token, optionData):
        """
        Store option chain data for requested symbol
        """
        try:
            self.cursor.execute(
                "INSERT INTO option_data (tradingsymbol, token, option_data) VALUES (%s, %s, %s) ON CONFLICT (tradingsymbol, token) DO UPDATE SET option_data = %s;",
                (tradingsymbol, token, json.dumps(optionData), json.dumps(optionData))
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error - {e}")

    def fetch_option_data(self, tradingsymbol, token):
        """
        Fetch stored option data
        """
        try:
            self.cursor.execute("SELECT option_data FROM option_data WHERE tradingsymbol = %s AND token = %s;", (tradingsymbol, token))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                raise Exception(f"Data not found for tradingsymbol {tradingsymbol} and token {token}")
        except Exception as e:
            raise Exception(f"Error - {e}")

    def __del__(self):
        # Close database connection
        self.cursor.close()
        self.conn.close()
