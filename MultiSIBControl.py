from bs4 import BeautifulSoup
import time, re, logging, os, requests, json, mysql.connector
from mysql.connector import Error
from datetime import datetime
from logging.handlers import RotatingFileHandler

dir_path = os.path.dirname(os.path.realpath(__file__))
log_fname = os.path.join(dir_path, 'ReadFromMultiSIBControl.log')
ConfigFilePath = os.path.join(dir_path, 'ConfigMultiSIBControl.json')

with open(ConfigFilePath) as user_file:
    file_contents = user_file.read()
Config = json.loads(file_contents)

#Logging Config
logging.basicConfig(
                    filename= log_fname,
                    encoding="utf-8",
                    filemode="a",
                    format="{asctime} - {levelname} - {message}",
                    style="{",
                    datefmt="%Y-%m-%d %H:%M"
                    )

handler = RotatingFileHandler(log_fname, maxBytes=2*1024*1024, backupCount=5)
logging.getLogger().addHandler(handler)

def create_connection(host, user, password, database):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host,  
            user = user, 
            password = password, 
            database = database
        )
        logging.info("Successful connection to the database.")
    except Error as e:
        logging.error("Error while connecting to the database: " + str(e))
    return connection

def insert_data(connection, data, table):
    cursor = connection.cursor()
    sql = f"""
    INSERT INTO {table} (timestamp, P1_V, P1_A, P1_SOC, P1_TEMP, P1_REMAIN_AH, P1_IMBALANCE, 
                              P2_V, P2_A, P2_SOC, P2_TEMP, P2_REMAIN_AH, P2_IMBALANCE, 
                              P3_V, P3_A, P3_SOC, P3_TEMP, P3_REMAIN_AH, P3_IMBALANCE, 
                              Pylon_SOC, Pylon_W, Pylon_A, Pylon_V, Pylon_Temp, Pylon_Remain_AH, 
                              Pylon_Remain_kWh, Inverter_Load_W, Inverter_Load_Percent, 
                              Inverter_Grid_W, Inverter_Grid_V, Inverter_PV_W) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, data)
    connection.commit()
    cursor.close()

def fetch_and_parse_data(connection, APIKey, AddressIP, Port, table):
    url = f'http://{AddressIP}:{Port}//?GetLiveData&APIKey={APIKey}'
    response = requests.get(url)
    
    # Acquire data only if the page is loading
    if response.status_code == 200:
        html_content = response.text
        
        # Parsing of the HTML using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Initialize a dictionary to store data
        data = {
            'timestamp': datetime.now(),
            'P1_V': None, 'P1_A': None, 'P1_SOC': None, 'P1_TEMP': None, 'P1_REMAIN_AH': None, 'P1_IMBALANCE': None,
            'P2_V': None, 'P2_A': None, 'P2_SOC': None, 'P2_TEMP': None, 'P2_REMAIN_AH': None, 'P2_IMBALANCE': None,
            'P3_V': None, 'P3_A': None, 'P3_SOC': None, 'P3_TEMP': None, 'P3_REMAIN_AH': None, 'P3_IMBALANCE': None,
            'Pylon_SOC': None, 'Pylon_W': None, 'Pylon_A': None, 'Pylon_V': None, 'Pylon_Temp': None, 'Pylon_Remain_AH': None,
            'Pylon_Remain_kWh': None,
            'Inverter_Load_W': None, 'Inverter_Load_Perc': None, 'Inverter_Grid_W': None, 'Inverter_Grid_V': None, 'Inverter_PV_W': None
        }

        # Function to remove units of measurement and convert values to float
        # for some reason the 
        def clean_value(value):
            value = value.strip()
            value = re.sub(r'[A-Za-z°%]', '', value)  # Remove char and symbols
            value = value.replace(',', '.')  # Replaces comma with dot
            try:
                return float(value)
            except ValueError:
                return value 

        # Find <p> tag to extract the key and value
        for item in soup.find_all('p'):
            strong_tag = item.find('strong')
            value_span = item.find(class_='data-value')
            
            if strong_tag and value_span:
                key = strong_tag.text.strip().replace(':', '')
                value = clean_value(value_span.text)

                if key in data and not key == "timestamp":
                    data[key] = value
                else:
                     logging.debug("Missing the key : "+key)

        # Create a tuple with the value
        data_tuple = (
            data['timestamp'],
            data['P1_V'], data['P1_A'], data['P1_SOC'], data['P1_TEMP'], data['P1_REMAIN_AH'], data['P1_IMBALANCE'],
            data['P2_V'], data['P2_A'], data['P2_SOC'], data['P2_TEMP'], data['P2_REMAIN_AH'], data['P2_IMBALANCE'],
            data['P3_V'], data['P3_A'], data['P3_SOC'], data['P3_TEMP'], data['P3_REMAIN_AH'], data['P3_IMBALANCE'],
            data['Pylon_SOC'], data['Pylon_W'], data['Pylon_A'], data['Pylon_V'], data['Pylon_Temp'], data['Pylon_Remain_AH'],
            data['Pylon_Remain_kWh'],
            data['Inverter_Load_W'], data['Inverter_Load_Perc'], data['Inverter_Grid_W'], data['Inverter_Grid_V'], data['Inverter_PV_W']
        )
        logging.debug(data)
        logging.debug(data_tuple)

        #Verify that the tuple has enough variables 
        if len(data_tuple) == 31:
            try:
                insert_data(connection, data_tuple, table)
            except Exception as e:
                logging.error("Error: "+str(e))
        else:
             logging.info("Invalid number of parameters")
    else:
         logging.error("HTTP error "+ response.status_code + " during the loading of the page")

# Get values from json config file
AddressIP = Config["MultiSIBControl"]["AddressIP"]
APIKey = Config["MultiSIBControl"]["APIKey"]
Port = Config["MultiSIBControl"]["Port"]
table = Config["DB"]["table"]
host = Config["DB"]["host"]
user = Config["DB"]["user"]
database  = Config["DB"]["database"]
password = Config["DB"]["password"]

try:
    connection = create_connection(host, user, password, database)
    while True:
        fetch_and_parse_data(connection, APIKey, AddressIP, Port, table)
        time.sleep(2)
except KeyboardInterrupt:
     logging.info("KeyboardInterrupt detected")
finally:
    if connection:
        connection.close() 