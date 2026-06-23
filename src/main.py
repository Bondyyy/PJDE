from databases.mysql_connect import MySQLConnect
from config.database_config import get_mysql_config

def main(config):
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_conn:
        connection, cursor = mysql_conn.connect(), mysql_conn.cursor
        
    
if __name__ == "__main__":
    config = get_mysql_config()
    main(config)