import psycopg2
from psycopg2.extras import RealDictCursor
from library.logger import create_logger

class DataBase:
    def __init__(self, host: str, port: str, user: str, password: str, database: str, logger=None):
        if logger is None:
            self.logger = create_logger()
        else:
            self.logger = logger
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.logger.info(f"host: {host}, port: {port}, user: {user}, password: {password}, database: {database}")

        self.connection = self.create_connection()

    def create_connection(self):
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            self.logger.info(f"Connection to {self.database} successful")
        except Exception as ex:
            self.log_error_connect = str(ex)
            self.logger.error(ex)

        return connection

    def close_connection(self):
        self.connection.close()
        self.logger.info(f"Connection to {self.database} closed")

    def execute_query(self, query: str) -> list:
        result = []
        self.logger.info(f"Executing query: {query}")
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                result = cursor.fetchall() # return format: [{'id': '1', 'age': '18'}, {'id': '2', 'age': '26'}] (из-за RealDictCursor)
            self.logger.info(f"Query executed successfully")
        except Exception as ex:
            self.logger.error(ex)
        return result

    def insert(self, schema: str, table_name: str, columns_list: list, data: tuple) -> bool:
        self.logger.info(f"Inserting into {schema}.{table_name}")
        try:
            values_str = ', '.join(['%s'] * len(columns_list))
            columns_str = ', '.join([column for column in columns_list])
            sql = """
                INSERT INTO {schema}.{table_name} 
                    ({columns_str})
                VALUES 
                    ({values_str});
                    """.format(schema=schema, table_name=table_name, columns_str=columns_str, values_str=values_str)
            self.logger.info(f"SQL:\n {sql}")
            with self.connection.cursor() as cursor:
                cursor.execute(sql, data)
            # Сохранение изменений
            self.connection.commit()
            self.logger.info(f"Inserted {data} into {schema}.{table_name} successfully")
            return True
        except Exception as ex:
            self.logger.error(ex)
        return False

    def delete(self, schema: str, table_name: str, filter: str) -> bool:
        self.logger.info(f"Deleting from {schema}.{table_name}")
        sql = "DELETE FROM {schema}.{table_name} WHERE {filter};".format(schema=schema, table_name=table_name, filter=filter)
        self.logger.info(f"SQL:\n {sql}")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            self.logger.info(f"Deleted {filter} from {schema}.{table_name} successfully")
            return True
        except Exception as ex:
            self.logger.error(ex)
        return False
