# import dependencies
import psycopg2
from psycopg2.extras import execute_values
import sshtunnel
import pandas

# py2edw
class py2edw:
    def __init__(self, db_params, ssh_params=False):
        self.ssh_params = ssh_params
        self.db_params = db_params
    
    def help(self):
        print("Avaliable Commands:")
        print("")
        print("close_connection()")
        print("example: py2edw.close_connection()")
        print("")
        print("show_tables()")
        print("example: py2edw.show_tables()")
        print("")
        print("sql_query('sql query here')")
        print("example: py2edw.sql_query('DROP TABLE table_name;')'")
        print("example: py2edw.sql_query('DELETE FROM table_name WHERE name = 'Jerry')")
        print("example: py2edw.sql_query('CREATE TABLE friends(id INT PRIMARY KEY, name VARCHAR(20), age INT NOT NULL', date TIMESTAMP);')")
        print("")
        print("import_DataFrame('sql query here')")
        print("example: py2edw.import_DataFrame('SELECT * FROM table_name')")
        print("")
        print("insert_DataFrame('table_name', pandas_df)")
        print("example: py2edw.insert_DataFrame('table_name', pandas_df)")
        print("")
        print("upsert_DataFrame('table_name', pandas_df, 'conflictCol', whereStatement)")
        print("example_1: py2edw.upsert_DataFrame('table_name', pandas_df, 'id', FALSE)")
        print("example_2: py2edw.upsert_DataFrame('table_name', pandas_df, 'id', 'WHERE excluded.dateCol > edw.dateCol')")
        print("explanation: excluded.dateCol is the panda's df col being inserted, edw.dateCol is the edw table's col")
        print("")
        
    def close_connection(self):
        #close conn and cursor
        if self.ssh_params == False:
            self.cursor.close()
            self.connection.close()
            print("py2edw: Connection Closed Successfully")
        else:
            self.cursor.close()
            self.connection.close()
            self.server.stop()
            print("py2edw: Connection Closed Successfully")
        
    def start_connection(self):
        if self.ssh_params == False:
            try:
                # establish connection
                self.connection = psycopg2.connect(**self.db_params)
                # enable autocommit
                self.connection.autocommit = True
                # establish cursor   
                self.cursor = self.connection.cursor()
                print("py2edw connection established")
            except:
                print("connection error")
            
        else:
            try:
                # SSH Tunnel
                self.server = sshtunnel.SSHTunnelForwarder(
                        (self.ssh_params['ssh_ip'], self.ssh_params['ssh_port']), #Remote server IP and SSH port
                        ssh_username = self.ssh_params['ssh_username'],
                        ssh_password = self.ssh_params['ssh_password'],
                        remote_bind_address=(self.ssh_params['remote_bind_ip'], self.ssh_params['remote_bind_port']))
                # start SSH Tunnel
                self.server.start()
                # define db params
                self.db_params.update({'port': self.server.local_bind_port})
                # establish connection
                self.connection = psycopg2.connect(**self.db_params)
                # enable autocommit
                self.connection.autocommit = True
                # establish cursor   
                self.cursor = self.connection.cursor()
                print("py2edw connection established")
            except:
                print("connection error")
    
    def show_tables(self):
        try:
            self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = self.cursor.fetchall()
            data = []
            for i in tables:
                data.append(i[0])
            return(data)
        except psycopg2.Error as e:
            print(e)
        
    def getCols(self, df):
        self.df = df
        s = "("
        s += ", ".join([str(i) for i in self.df.columns])
        s += ")"
        return(s)
    
    def get_excludedCols(self, df):
        self.df = df
        s = "(EXCLUDED."
        s += ", EXCLUDED.".join([str(i) for i in self.df.columns])
        s += ")"
        return(s)
    
    # py2edw functions
    
    def sql_query(self, sql_query):
        create_table_command = sql_query
        try:
            self.cursor.execute(create_table_command)
            print("Query Successful")
        except psycopg2.Error as e:
            print(e)
    
    def insert_DataFrame(self, table_name, df):
        sql_q = "INSERT INTO "+str(table_name)+" "+ self.getCols(df)+ " VALUES %s"
        try:
            execute_values(self.cursor, sql_q, df.values.tolist())
            print("Query Successful")
        except psycopg2.Error as e:
            print(e)
        
    def import_DataFrame(self, sql_query):
        self.cursor.execute(sql_query)
        colnames = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return(pandas.DataFrame(data, columns=colnames))
        
    def upsert_DataFrame(self, table_name, df, conflictCol, whereStatement):
        if whereStatement == False:
            whereStatement = ""
        else:
            whereStatement = " "+str(whereStatement)
        sql_q = "INSERT INTO "+str(table_name)+" AS edw "+self.getCols(df)+" VALUES %s "+"ON CONFLICT ("+str(conflictCol)+")"+" DO UPDATE SET "+self.getCols(df.drop(columns={conflictCol}))+" = "+self.get_excludedCols(df.drop(columns={conflictCol}))+whereStatement+";"
        try:
            execute_values(self.cursor, sql_q, df.values.tolist())
            print("Query Successful")
        except psycopg2.Error as e:
            print(e)