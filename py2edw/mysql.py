import mysql.connector
import sshtunnel
import pandas

class py2edw:
    """
    mysql connector
    """
    def __init__(self, db_params, ssh_params=False):
        self.db_params = db_params
        self.ssh_params = ssh_params
        # autoconnection
        self.autoconnection = True
        
        
    def _autoconn(foo):
        """
        decorator open/close connection wrap
        """
        def conn(self, *method_args, **method_kwargs):
            if self.autoconnection == True:
                try:
                    self.start_connection()
                    var = foo(self, *method_args, **method_kwargs)
                    self.close_connection()
                    return var
                except:
                    return error
            else:
                foo(self, *method_args, **method_kwargs)
        return conn
    
    def help(self):
        """
        print help method to console
        """
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
        print("upsert_DataFrame('table_name', pandas_df, 'conflictCol')")
        print("example_1: py2edw.upsert_DataFrame('table_name', pandas_df, 'id')")
        print("")
    
    def close_connection(self):
        """
        close conn, cursor and ssh connection
        """
        if self.ssh_params == False:
            self.cursor.close()
            self.connection.close()
            if self.autoconnection == False:
                    print("py2edw: Connection Closed Successfully")
        else:
            self.cursor.close()
            self.connection.close()
            self.server.stop()
            if self.autoconnection == False:
                    print("py2edw: Connection Closed Successfully")
    
    def start_connection(self):
        """
        start conn, cursor and ssh connection
        """
        if self.ssh_params == False:
            try:
                # establish connection
                self.connection = mysql.connector.connect(**self.db_params)
                # enable autocommit
                self.connection.autocommit = True
                # establish cursor
                self.cursor = self.connection.cursor()
                if self.autoconnection == False:
                    print("py2edw: Connection Established")
            except mysql.connector.Error as e:
                print(e)
        else:
            try:
                # convert strings to int
                self.ssh_params['ssh_port'] = int(self.ssh_params['ssh_port'])
                self.ssh_params['remote_bind_port'] = int(self.ssh_params['remote_bind_port'])
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
                self.connection = mysql.connector.connect(**self.db_params)
                # enable autocommit
                self.connection.autocommit = True
                # establish cursor
                self.cursor = self.connection.cursor()
                if self.autoconnection == False:
                    print("py2edw: Connection Established")
            except mysql.connector.Error as e:
                print(e)
    
    @_autoconn
    def import_DataFrame(self, query):
        try:
            self.cursor.execute(query)
            df_list = []
            for i in self.cursor:
                df_list.append(i)
            df = pandas.DataFrame(df_list, columns=list(self.cursor.column_names))
            return df
        except mysql.connector.Error as e:
            print(e)
            
    @_autoconn
    def show_tables(self):
        try:
            self.cursor.execute("SHOW TABLES")
            df_list = []
            for i in self.cursor:
                df_list.append(i)
            df = pandas.DataFrame(df_list, columns=list(self.cursor.column_names))
            return df
        except mysql.connector.Error as e:
            print(e)
    
    def getCols(self, df):
        s = "("
        s += ", ".join([str(i) for i in df.columns])
        s += ")"
        return(s)
    
    def getCol_proxy(self, df):
        s = "("
        s += ", ".join(['%s' for i in range(0,len(df.columns))])
        s += ")"
        return s
    
    def zipmap(self, df): 
        return list(zip(*map(df.get, df)))
    
    @_autoconn
    def sql_query(self, query):
        try:
            self.cursor.execute(query)
            for i in self.cursor:
                pass
            print("Query Successful")
        except mysql.connector.Error as e:
            print(e)
    
    @_autoconn        
    def insert_DataFrame(self, table_name, df):
        try:
            sql_q = "INSERT INTO "+str(table_name)+" "+ self.getCols(df)+ " VALUES {}".format(self.getCol_proxy(df))
            self.cursor.executemany(sql_q, self.zipmap(df))
        except mysql.connector.Error as e:
                print(e)
    
    def getCols_update(self, df):
        s = ""
        s += ", ".join([str(i)+"=VALUES("+str(i)+")" for i in list(df.columns)])
        return s
    
    @_autoconn
    def upsert_DataFrame(self, table_name, df, conflictCol):
        sql_q = "INSERT INTO "+str(table_name)+" "+self.getCols(df)+" VALUES "+self.getCol_proxy(df)+" ON DUPLICATE KEY UPDATE "+self.getCols_update(df.drop(columns={conflictCol}))
        try:
            self.cursor.executemany(sql_q, self.zipmap(df))
            print("Query Successful")
        except mysql.connector.Error as e:
                print(e)