import sqlite3

class dbHandler:
    def __init__(self,dbPath):
        self.path=dbPath
        self.conn = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            conn = sqlite3.connect(self.path)
            self.conn = conn
            return self.conn
        except :
            print ('connection error')
            return -1

    def disconnect(self):
        if self.conn:
            self.conn.cursor().close()
            print("disconnected from database")

    def create(self, query):
        self.conn.cursor().execute(query)
        self.conn.commit()

    def select(self, query):
        cur = self.conn.cursor().execute(query)
        rs = cur.fetchall()
        self.disconnect()
        return rs





