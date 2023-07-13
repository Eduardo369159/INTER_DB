import os
from sqlalchemy import create_engine,text,insert,Table,MetaData,select
from sqlalchemy.engine import URL
from dotenv import find_dotenv,load_dotenv
load_dotenv(find_dotenv())

class Dbs():
    def __init__(self):
        connection_str = {
            'database':os.getenv("database2"),
            'drivername':os.getenv("drivername2"),
            'host':os.getenv("host2"),
            'username':os.getenv("user2"),
            'password':os.getenv("password2")
        }

        self.metadata = MetaData()

        connection = URL.create(**connection_str)

        self.__engine = create_engine(connection,future=True)

    def execute(self,query):
        with self.__engine.begin() as conn:
            return conn.execute(text(query)).all()


    def insert(self,query,values):
      with self.__engine.begin() as conn:
          return conn.execute(text(query),values)
      
    def inserir(self, data):
        fato_rel_port = Table('fato_rel_port',self.metadata,autoload_with=self.__engine)
        stmt = insert(fato_rel_port)
        with self.__engine.begin() as conn:
            conn.execute(stmt, data)

    def excluir(self):
        sql ="""WITH CTE AS (
                SELECT id_proposta, 
                     ROW_NUMBER() OVER (PARTITION BY id_proposta ORDER BY id_proposta) AS row_num
                FROM lev..fato_rel_port
                )
                DELETE FROM CTE WHERE row_num > 1;"""
        with self.__engine.begin() as conn:
          return conn.execute(text(sql))


        

            
