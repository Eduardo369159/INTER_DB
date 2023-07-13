import os
from sqlalchemy import create_engine,text,insert,Table,MetaData,select, func
from sqlalchemy.engine import URL
from dotenv import find_dotenv,load_dotenv
load_dotenv(find_dotenv())

class Db():
    def __init__(self):
        connection_string = {
            'database':os.getenv("database"),
            'drivername':os.getenv("drivername"),
            'host':os.getenv("host"),
            'username':os.getenv("user"),
            'password':os.getenv("password")
        }

        self.metadata = MetaData()

        connection_url = URL.create(**connection_string)

        self.__engine = create_engine(connection_url,future=True)

    def execute(self,query):
        with self.__engine.begin() as conn:
            return conn.execute(text(query)).all()


    def insert(self,query,values):
      with self.__engine.begin() as conn:
          return conn.execute(text(query),values)
      
    def popular(self):
        query = """
            declare @limite date;
            set @limite = getdate()-3; 
            
            truncate table lev_main..fato_rel_port;
            
            insert into lev_main..fato_rel_port (id_proposta, Num_Proposta, NOM_SIGLA, CPF_CLIENTE, nmAgente, cdAgente, nome, dat_crc)
            select 
            	a.id_proposta, 
            	a.num_Proposta, 
            	b.NOM_SIGLA, 
            	a.CPF_CLIENTE, 
            	c.nmAgente, 
            	c.cdAgente, 
            	a.NOM_USUARIO_DIGITADOR as nome,
            	a.dat_crc
            from lev..DIM_PROPOSTA a
            	left join lev..dim_banco b 
            	on a.id_banco = b.id_banco
            	left join Controller..tbagente c 
            	on a.id_agente = c.cdagente
            where 
            	a.DAT_CRC >= @limite 
            
            WITH CTE AS (
              SELECT id_proposta, 
                     ROW_NUMBER() OVER (PARTITION BY id_proposta ORDER BY id_proposta) AS row_num
              FROM lev_main..fato_rel_port
            )
            DELETE FROM CTE WHERE row_num > 1;"""
        with self.__engine.begin() as conn:
          return conn.execute(text(query))
      
    def inserir(self,data):
        fato_rel_port = Table('fato_rel_port',self.metadata,autoload_with=self.__engine)
        stmt = insert(fato_rel_port).values(data)
        with self.__engine.begin() as conn: 
            conn.execute(stmt)
        
    def selected(self):
        fato_rel_port = Table('fato_rel_port', self.metadata, autoload_with=self.__engine)

        stmt = select(
            fato_rel_port.c.id_proposta,
            fato_rel_port.c.Num_Proposta,
            fato_rel_port.c.NOM_SIGLA,
            fato_rel_port.c.CPF_CLIENTE,
            fato_rel_port.c.nmAgente,
            fato_rel_port.c.cdAgente,
            fato_rel_port.c.nome,
            fato_rel_port.c.dat_crc
        ).select_from(fato_rel_port)


        query_contagem = select(func.count()).select_from(fato_rel_port)

        with self.__engine.begin() as conn:
            result = conn.execute(query_contagem)
            count = result.scalar()

        page_size = 1000

        num_pages = count // page_size + (count % page_size > 0)
        
        for x in range(num_pages):
            start = x * page_size
            end = min((x + 1) * page_size, count)
        
            query = stmt.order_by(fato_rel_port.c.id_proposta).offset(start).limit(end)

            with self.__engine.begin() as conn:
                result = conn.execute(query)
                batch = result.fetchall()
            
            yield batch

        

            
