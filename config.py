from conection_199 import Dbs
from connection import Db

dbs = Dbs()
db = Db()

#print('')
#print('Populando a Base de Dados . . .')
#print('')
#
##db.popular()
#
print('')
print('Iniciando Processo de Inserção . . .')
print('')

carga = db.selected()
for index in carga:
    count = 0
    data = []
    for i in index:
        data.append ({
            'id_proposta': str(i[0]),
            'Num_Proposta': str(i[1]),
            'NOM_SIGLA': str(i[2]),
            'CPF_CLIENTE': str(i[3]),
            'nmAgente': str(i[4]),
            'cdAgente': str(i[5]),
            'nome': str(i[6]),
            'dat_crc': str(i[7])
            })
        count+=1
    print(data)
    dbs.inserir(data)

print('')
print('Excluindo Duplicatas . . .')
print('')

dbs.excluir()





