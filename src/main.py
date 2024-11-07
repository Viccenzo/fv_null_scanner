
import os
import dotenv
import psycopg2
import schedule
import time
from datetime import datetime

# Função para ler colunas ignoradas da tabela NULL_TABLE_IGNORE_STATUS
def get_ignore_columns(cursor,table):
    cursor.execute(f'SELECT row_name FROM "NULL_TABLE_IGNORE_STATUS" WHERE table_name = \'{table}\'')
    return [row[0] for row in cursor.fetchall()]

# Função para atualizar a tabela de status de NULLs
def update_null_table_status():
    try:
        dotenv.load_dotenv()
    
        server = os.getenv("POSTGRESQL_SERVERS").split(',')

        print(server)

        # Conectar ao banco de dados
        connection = psycopg2.connect(
            dbname=server[0],
            user=server[3],
            password=server[1],
            host=server[2],  # ou o IP do servidor PostgreSQL
            port="5432"
        )
        cursor = connection.cursor()

        # Obtém todas as tabelas do esquema 'public'
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
        """)
        tables = cursor.fetchall()
        print(tables)
        exit(0)
        # Itera por todas as tabelas
        for table in tables:
            #print(table[0])
            table_name = table[0]
            
            # Verifica se a tabela possui a coluna TIMESTAMP
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND column_name = 'TIMESTAMP'
                AND table_name NOT LIKE '%_OLD'
                AND table_name NOT LIKE '%_STATUS';
            """)
            has_timestamp = cursor.fetchone()
            
            if not has_timestamp:
                # Ignora a tabela se não tiver a coluna TIMESTAMP
                print(f"Tabela '{table_name}' ignorada (sem coluna TIMESTAMP)")
                continue

            # Obtém o nome das colunas da tabela
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            columns = [col[0] for col in cursor.fetchall()]
            
            # Seleciona o registro mais recente com base em um campo TIMESTAMP
            cursor.execute(f'SELECT * FROM "{table_name}" ORDER BY "TIMESTAMP" DESC LIMIT 1')
            latest_row = cursor.fetchone()
            
            if latest_row is None:
                # Ignora tabelas sem registros
                print(f"Tabela '{table_name}' ignorada (sem registros)")
                continue

            ignore_columns = get_ignore_columns(cursor,table_name)
            null_columns = []

            # Identificar colunas com valor NULL e que não estão na lista de colunas ignoradas
            for i, column in enumerate(columns):
                if latest_row[i] is None and column not in ignore_columns:
                    null_columns.append(column)

            print(f"ignore_columns: {ignore_columns}")
            print(f'null_columns: {null_columns}')

            # Inserir ou excluir do NULL_TABLE_STATUS com base nos null_columns encontrados
            if null_columns:
                # Inserir ou atualizar com as colunas NULL encontradas
                print(f'Inserted into table {table_name}: {null_columns}')
                cursor.execute("""
                    INSERT INTO "NULL_TABLE_STATUS" (table_name, null_rows)
                    VALUES (%s, %s)
                    ON CONFLICT (table_name) DO UPDATE 
                    SET null_rows = EXCLUDED.null_rows;
                """, (table_name, null_columns))
            else:
                # Excluir a entrada da tabela se não houver mais colunas NULL
                print(f'DELETE FROM "NULL_TABLE_STATUS" WHERE table_name = {table_name}')
                cursor.execute("""
                    DELETE FROM "NULL_TABLE_STATUS"
                    WHERE table_name = %s;
                """, (table_name,))

            # Confirma a transação
            connection.commit()

    except Exception as e:
        print(f"Erro: {e}")
    finally:
        cursor.close()
        connection.close()

# Configura o agendamento para rodar a cada 15 minutos
schedule.every(15).minutes.do(update_null_table_status)

print("Iniciando o monitoramento de NULLs nas tabelas...")

#update_null_table_status()

# Loop de execução do agendamento
while True:
    schedule.run_pending()
    time.sleep(1)