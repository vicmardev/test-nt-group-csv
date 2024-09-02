import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
def readCSV(filepath):
    df = pd.read_csv(filepath)
    return df
    

def connectionMysql():
    try:
        connection = mysql.connector.connect(
            host = 'localhost',
            database = 'nt_group_1',
            user = 'vicdev',
            password = '1234',
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        if connection.is_connected():
            print ('Conexión exitosa')
            return connection

    except Error as e:
        print(f"Error al hacer la conexión{e}")
        return None

def insertar(conexion, df):
    cursor = conexion.cursor()
    try:
        cursor.execute("TRUNCATE TABLE csv_data")
        conexion.commit()
        print("Tabla truncada exitosamente.")
    except mysql.connector.Error as e:
        print(f"Error al truncar la tabla: {e}")
        conexion.rollback()  # Revertir cambios si falla el truncamiento
        return
    
    for index, row in df.iterrows():
        queryInsert= "INSERT INTO csv_data(id_data, company_name,company_id,amount, status, created_at, update_at) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(queryInsert, tuple(row))
    conexion.commit()

def get_unique_companies(df):
    unique_companies = df[['company_id', 'name']].drop_duplicates()
    return unique_companies

def insert_companies(connection, companies_df):
    if not companies_df.empty:
        with connection.cursor() as cursor:
            query = """
            INSERT IGNORE INTO companies ( company_name, company_id)
            VALUES (%s, %s)
            """
            for index, row in companies_df.iterrows():
                try:
                    cursor.execute(query, (row['name'], row['company_id']))
                    connection.commit()
                    print(f"Compañía insertada: {row['name']}")
                except Error as e:
                    print(f"Error al insertar la compañía {row['name']}: {e}")
                    connection.rollback()

def insert_charges(conexion, df):
    cursor = conexion.cursor()
    cursor.execute("SELECT id_company, company_id FROM companies")
    company_mapping = {company_id: id_company for id_company, company_id in cursor.fetchall()}
    query = """
    INSERT INTO charges (id_company, amount, status, created_at, update_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    for index, row in df.iterrows():
        id_company = company_mapping.get(row['company_id'])
        if id_company:
            data = (id_company, row['amount'], row['status'], row['created_at'], row['paid_at'])
            try:
                cursor.execute(query, data)
            except Error as e:
                print(f"Error al insertar registro para company_id {row['company_id']}: {e}")
                conexion.rollback()
                continue
    conexion.commit()
    cursor.close()

def cerrarConexion(conexion):
    if conexion.is_connected():
        conexion.close()
        print("Conexion cerrada")


def main():
    path = "./data_prueba_tecnica.csv"
    df = readCSV(path)
    conexion = connectionMysql()
    if conexion:
        insertar(conexion, df)
        unique_companies = get_unique_companies(df)
        insert_companies(conexion, unique_companies)
        insert_charges(conexion, df)
        cerrarConexion(conexion)

if __name__ == '__main__':
    main()