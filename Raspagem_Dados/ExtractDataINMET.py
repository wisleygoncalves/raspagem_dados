from datetime import datetime
import pandas as pd
import os
import psycopg2
from io import StringIO

class ExtractDataINMET(object):
    path_base_files = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Dados Metereológicos"

    def __init__(self):
        print('Obtendo dados metereologicos...')
        pass

    def control_manipulate_data(self):
        for ano in range(2013, 2025, 1):
            path_dir = os.path.join(self.path_base_files, f'MG_{ano}')

            for file in os.listdir(path_dir):
                path_file = os.path.join(path_dir, file)
                id_estacao = file.split('_')[3]
                print(file, '---------------')

                data = self.get_data_plain(path_file, id_estacao, ano)

                # for index, row in data.iterrows():
                this_exist = self.check_exist(ano, id_estacao, '')

                if this_exist == 0:
                    print(data)
                    self.insert_data(data)                
    
    def get_data_plain(self, path_file, id_estacao, ano):
        df = pd.read_csv(path_file,
                         encoding='ISO-8859-1',
                         sep=';',
                         skiprows=8,
                         header=0,
                         on_bad_lines='skip')
        
        precipitation = pd.to_numeric(df.iloc[:, 2].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        temperatura = pd.to_numeric(df.iloc[:, 7].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        humidity = pd.to_numeric(df.iloc[:, 15].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        wind_grau = pd.to_numeric(df.iloc[:, 16].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        wind_vel = pd.to_numeric(df.iloc[:, 18].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        date = pd.to_datetime(df.iloc[:, 0], errors='coerce')
        hours = df.iloc[:, 1]

        contains_utc_count = hours.str.contains('utc', case=False, na=False).sum()

        if contains_utc_count > 0:
            hours = df.iloc[:, 1].astype(str).str.replace(' UTC', '', regex=False)
            hours = hours.str.zfill(4)
            hours = hours.str[:2] + ':' + hours.str[2:]
            hours = pd.to_datetime(hours, format='%H:%M', errors='coerce').dt.time

        datetime_combined = pd.to_datetime(date.astype(str) + ' ' + hours.astype(str), errors='coerce')

        data_bd = pd.DataFrame({
            'id_estacao': [id_estacao] * len(date),
            'ano': [ano] * len(date),
            'precipitacao': precipitation,
            'temperatura': temperatura,
            'umidade': humidity,
            'vento_graus': wind_grau,
            'vento_vel': wind_vel,
            'dt_registro': datetime_combined,
        })

        print(data_bd)

        return data_bd
    
    def check_exist(self, ano, id_estacao, dt_registro):
        sql_check = f'''
        SELECT
            COUNT(*)
        FROM
            dados_mg.dados_metereologico
        WHERE
            ano = {ano}
            AND id_estacao = '{id_estacao}'
            --AND dt_registro = '{dt_registro}'
        '''

        conn = self.conn()
        cursor = conn.cursor()
        cursor.execute(sql_check)
        count = cursor.fetchone()[0]

        return count
    
    def insert_data(self, df):
        try:
            # Conecte ao banco de dados
            conn = self.conn()
            cursor = conn.cursor()
            
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep=',')
            buffer.seek(0) 
            
            cursor.copy_from(
                buffer,
                'dados_metereologico',
                sep=',',
                columns=(
                    'id_estacao',
                    'ano',
                    'precipitacao',
                    'temperatura',
                    'umidade',
                    'vento_graus',
                    'vento_vel',
                    'dt_registro'
                )
            )
            
            # Commit e fechamento da conexão
            conn.commit()
            cursor.close()
            conn.close()
            
            print("Inserção em lote realizada com sucesso.")
            
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")

    def conn(self):
        conn = psycopg2.connect(
            host="localhost",
            database="artigo",
            user="postgres",
            password="vitor987sa@A",
            options="-c search_path=dados_mg"
        )

        return conn
        
def main():
    edi = ExtractDataINMET()
    edi.control_manipulate_data()

if __name__ == "__main__":
    main()   
