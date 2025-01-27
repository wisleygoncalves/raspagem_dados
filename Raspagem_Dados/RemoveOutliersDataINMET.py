import psycopg2

class RemoveOutliersDataINMET(object):
    def __init__(self):
        print('Analisando Outliers nos Dados INMET...')
        pass

    def control_analysis_outliers(self):
        for i in range(2013, 2025, 1):
            outliers = self.get_outiliers(i)

            precipitacao = outliers[1]
            temperatura = outliers[2]
            umidade = outliers[3]
            vento_graus = outliers[4]
            vento_vel = outliers[5]

            if precipitacao < 0:
                self.update_outiliers('precipitacao', precipitacao, i)
            
            if temperatura < -10:
                self.update_outiliers('temperatura', temperatura, i)
            
            if umidade < 0:
                self.update_outiliers('umidade', umidade, i)
            
            if vento_graus < 0:
                self.update_outiliers('vento_graus', vento_graus, i)
            
            if vento_vel < 0:
                self.update_outiliers('vento_vel', vento_vel, i)
    
    def get_outiliers(self, ano):
        sql_data = f'''
        SELECT
            ano,
            min(precipitacao) as min_precipitacao,
            min(temperatura) as min_temperatura,
            min(umidade) as min_umidade,
            min(vento_graus) as min_vento_graus,
            min(vento_vel) as min_vento_vel
        FROM
            dados_mg.dados_metereologico 
        WHERE
            ano = {ano}
        GROUP BY
            ano
        '''

        conn = self.conn()
        cursor = conn.cursor()
        cursor.execute(sql_data)
        data = cursor.fetchone()

        return data
    
    def update_outiliers(self, column, data, i):
        sql_update = f'''
        UPDATE
            dados_mg.dados_metereologico 
        SET
            {column} = 0
        WHERE
            {column} = {data}
            AND ano = {i}
        '''

        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(sql_update)
            conn.commit()
            cursor.close()
            print(f'{column} --> Atualizado com sucesso')
        except Exception as e:
            print(f'Erro --> {e}')

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
    rpdi = RemoveOutliersDataINMET()
    rpdi.control_analysis_outliers()

if __name__ == '__main__':
    main()