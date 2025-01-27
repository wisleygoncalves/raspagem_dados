import requests as r
import os
import pandas as pd
import psycopg2
from io import StringIO

class ExtractDataConab(object):
    path = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Series Historica Conab"

    list_data = [
        ('cana-de-açúcar', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/54632_4275d925fcc7e678291e0ff606c5fd7b'),
        ('café', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55038_bc9a18281db4f09c1610a49120365f13'),
        ('algodão em caroço', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55673_e94ac3914e421f6bf8d961221fa3f4c8'),
        ('algodão de caroço', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55673_e94ac3914e421f6bf8d961221fa3f4c8'),
        ('algodão pluma', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55673_e94ac3914e421f6bf8d961221fa3f4c8'),
        ('feijão', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55688_8141a37fb20a6aed58b2d6e8c9a5711f'),
        ('milho', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55693_ac5b88574b366dc8f161aca1a1cb13da'),
        ('soja', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55694_419a7429544c72394f6279c6383d5aaa'),
        ('sorgo', 'https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras/item/download/55695_81358411707318b0de76f6caf13e48df')
    ]

    def __init__(self):
        print('Obtendo séries históricas...')
        pass

    def control_extract_file(self):
        self.get_file_xls()
        
        for i in range(0, 9, 1):
            cultura = self.list_data[i][0]
            path_plain = os.path.join(self.path, f'{self.list_data[i][0]}.xls')
            
            area_plantada = self.get_data_area(cultura, path_plain)
            produtividade = self.get_data_produtividade(cultura, path_plain)
            producao = self.get_data_producao(cultura, path_plain)

            anos_comuns = area_plantada.columns.intersection(produtividade.columns).intersection(producao.columns)

            # Filtra todas as séries com os mesmos anos
            area_plantada = area_plantada.loc[:, anos_comuns]
            produtividade = produtividade.loc[:, anos_comuns]
            producao = producao.loc[:, anos_comuns]

            print(area_plantada.shape[1])
            print(produtividade.shape[1])
            print(producao.shape[1])

            data_bd = pd.DataFrame({
                'cultura': [cultura] * area_plantada.shape[1],  
                'ano': area_plantada.columns,
                'area': area_plantada.iloc[0], 
                'produtividade': produtividade.iloc[0],
                'producao': producao.iloc[0]
            })

            this_exist = self.check_exist(cultura)

            if this_exist == 0:
                self.insert_data(data_bd)


    def get_file_xls(self):
        for data in self.list_data:
            res = r.get(data[1])

            if res.status_code == 200:
                print('Ok...')
                path_file_save = os.path.join(self.path, f'{data[0]}.xls')

                try:
                    with open(path_file_save, 'wb') as file:
                        file.write(res.content)
                    
                    print('Arquivo salvo com sucesso...')
                except Exception as e:
                    print(f'Error --> {e}')

            else:
                print('Error...')
    
    def get_data_area(self, cultura, path_plain):
        area_sheet_name = None

        if 'café' not in cultura:
            area_sheet_name = 'Área'
        
        if 'café' in cultura:
            area_sheet_name = 'Área em produção'

        df_area = pd.read_excel(path_plain,
                            skiprows=5,
                            header=0,
                            sheet_name=area_sheet_name)
            
        if 'café' not in cultura:
            df_mg_area = df_area[df_area['REGIÃO/UF'] == 'MG']
            df_mg_area = df_mg_area.loc[:, '2013/14':'2023/24']
            print(cultura)
            print(df_mg_area, '\n\n')
        
        if 'café' in cultura:
            df_mg_area = df_area[df_area['UNIDADE DA FEDERAÇÃO / REGIÃO'] == 'MG']
            df_mg_area = df_mg_area.iloc[:, 13:24]
            print(cultura)
            print(df_mg_area, '\n\n')

        return df_mg_area
    
    def get_data_produtividade(self, cultura, path_plain):
        produtividade_sheet_name = None

        if 'algodão em caroço' not in cultura and 'algodão de caroço' not in cultura \
        and 'algodão pluma' not in cultura:
            produtividade_sheet_name= 'Produtividade'
        
        if 'algodão em caroço' in cultura:
            produtividade_sheet_name = 'Produtividade Algodão em Caroço'
        
        if 'algodão de caroço' in cultura:
            produtividade_sheet_name = 'Produtividade Caroço de Algodão'
        
        if 'algodão pluma' in cultura:
            produtividade_sheet_name = 'Produtividade Pluma'

        df_produtividade = pd.read_excel(path_plain,
                            skiprows=5,
                            header=0,
                            sheet_name=produtividade_sheet_name)
            
        if 'café' not in cultura:
            df_mg_produtividade = df_produtividade[df_produtividade['REGIÃO/UF'] == 'MG']
            df_mg_produtividade = df_mg_produtividade.loc[:, '2013/14':'2023/24']
            print(cultura)
            print(df_mg_produtividade, '\n\n')
        
        if 'café' in cultura:
            df_mg_produtividade = df_produtividade[df_produtividade['UNIDADE DA FEDERAÇÃO / REGIÃO'] == 'MG']
            df_mg_produtividade = df_mg_produtividade.iloc[:, 13:24]
            print(cultura)
            print(df_mg_produtividade, '\n\n')

        return df_mg_produtividade
    
    def get_data_producao(self, cultura, path_plain):
        producao_sheet_name = None

        if 'algodão em caroço' not in cultura and 'algodão de caroço' not in cultura \
        and 'algodão pluma' not in cultura:
            producao_sheet_name= 'Produtividade'
        
        if 'algodão em caroço' in cultura:
            producao_sheet_name = 'Produção Algodão em Caroço'
        
        if 'algodão de caroço' in cultura:
            producao_sheet_name = 'Produção de Caroço de Algodão'
        
        if 'algodão pluma' in cultura:
            producao_sheet_name = 'Produção de Pluma'

        df_producao = pd.read_excel(path_plain,
                            skiprows=5,
                            header=0,
                            sheet_name=producao_sheet_name)
            
        if 'café' not in cultura:
            df_mg_producao = df_producao[df_producao['REGIÃO/UF'] == 'MG']
            df_mg_producao = df_mg_producao.loc[:, '2013/14':'2023/24']
            print(cultura)
            print(df_mg_producao, '\n\n')
        
        if 'café' in cultura:
            df_mg_producao = df_producao[df_producao['UNIDADE DA FEDERAÇÃO / REGIÃO'] == 'MG']
            df_mg_producao= df_mg_producao.iloc[:, 13:24]
            print(cultura)
            print(df_mg_producao, '\n\n')

        return df_mg_producao
    
    def check_exist(self, cultura):
        sql_check = f'''
        SELECT
            COUNT(*)
        FROM
            dados_mg.series_historica_cultura
        WHERE
            cultura = '{cultura}'
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
                'series_historica_cultura',
                sep=',',
                columns=(
                    'cultura',
                    'ano',
                    'area',
                    'produtividade',
                    'producao'
                )
            )
            
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
    edc = ExtractDataConab()
    edc.control_extract_file()

if __name__ == '__main__':
    main()