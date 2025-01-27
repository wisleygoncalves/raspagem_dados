import requests as r
import os
import shutil
import zipfile as z
import geopandas as gpd
import psycopg2
import pandas as pd

class MalhaIGBE(object):
    urls = ['https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/UFs/MG/MG_UF_2022.zip',
            'https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/UFs/MG/MG_Municipios_2022.zip',
            'https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/UFs/MG/MG_Mesorregioes_2022.zip']
    
    name_files = ['uf', 'municipios', 'mesoregiao']

    path_zip = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Malha Municipal (.zip)"
    path_extract = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Malha Municipal"

    def __init__(self):
        print('Obtendo Malha IBGE...')
        pass

    def control_get_malha(self):
        self.get_file_zip()
        self.zipfile_descompate()
        self.get_shp_limite()
        self.get_shp_messoregiao()
        self.get_shp_municipio()
    
    def get_file_zip(self):
        for i in range(0, len(self.urls), 1):
            res = r.get(self.urls[i])

            if res.status_code == 200:
                print('Ok...')
                path_file_save = os.path.join(self.path_zip, f'{self.name_files[i]}.zip')

                try:
                    with open(path_file_save, 'wb') as file:
                        file.write(res.content)
                    
                    print('Arquivo salvo com sucesso...')
                except Exception as e:
                    print(f'Error --> {e}')

            else:
                print('Error...')
    
    def zipfile_descompate(self):
        for i in range(0, len(self.name_files), 1):
            path_original = os.path.join(self.path_zip, f'{self.name_files[i]}.zip')
            path_destinate = os.path.join(self.path_extract, str(self.name_files[i]))

            if os.path.exists(path_destinate):
                print(f'Pasta {i} já existe no sitema...')
                shutil.rmtree(os.path.join(path_destinate))
            
            try:
                os.makedirs(path_destinate, exist_ok=True)

                print('Movendo Pasta...')
                shutil.copy(path_original, os.path.join(self.path_extract, f'{self.name_files[i]}.zip'))

                with z.ZipFile(os.path.join(self.path_extract, f'{self.name_files[i]}.zip'), 'r') as file:
                    file.extractall(path_destinate)
                
                print('Pasta descompactada...')
                os.remove(os.path.join(self.path_extract, f'{self.name_files[i]}.zip'))
            except Exception as e:
                print(f'Error --> {e}')
    
    def get_shp_limite(self):
        shp = gpd.read_file(os.path.join(self.path_extract, self.name_files[0], 'MG_UF_2022.shp'))

        for idx, row in shp.iterrows():
            geom_wkt = row['geometry'].wkt 
            print(geom_wkt)
            cod = row['CD_UF']
            nome_limite = 'UF - MG'
            area = row['AREA_KM2']

            this_exist = self.check_exist_limit(nome_limite)

            if this_exist == 0:
                self.insert_limit(cod, nome_limite, area, geom_wkt)

    def get_shp_municipio(self):
        shp = gpd.read_file(os.path.join(self.path_extract, self.name_files[1], 'MG_Municipios_2022.shp'))

        for idx, row in shp.iterrows():
            geom_wkt = row['geometry'].wkt 
            cod = row['CD_MUN']
            nome_limite = f"CIDADE - {row['NM_MUN']}"
            area = row['AREA_KM2']

            if "Olhos-d'Água" in nome_limite or "Pingo-d'Água" in nome_limite:
                nome_limite = nome_limite.replace("'", "`")
            
            print(nome_limite)

            this_exist = self.check_exist_limit(nome_limite)

            if this_exist == 0:
                self.insert_limit(cod, nome_limite, area, geom_wkt)
        
    def get_shp_messoregiao(self):
        shp = gpd.read_file(os.path.join(self.path_extract, self.name_files[2], 'MG_Mesorregioes_2022.shp'))

        for idx, row in shp.iterrows():
            geom_wkt = row['geometry'].wkt 
            cod = row['CD_MESO']
            nome_limite = f"MESSORREGIAO - {row['NM_MESO']}"
            area = row['AREA_KM2']

            nome_limite = nome_limite.replace("'", "`")
            
            print(nome_limite)

            this_exist = self.check_exist_limit(nome_limite)

            if this_exist == 0:
                self.insert_limit(cod, nome_limite, area, geom_wkt)

    def check_exist_limit(self, nome):
        sql_exist = f'''
        SELECT
            COUNT(*)
        FROM
            dados_mg.malha_municipal
        WHERE
            nome_limite = '{nome}'
        '''

        conn = self.conn()
        cursor = conn.cursor()
        cursor.execute(sql_exist)
        count = cursor.fetchone()[0]

        return count
    
    def insert_limit(self, cod, nome_limite, area, geom):
        sql_insert = f'''
        INSERT INTO
            dados_mg.malha_municipal (
                cod,
                nome_limite,
                area,
                shape
            )
        VALUES (
            {cod},
            '{nome_limite}',
            {area},
            '{geom}'
        )
        '''

        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(sql_insert)
            conn.commit()
            cursor.close()

            print(f'{nome_limite} --> Inserido com sucesso... ')
        except Exception as e:
            print(f'{nome_limite} --> {e}')
    
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
    mi = MalhaIGBE()
    mi.control_get_malha()

if __name__ == '__main__':
    main()