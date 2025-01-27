import geopandas as gpd
import psycopg2
import pandas as pd
from shapely.geometry import Point

class GetDataEstacao(object):
    path_csv = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Catalago Estacao\CatalogoEstaçõesAutomáticas.csv"

    def __init__(self):
        print('Obtendo Dados das Estações...')
        pass
    
    def control_data_estacao(self):
        data_estacao = self.get_file_csv()
        self.manipulate_gdp(data_estacao)
    
    def get_file_csv(self):
        df = pd.read_csv(self.path_csv, sep=';', on_bad_lines='skip')
        df_mg = df[df['SG_ESTADO'] == 'MG']

        id_estacao = df_mg['CD_ESTACAO']
        nome_limite = df_mg['DC_NOME']
        lat = df_mg['VL_LATITUDE'].astype(str).str.replace(',', '.').astype(float)
        lon = df_mg['VL_LONGITUDE'].astype(str).str.replace(',', '.').astype(float)
        altitude = df_mg['VL_ALTITUDE'].astype(str).str.replace(',', '.').astype(float)
        
        data_estacao = pd.DataFrame({
            'id_estacao': id_estacao,
            'nome_limite': nome_limite ,
            'lat': lat,
            'lon': lon,
            'altitude': altitude
        })

        data_estacao['geometry'] = data_estacao.apply(lambda row: Point(row['lon'], row['lat']), axis=1)
        gdf_estacao = gpd.GeoDataFrame(data_estacao, geometry='geometry')

        print(gdf_estacao)

        return data_estacao

    def manipulate_gdp(self, data_estacao):
        for idx, row in data_estacao.iterrows():
            geom_wkt = row['geometry'].wkt 
            id_estacao = row['id_estacao']
            nome_limite = row['nome_limite']
            lat = row['lat']
            lon = row['lon']
            altitude = row['altitude']

            if "Olhos-d'Água" in nome_limite or "Pingo-d'Água" in nome_limite:
                nome_limite = nome_limite.replace("'", "`")
            
            print(id_estacao)

            this_exist = self.check_exist_estacao(id_estacao)

            if this_exist == 0:
                self.insert_estacao(id_estacao, nome_limite, lat, lon, altitude, geom_wkt)

    def check_exist_estacao(self, id_estacao):
        sql_exist = f'''
        SELECT
            COUNT(*)
        FROM
            dados_mg.estacoes
        WHERE
            id_estacao = '{id_estacao}'
        '''

        conn = self.conn()
        cursor = conn.cursor()
        cursor.execute(sql_exist)
        count = cursor.fetchone()[0]

        return count
    
    def insert_estacao(self, id_estacao, nome_limite, lat, lon, altitude, geom_wk):
        sql_insert = f'''
        INSERT INTO
            dados_mg.estacoes (
                id_estacao,
                nome_limite,
                lat,
                lon,
                altitude,
                point
            )
        VALUES (
            '{id_estacao}',
            '{nome_limite}',
            {lat},
            {lon},
            {altitude},
            '{geom_wk}'
        )
        '''

        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(sql_insert)
            conn.commit()
            cursor.close()

            print(f'{id_estacao} --> Inserido com sucesso... ')
        except Exception as e:
            print(f'{id_estacao} --> {e}')
    
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
    gde = GetDataEstacao()
    gde.control_data_estacao()

if __name__ == '__main__':
    main()

