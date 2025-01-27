import requests as r
import os
import zipfile as z
import shutil

class GetDataINMET(object):
    url_base = 'https://portal.inmet.gov.br/uploads/dadoshistoricos/'
    path_base_zip = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Dados Metereológicos (.zip)"
    path_base_files = r"C:\Artigos Cientificos\Analise_Historica_MG\Files\Dados Metereológicos"

    def __init__(self):
        print('Obtendo arquivos do  INMET...')
        pass
    
    def control_manipulate_file(self):
        self.get_file_zip()
        self.zipfile_descompate()
        self.get_files_mg()

    def get_file_zip(self):
        for i in range(2013, 2025, 1):
            url = f'{self.url_base}/{i}.zip'
            res = r.get(url)

            if res.status_code == 200:
                print('Ok...')
                path_file_save = os.path.join(self.path_base_zip, f'{i}.zip')

                try:
                    with open(path_file_save, 'wb') as file:
                        file.write(res.content)
                    
                    print('Arquivo salvo com sucesso...')
                except Exception as e:
                    print(f'Error --> {e}')

            else:
                print('Error...')
    
    def zipfile_descompate(self):
        for i in range(2013, 2025, 1):
            path_original = os.path.join(self.path_base_zip, f'{i}.zip')
            path_destinate = os.path.join(self.path_base_files, str(i))

            if os.path.exists(path_destinate):
                print(f'Pasta {i} já existe no sitema...')
                shutil.rmtree(os.path.join(path_destinate))
            
            try:
                os.makedirs(path_destinate, exist_ok=True)

                print('Movendo Pasta...')
                shutil.copy(path_original, os.path.join(self.path_base_files, f'{i}.zip'))

                with z.ZipFile(os.path.join(self.path_base_files, f'{i}.zip'), 'r') as file:
                    file.extractall(path_destinate)
                
                print('Pasta descompactada...')
                os.remove(os.path.join(self.path_base_files, f'{i}.zip'))
            except Exception as e:
                print(f'Error --> {e}')
    
    def get_files_mg(self):
        for i in range(2013, 2025, 1):
            path_destinate = os.path.join(self.path_base_files, str(i), str(i))
            _path_destinate = os.path.join(self.path_base_files, str(i))

            path_finaly = os.path.join(self.path_base_files, f'MG_{i}')

            if not os.path.exists(path_finaly):
                os.makedirs(path_finaly)

            if os.path.exists(path_destinate):
                for file in os.listdir(path_destinate):
                    if 'INMET_SE_MG_' in file:
                        print(file)

                        file_path = os.path.join(path_destinate, file)
                        _file_path = os.path.join(path_finaly, file)

                        shutil.move(file_path, _file_path)
            
            if os.path.exists(_path_destinate):
                for file in os.listdir(_path_destinate):
                    if 'INMET_SE_MG_' in file:
                        print(file)

                        file_path = os.path.join(_path_destinate, file)
                        _file_path = os.path.join(path_finaly, file)

                        shutil.move(file_path, _file_path)
            
            shutil.rmtree(_path_destinate)

def main():
    gdi = GetDataINMET()
    gdi.control_manipulate_file()

if __name__ == "__main__":
    main()
