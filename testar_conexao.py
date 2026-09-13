from dotenv import load_dotenv
load_dotenv()

from api.autenticacao import autenticar
from api.buscar_posicao import buscar_posicao_veiculos

session = autenticar()

if session:
    veiculos = buscar_posicao_veiculos(session)
    print(f"\n🚌 Total de veículos encontrados: {len(veiculos)}")
    print("Exemplo de registro:")
    print(veiculos[0] if veiculos else "Nenhum veículo retornado.")