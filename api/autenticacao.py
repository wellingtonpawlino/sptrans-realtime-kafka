

import os
import requests  # também precisa para usar requests.Session()

def autenticar():
    token = os.getenv("SPTRANS_TOKEN")
    if not token:
        print("❌ Token não encontrado.")
        return None

    session = requests.Session()
    url_login = f"https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={token}"
    response = session.post(url_login)

    if response.status_code == 200 and response.text.lower() == "true":
        print("✅ Autenticação bem-sucedida!")
        return session
    else:
        print(f"❌ Falha na autenticação: {response.status_code} - {response.text}")
        return None