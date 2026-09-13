def buscar_posicao_veiculos(session):
    url = "https://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    print(f"➡️ Consultando: {url}")
    response = session.get(url)
    if response.status_code == 200:
        dados = response.json()
        hr = dados.get("hr")  # horário de referência
        veiculos = []
        for linha in dados.get("l", []):
            for v in linha.get("vs", []):
                veiculos.append({
                    "codigo_linha": linha.get("c"),
                    "prefixo_veiculo": v.get("p"),
                    "sentido": linha.get("sl"),  # Sentido da linha
                    "latitude": v.get("py"),
                    "longitude": v.get("px"),
                    "hr_referencia": hr,
                    "hr_atualizacao": v.get("ta"),
                    "acessivel": v.get("a")
                })

        return veiculos
    else:
        print(f"❌ Erro HTTP {response.status_code}")
        return []