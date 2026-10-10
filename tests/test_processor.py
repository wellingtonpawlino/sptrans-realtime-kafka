from collections import defaultdict


def agregar_veiculos(eventos):
    """Lógica central do processor: agrega veículos únicos por linha."""
    veiculos_por_linha = defaultdict(set)
    for evento in eventos:
        linha = evento.get("codigo_linha")
        prefixo = evento.get("prefixo_veiculo")
        if linha is not None and prefixo is not None:
            veiculos_por_linha[linha].add(prefixo)
    return {linha: len(prefixos) for linha, prefixos in veiculos_por_linha.items()}


def test_agrega_veiculos_por_linha():
    eventos = [
        {"codigo_linha": 1, "prefixo_veiculo": "A001"},
        {"codigo_linha": 1, "prefixo_veiculo": "A002"},
        {"codigo_linha": 2, "prefixo_veiculo": "B001"},
    ]
    resultado = agregar_veiculos(eventos)
    assert resultado[1] == 2
    assert resultado[2] == 1


def test_deduplica_veiculo_repetido():
    eventos = [
        {"codigo_linha": 1, "prefixo_veiculo": "A001"},
        {"codigo_linha": 1, "prefixo_veiculo": "A001"},  # duplicado
    ]
    resultado = agregar_veiculos(eventos)
    assert resultado[1] == 1


def test_ignora_evento_sem_campos():
    eventos = [
        {"codigo_linha": 1},           # sem prefixo
        {"prefixo_veiculo": "A001"},   # sem linha
        {},                             # vazio
    ]
    resultado = agregar_veiculos(eventos)
    assert resultado == {}
