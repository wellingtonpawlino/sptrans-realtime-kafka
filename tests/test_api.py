from api.buscar_posicao import buscar_posicao_veiculos


def _mock_resposta_sptrans():
    """Simula a estrutura de resposta da API Olho Vivo."""
    return {
        "l": [
            {
                "c": "1234-10",
                "cl": 34041,
                "sl": 1,
                "vs": [
                    {"p": "A001", "a": True,  "ta": "2026-01-01T10:00:00Z", "py": -23.5, "px": -46.6},
                    {"p": "A002", "a": False, "ta": "2026-01-01T10:00:00Z", "py": -23.6, "px": -46.7},
                ],
            }
        ]
    }


def test_parsing_retorna_lista(monkeypatch):
    import api.buscar_posicao as modulo

    class SessionMock:
        def get(self, url):
            class Resp:
                status_code = 200
                def json(self):
                    return _mock_resposta_sptrans()
            return Resp()

    resultado = buscar_posicao_veiculos(SessionMock())
    assert isinstance(resultado, list)
    assert len(resultado) == 2


def test_campos_obrigatorios_presentes(monkeypatch):
    import api.buscar_posicao as modulo

    class SessionMock:
        def get(self, url):
            class Resp:
                status_code = 200
                def json(self):
                    return _mock_resposta_sptrans()
            return Resp()

    resultado = buscar_posicao_veiculos(SessionMock())
    veiculo = resultado[0]
    assert "codigo_linha" in veiculo
    assert "prefixo_veiculo" in veiculo
    assert "latitude" in veiculo
    assert "longitude" in veiculo
