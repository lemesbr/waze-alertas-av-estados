"""
Coletor de Alertas Waze - Av. dos Estados (SP)
Usa a API pública do Waze via endpoint de embed/partner — sem API key.
"""

import requests
import pandas as pd
import json
import os
import sys
import time
import random
from datetime import datetime

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_SAIDA   = os.path.join(SCRIPT_DIR, "dados", "alertas_waze.csv")
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

# ── Bounding Box: Av. dos Estados (Ligação Leste-Oeste → Anhaia Mello) ──
BOTTOM_LAT = -23.5700
TOP_LAT    = -23.5450
LEFT_LON   = -46.6300
RIGHT_LON  = -46.5950

# Eixo central da via (para classificar sentido)
EIXO_CENTRAL_LON = -46.6130

# Tipos de alerta relevantes para atividade criminal
TIPOS_RELEVANTES = ["POLICE", "HAZARD", "ACCIDENT"]

# User-agents rotativos para evitar bloqueio
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://embed.waze.com/",
        "Origin": "https://embed.waze.com",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }


def fetch_waze_via_scraperapi():
    """
    Busca alertas via ScraperAPI em API mode (não proxy mode).
    A ScraperAPI faz a requisição por um IP residencial e retorna o JSON.
    render=false economiza créditos (não executa JS, só retorna a resposta HTTP).
    """
    import urllib.parse

    waze_params = {
        "bottom": BOTTOM_LAT, "top": TOP_LAT,
        "left":   LEFT_LON,   "right": RIGHT_LON,
        "ma": 200, "mj": 100, "mu": 100, "types": "alerts",
    }

    # Tenta 2 endpoints para não desperdiçar créditos do free tier
    endpoints = [
        "https://www.waze.com/row-Ede3-api/georss",
        "https://www.waze.com/live-map/api/georss",
    ]

    for endpoint in endpoints:
        target_url = endpoint + "?" + urllib.parse.urlencode(waze_params)
        try:
            resp = requests.get(
                "https://api.scraperapi.com",
                params={
                    "api_key": SCRAPER_API_KEY,
                    "url": target_url,
                    "render": "false",
                },
                headers={"Accept": "application/json"},
                timeout=70,
            )
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    alertas = data.get("alerts", [])
                    print(f"  ScraperAPI OK ({endpoint.split('/')[-1]}): {len(alertas)} alertas brutos")
                    return alertas
                except Exception:
                    print(f"  ScraperAPI sem JSON ({endpoint.split('/')[-1]}): {resp.text[:100]}")
                    continue
            else:
                print(f"  ScraperAPI HTTP {resp.status_code} ({endpoint.split('/')[-1]}): {resp.text[:80]}")
        except Exception as e:
            print(f"  ScraperAPI erro ({endpoint.split('/')[-1]}): {type(e).__name__}: {e}")

    return []


def fetch_waze_direct():
    """
    Busca alertas diretamente — funciona em IP residencial, bloqueado em cloud.
    """
    params = {
        "bottom": BOTTOM_LAT, "top": TOP_LAT,
        "left":   LEFT_LON,   "right": RIGHT_LON,
        "ma": 200, "mj": 100, "mu": 100, "types": "alerts",
    }
    endpoints = [
        "https://embed.waze.com/row-Ede3-api/georss",
        "https://embed.waze.com/georss",
        "https://www.waze.com/row-Ede3-api/georss",
        "https://www.waze.com/row-rtserver/web/TGeoRSS",
        "https://www.waze.com/rtserver/web/TGeoRSS",
        "https://www.waze.com/live-map/api/georss",
    ]
    session = requests.Session()
    try:
        session.get("https://embed.waze.com/", headers=get_headers(), timeout=15)
        time.sleep(random.uniform(0.5, 1.5))
    except Exception:
        pass

    for endpoint in endpoints:
        try:
            resp = session.get(endpoint, params=params, headers=get_headers(), timeout=30)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"  Direto OK: {endpoint.split('/')[-3]}")
                    return data.get("alerts", [])
                except Exception:
                    print(f"  Sem JSON ({endpoint.split('/')[-3]}): {resp.text[:80]}")
                    continue
            elif resp.status_code == 403:
                print(f"  Bloqueado (403): {endpoint.split('/')[-3]}")
            elif resp.status_code == 429:
                print(f"  Rate limit (429): aguardando...")
                time.sleep(random.uniform(5, 10))
            else:
                print(f"  HTTP {resp.status_code}: {endpoint.split('/')[-3]}")
        except requests.exceptions.Timeout:
            print(f"  Timeout: {endpoint.split('/')[-3]}")
        except Exception as e:
            print(f"  Erro ({endpoint.split('/')[-3]}): {type(e).__name__}")
        time.sleep(random.uniform(0.3, 0.8))

    print("  AVISO: Nenhum endpoint respondeu com dados.")
    return []


def fetch_alertas():
    """Escolhe automaticamente o método de coleta disponível."""
    if SCRAPER_API_KEY:
        print("  Modo: ScraperAPI (proxy residencial)")
        return fetch_waze_via_scraperapi()
    print("  Modo: direto (requer IP residencial)")
    return fetch_waze_direct()


def classificar_sentido(lon):
    if lon < EIXO_CENTRAL_LON:
        return "Sentido ABC (Sul)"
    return "Sentido Centro (Norte)"


def processar_alertas(alertas_brutos):
    """Filtra e normaliza alertas relevantes."""
    resultados = []
    agora = datetime.utcnow()

    for a in alertas_brutos:
        tipo = a.get("type", "")
        if tipo not in TIPOS_RELEVANTES:
            continue

        lat = float(a.get("location", {}).get("y", a.get("latitude", a.get("lat", 0))))
        lon = float(a.get("location", {}).get("x", a.get("longitude", a.get("lon", 0))))

        if lat == 0 and "nThumbsUp" in a:
            lat = float(a.get("latitude", 0))
            lon = float(a.get("longitude", 0))

        if lat == 0 or lon == 0:
            continue

        resultados.append({
            "alert_id":       str(a.get("uuid", a.get("id", a.get("alert_id", "")))),
            "coleta_utc":     agora.strftime("%Y-%m-%d %H:%M:%S"),
            "data":           agora.strftime("%Y-%m-%d"),
            "hora_utc":       agora.strftime("%H:%M"),
            "hora_brt":       (agora.replace(hour=(agora.hour - 3) % 24)).strftime("%H:%M"),
            "dia_semana":     agora.strftime("%A"),
            "latitude":       lat,
            "longitude":      lon,
            "tipo":           tipo,
            "subtipo":        a.get("subtype", ""),
            "sentido_via":    classificar_sentido(lon),
            "confiabilidade": a.get("reliability", a.get("alert_reliability", 0)),
            "thumbs_up":      a.get("nThumbsUp", a.get("num_thumbs_up", 0)),
            "rua":            a.get("street", a.get("roadType", "")),
            "descricao":      a.get("reportDescription", a.get("description", "")),
        })

    return resultados


def salvar_incremental(novos_alertas):
    """Adiciona alertas ao CSV, evitando duplicatas."""
    os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)
    df_novo = pd.DataFrame(novos_alertas)

    if len(df_novo) == 0:
        return 0

    if os.path.exists(ARQUIVO_SAIDA):
        df_existente = pd.read_csv(ARQUIVO_SAIDA)
        ids_existentes = set(df_existente["alert_id"].astype(str))
        df_novo = df_novo[~df_novo["alert_id"].astype(str).isin(ids_existentes)]
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_final = df_novo

    df_final.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")
    return len(df_novo)


def main():
    print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}] Buscando alertas Waze...")

    delay = random.uniform(1, 5)
    time.sleep(delay)

    try:
        brutos = fetch_alertas()
        processados = processar_alertas(brutos)
        novos = salvar_incremental(processados)
        print(f"  Brutos: {len(brutos)} | Relevantes: {len(processados)} | Novos: {novos}")
    except Exception as e:
        print(f"  ERRO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
