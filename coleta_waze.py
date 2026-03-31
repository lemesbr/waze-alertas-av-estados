"""
Coletor de Alertas Waze - Av. dos Estados (SP)
Acesso direto ao Live Map — sem API key, sem custo.
"""

import requests
import pandas as pd
import json
import os
import sys
import time
import random
from datetime import datetime

ARQUIVO_SAIDA = "dados/alertas_waze.csv"

# ── Bounding Box: Av. dos Estados (Ligação Leste-Oeste → Anhaia Mello) ──
BOTTOM_LAT = -23.5700
TOP_LAT    = -23.5450
LEFT_LON   = -46.6300
RIGHT_LON  = -46.5950

# Eixo central da via (para classificar sentido)
EIXO_CENTRAL_LON = -46.6130

# Tipos de alerta relevantes para atividade criminal
TIPOS_RELEVANTES = ["POLICE", "HAZARD", "ACCIDENT"]

# Endpoints regionais do Waze (tentar na ordem)
WAZE_ENDPOINTS = [
    "https://www.waze.com/row-Ede3-api/georss",
    "https://www.waze.com/row-rtserver/web/TGeoRSS",
    "https://www.waze.com/rtserver/web/TGeoRSS",
    "https://www.waze.com/live-map/api/georss",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.waze.com/live-map",
    "Accept": "application/json",
}


def fetch_waze_direct():
    """
    Busca alertas diretamente no endpoint interno do Waze Live Map.
    Tenta múltiplos endpoints regionais até encontrar um que funcione.
    """
    params = {
        "bottom": BOTTOM_LAT,
        "top": TOP_LAT,
        "left": LEFT_LON,
        "right": RIGHT_LON,
        "ma": 200,
        "mj": 100,
        "mu": 100,
        "types": "alerts",
    }

    for endpoint in WAZE_ENDPOINTS:
        try:
            resp = requests.get(
                endpoint,
                params=params,
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                alertas = data.get("alerts", [])
                print(f"  Endpoint OK: {endpoint.split('waze.com/')[1]}")
                return alertas
        except Exception:
            continue

    # Fallback: endpoint com formato antigo (usado por vários scrapers)
    try:
        url = (
            f"https://www.waze.com/row-Rutserver/web/TGeoRSS"
            f"?bottom={BOTTOM_LAT}&top={TOP_LAT}"
            f"&left={LEFT_LON}&right={RIGHT_LON}"
            f"&ma=200&mj=100&mu=100&types=alerts"
        )
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            print(f"  Endpoint OK: row-Rutserver (fallback)")
            return data.get("alerts", [])
    except Exception:
        pass

    print("  ERRO: Nenhum endpoint respondeu.")
    return []


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

        # Fallback para estruturas diferentes
        if lat == 0 and "nThumbsUp" in a:
            lat = float(a.get("latitude", 0))
            lon = float(a.get("longitude", 0))

        if lat == 0 or lon == 0:
            continue

        resultados.append({
            "alert_id": str(a.get("uuid", a.get("id", a.get("alert_id", "")))),
            "coleta_utc": agora.strftime("%Y-%m-%d %H:%M:%S"),
            "data": agora.strftime("%Y-%m-%d"),
            "hora_utc": agora.strftime("%H:%M"),
            "hora_brt": (agora.replace(hour=(agora.hour - 3) % 24)).strftime("%H:%M"),
            "dia_semana": agora.strftime("%A"),
            "latitude": lat,
            "longitude": lon,
            "tipo": tipo,
            "subtipo": a.get("subtype", ""),
            "sentido_via": classificar_sentido(lon),
            "confiabilidade": a.get("reliability", a.get("alert_reliability", 0)),
            "thumbs_up": a.get("nThumbsUp", a.get("num_thumbs_up", 0)),
            "rua": a.get("street", a.get("roadType", "")),
            "descricao": a.get("reportDescription", a.get("description", "")),
        })

    return resultados


def salvar_incremental(novos_alertas):
    """Adiciona alertas ao CSV, evitando duplicatas."""
    os.makedirs("dados", exist_ok=True)
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
    print(f"[{datetime.utcnow():%Y-%m-%d %H:%M:%S UTC}] Buscando alertas Waze (direto)...")

    # Delay aleatório para evitar padrão previsível
    delay = random.uniform(1, 5)
    time.sleep(delay)

    try:
        brutos = fetch_waze_direct()
        processados = processar_alertas(brutos)
        novos = salvar_incremental(processados)
        print(f"  Brutos: {len(brutos)} | Relevantes: {len(processados)} | Novos: {novos}")
    except Exception as e:
        print(f"  ERRO: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
