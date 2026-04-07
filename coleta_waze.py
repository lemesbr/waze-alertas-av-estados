"""
Coletor de Alertas Waze - Av. dos Estados (SP)
Prioridade de coleta:
  1. RapidAPI (letscrape/waze) -- cloud, sem WAF, bounding box nativo
  2. ScraperAPI proxy           -- fallback cloud (legado)
  3. Direto                     -- IP residencial (self-hosted runner)
"""

import requests
import pandas as pd
import os
import sys
import time
import random
from datetime import datetime, timezone, timedelta

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_SAIDA   = os.path.join(SCRIPT_DIR, "dados", "alertas_waze.csv")
LOG_COLETAS     = os.path.join(SCRIPT_DIR, "dados", "log_coletas.csv")

RAPIDAPI_KEY    = os.environ.get("RAPIDAPI_KEY", "")
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

# Bounding Box: Av. dos Estados + Juntas Provisorias / Dom Lucas Obes
BOTTOM_LAT = -23.6050
TOP_LAT    = -23.5450
LEFT_LON   = -46.6300
RIGHT_LON  = -46.5950

EIXO_CENTRAL_LON = -46.6130
TIPOS_RELEVANTES = ["POLICE", "HAZARD", "ACCIDENT"]
BRT = timezone(timedelta(hours=-3))

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
        "Referer": "https://embed.waze.com/",
        "Origin": "https://embed.waze.com",
        "Cache-Control": "no-cache",
    }


# --- METODO 1: RapidAPI ---

def fetch_waze_via_rapidapi():
    url = "https://waze.p.rapidapi.com/alerts-and-jams"
    headers = {
        "x-rapidapi-host": "waze.p.rapidapi.com",
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "Content-Type":    "application/json",
    }
    params = {
        "bottom_left": f"{BOTTOM_LAT},{LEFT_LON}",
        "top_right":   f"{TOP_LAT},{RIGHT_LON}",
        "max_alerts":  200,
        "max_jams":    50,
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            # A RapidAPI encapsula os dados em data["data"]
            payload         = data.get("data", data)
            alertas         = payload.get("alerts", [])
            engarrafamentos = payload.get("jams", [])
            usuarios = payload.get("usersCount", 0) or len(payload.get("users", []))
            if len(alertas) == 0:
                if usuarios > 0 or len(engarrafamentos) > 0:
                    print("  OK: Zero alertas confirmado (area ativa, sem incidentes)")
                else:
                    print("  AVISO: Zero alertas E zero usuarios/jams")
            return {"alertas": alertas, "usuarios": usuarios, "engarrafamentos": len(engarrafamentos), "ok": True, "metodo": "rapidapi"}
        elif resp.status_code == 429:
            print("  RapidAPI rate limit (429) - plano free esgotado")
        elif resp.status_code == 403:
            print("  RapidAPI 403 - chave invalida")
        else:
            print(f"  RapidAPI HTTP {resp.status_code}: {resp.text[:150]}")
    except requests.exceptions.Timeout:
        print("  RapidAPI timeout")
    except Exception as e:
        print(f"  RapidAPI erro: {type(e).__name__}: {e}")
    return {"alertas": [], "usuarios": 0, "engarrafamentos": 0, "ok": False, "metodo": "rapidapi"}


# --- METODO 2: ScraperAPI (fallback) ---

def fetch_waze_via_scraperapi():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    proxies = {
        "http":  f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001",
        "https": f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001",
    }
    params = {"bottom": BOTTOM_LAT, "top": TOP_LAT, "left": LEFT_LON, "right": RIGHT_LON,
              "ma": 200, "mj": 50, "mu": 100, "types": "alerts"}
    try:
        resp = requests.get("https://embed.waze.com/row-Ede3-api/georss",
                            params=params, headers=get_headers(), proxies=proxies, verify=False, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            alertas  = data.get("alerts", [])
            usuarios = data.get("usersCount", 0)
            jams     = len(data.get("jams", []))
            print(f"  ScraperAPI OK -> alertas={len(alertas)} | jams={jams} | usuarios={usuarios}")
            return {"alertas": alertas, "usuarios": usuarios, "engarrafamentos": jams, "ok": True, "metodo": "scraperapi"}
        else:
            print(f"  ScraperAPI HTTP {resp.status_code}: {resp.text[:120]}")
    except Exception as e:
        print(f"  ScraperAPI erro: {type(e).__name__}: {e}")
    return {"alertas": [], "usuarios": 0, "engarrafamentos": 0, "ok": False, "metodo": "scraperapi"}


# --- METODO 3: Direto (IP residencial) ---

def fetch_waze_direct():
    params = {"bottom": BOTTOM_LAT, "top": TOP_LAT, "left": LEFT_LON, "right": RIGHT_LON,
              "ma": 200, "mj": 50, "mu": 100, "types": "alerts"}
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
                data = resp.json()
                alertas  = data.get("alerts", [])
                usuarios = data.get("usersCount", 0)
                jams     = len(data.get("jams", []))
                print(f"  Direto OK ({endpoint.split('/')[-2]}) -> alertas={len(alertas)} | jams={jams} | usuarios={usuarios}")
                return {"alertas": alertas, "usuarios": usuarios, "engarrafamentos": jams, "ok": True, "metodo": "direto"}
            else:
                print(f"  HTTP {resp.status_code}: {endpoint.split('/')[-2]}")
        except requests.exceptions.Timeout:
            print(f"  Timeout: {endpoint.split('/')[-2]}")
        except Exception as e:
            print(f"  Erro ({endpoint.split('/')[-2]}): {type(e).__name__}")
        time.sleep(random.uniform(0.3, 0.8))
    print("  AVISO: Nenhum endpoint respondeu.")
    return {"alertas": [], "usuarios": 0, "engarrafamentos": 0, "ok": False, "metodo": "direto"}


def fetch_alertas():
    if RAPIDAPI_KEY:
        print("  Modo: RapidAPI")
        return fetch_waze_via_rapidapi()
    if SCRAPER_API_KEY:
        print("  Modo: ScraperAPI (fallback legado)")
        return fetch_waze_via_scraperapi()
    print("  Modo: direto (requer IP residencial)")
    return fetch_waze_direct()


# --- Processamento ---

def classificar_sentido(lon):
    return "Sentido ABC (Sul)" if lon < EIXO_CENTRAL_LON else "Sentido Centro (Norte)"


def processar_alertas(alertas_brutos):
    resultados = []
    agora_utc = datetime.now(timezone.utc)
    agora_brt = agora_utc.astimezone(BRT)
    for a in alertas_brutos:
        tipo = a.get("type", "")
        if tipo not in TIPOS_RELEVANTES:
            continue
        loc = a.get("location", {})
        lat = float(loc.get("y", 0) or a.get("lat", 0) or a.get("latitude", 0))
        lon = float(loc.get("x", 0) or a.get("lon", 0) or a.get("longitude", 0))
        if lat == 0 or lon == 0:
            continue
        resultados.append({
            "alert_id":       str(a.get("uuid", a.get("id", a.get("alert_id", "")))),
            "coleta_utc":     agora_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "data":           agora_brt.strftime("%Y-%m-%d"),
            "hora_utc":       agora_utc.strftime("%H:%M"),
            "hora_brt":       agora_brt.strftime("%H:%M"),
            "dia_semana":     agora_brt.strftime("%A"),
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


# --- Persistencia ---

def salvar_incremental(novos_alertas):
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


def salvar_log_coleta(metodo, ok, brutos, relevantes, novos, usuarios, engarrafamentos):
    os.makedirs(os.path.dirname(LOG_COLETAS), exist_ok=True)
    agora_utc = datetime.now(timezone.utc)
    agora_brt = agora_utc.astimezone(BRT)
    linha = {
        "coleta_utc":         agora_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "coleta_brt":         agora_brt.strftime("%Y-%m-%d %H:%M:%S"),
        "metodo":             metodo,
        "api_ok":             ok,
        "alertas_brutos":     brutos,
        "alertas_relevantes": relevantes,
        "alertas_novos":      novos,
        "usuarios_ativos":    usuarios,
        "engarrafamentos":    engarrafamentos,
        "diagnostico": (
            "sem_incidentes" if (ok and brutos == 0 and usuarios > 0)
            else "com_incidentes" if (ok and brutos > 0)
            else "area_inativa"  if (ok and brutos == 0 and usuarios == 0)
            else "falha_api"
        ),
    }
    df_linha = pd.DataFrame([linha])
    if os.path.exists(LOG_COLETAS):
        df_log = pd.read_csv(LOG_COLETAS)
        df_log = pd.concat([df_log, df_linha], ignore_index=True)
    else:
        df_log = df_linha
    df_log.to_csv(LOG_COLETAS, index=False, encoding="utf-8-sig")


# --- Main ---

def main():
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}] Buscando alertas Waze...")
    time.sleep(random.uniform(1, 5))

    resultado       = fetch_alertas()
    alertas_brutos  = resultado["alertas"]
    usuarios        = resultado["usuarios"]
    engarrafamentos = resultado["engarrafamentos"]
    api_ok          = resultado["ok"]
    metodo          = resultado["metodo"]

    processados = processar_alertas(alertas_brutos)
    novos       = salvar_incremental(processados)

    salvar_log_coleta(
        metodo=metodo, ok=api_ok, brutos=len(alertas_brutos),
        relevantes=len(processados), novos=novos,
        usuarios=usuarios, engarrafamentos=engarrafamentos,
    )

    print(f"  Brutos: {len(alertas_brutos)} | Relevantes: {len(processados)} | Novos: {novos}")
    print(f"  Usuarios ativos: {usuarios} | Engarrafamentos: {engarrafamentos}")

    if not api_ok:
        print("  ERRO: API nao respondeu")
        sys.exit(1)


if __name__ == "__main__":
    main()
