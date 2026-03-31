"""
Análise dos alertas coletados - Av. dos Estados
Gera mapa HTML e relatório de padrões.
"""

import pandas as pd
import os
import sys

ARQUIVO_DADOS = "dados/alertas_waze.csv"
EIXO_CENTRAL_LON = -46.6130
BOTTOM_LAT, TOP_LAT = -23.5700, -23.5450
LEFT_LON, RIGHT_LON = -46.6300, -46.5950


def gerar_mapa(df):
    try:
        import folium
    except ImportError:
        print("Instale folium: pip install folium")
        return

    centro = [df["latitude"].mean(), df["longitude"].mean()]
    m = folium.Map(location=centro, zoom_start=15)

    cores = {"Sentido ABC (Sul)": "red", "Sentido Centro (Norte)": "blue"}

    for _, row in df.iterrows():
        cor = cores.get(row["sentido_via"], "gray")
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=6, color=cor, fill=True, fill_opacity=0.7,
            popup=(
                f"<b>{row.get('hora_brt', row.get('hora_utc', ''))}</b><br>"
                f"{row['tipo']}<br>"
                f"{row['sentido_via']}<br>"
                f"{row.get('rua', '')}"
            ),
        ).add_to(m)

    folium.PolyLine(
        [[BOTTOM_LAT, EIXO_CENTRAL_LON], [TOP_LAT, EIXO_CENTRAL_LON]],
        color="green", weight=2, dash_array="10",
    ).add_to(m)

    legend = """
    <div style="position:fixed;bottom:50px;left:50px;z-index:1000;
         background:white;padding:10px;border-radius:5px;border:1px solid #ccc;font-size:13px;">
    <b>Legenda</b><br>
    <span style="color:red;">&#9679;</span> Sentido ABC (Sul)<br>
    <span style="color:blue;">&#9679;</span> Sentido Centro (Norte)<br>
    <span style="color:green;">---</span> Eixo central
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend))
    m.save("dados/mapa_alertas.html")
    print("Mapa salvo: dados/mapa_alertas.html")


def analisar_transicoes(df, janela_min=30):
    df = df.sort_values("coleta_utc").reset_index(drop=True)
    df["coleta_utc"] = pd.to_datetime(df["coleta_utc"])
    transicoes = []

    for i in range(len(df) - 1):
        for j in range(i + 1, len(df)):
            delta = (df.loc[j, "coleta_utc"] - df.loc[i, "coleta_utc"]).total_seconds()
            if delta > janela_min * 60:
                break
            if df.loc[i, "sentido_via"] != df.loc[j, "sentido_via"]:
                transicoes.append({
                    "de": df.loc[i, "sentido_via"],
                    "para": df.loc[j, "sentido_via"],
                    "delta_min": delta / 60,
                    "lat_de": df.loc[i, "latitude"],
                    "lat_para": df.loc[j, "latitude"],
                    "hora_de": df.loc[i, "hora_utc"],
                    "hora_para": df.loc[j, "hora_utc"],
                })

    return pd.DataFrame(transicoes)


def main():
    if not os.path.exists(ARQUIVO_DADOS):
        print(f"Arquivo {ARQUIVO_DADOS} não encontrado.")
        sys.exit(1)

    df = pd.read_csv(ARQUIVO_DADOS)
    print(f"\n{'='*50}")
    print(f"ANÁLISE - {len(df)} alertas coletados")
    print(f"Período: {df['data'].min()} a {df['data'].max()}")
    print(f"{'='*50}")

    print("\nPor sentido:")
    print(df["sentido_via"].value_counts().to_string())

    print("\nPor tipo:")
    print(df["tipo"].value_counts().to_string())

    df_trans = analisar_transicoes(df)
    if len(df_trans) > 0:
        sul_norte = len(df_trans[df_trans["de"].str.contains("Sul")])
        norte_sul = len(df_trans[df_trans["de"].str.contains("Norte")])
        print(f"\nTransições detectadas: {len(df_trans)}")
        print(f"  Sul -> Norte: {sul_norte}")
        print(f"  Norte -> Sul: {norte_sul}")
        print(f"  Delta médio: {df_trans['delta_min'].mean():.1f} min")
        df_trans.to_csv("dados/transicoes.csv", index=False)
    else:
        print("\nNenhuma transição detectada (dados insuficientes).")

    gerar_mapa(df)


if __name__ == "__main__":
    main()
