# Coletor de Alertas Waze - Av. dos Estados (SP)

Coleta automatizada de alertas do Waze no trecho da Av. dos Estados entre a Ligação Leste-Oeste e a Av. Anhaia Mello, via GitHub Actions.

## Objetivo

Analisar padrões de deslocamento de atividade criminal ao longo da via.

## Como funciona

- GitHub Actions executa `coleta_waze.py` a cada 5 minutos nos horários noturnos
- Acessa diretamente o endpoint público do Waze Live Map (sem API key)
- Alertas são salvos incrementalmente em `dados/alertas_waze.csv`
- `analise_waze.py` gera mapa interativo e detecta padrões de transição entre sentidos

## Configuração

1. Crie o repositório no GitHub
2. Suba os arquivos
3. Ative o GitHub Actions
4. Pronto — sem API key necessária

## Análise local

```bash
pip install -r requirements.txt
python analise_waze.py
```
