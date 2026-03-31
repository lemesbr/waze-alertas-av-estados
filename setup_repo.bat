@echo off
echo === Configurando repositorio GitHub ===

cd /d "%~dp0"

echo [1/5] Inicializando git...
git init

echo [2/5] Adicionando arquivos...
git add .gitignore README.md requirements.txt coleta_waze.py analise_waze.py dados/.gitkeep .github/workflows/coleta.yml

echo [3/5] Commit inicial...
git commit -m "feat: coleta automatizada de alertas Waze - Av. dos Estados (SP)"

echo [4/5] Configurando remote...
git branch -M main
git remote add origin https://github.com/lemesbr/waze-alertas-av-estados.git

echo [5/5] Push para GitHub...
git push -u origin main

echo.
echo === Pronto! Repositorio configurado com sucesso ===
echo https://github.com/lemesbr/waze-alertas-av-estados
pause
