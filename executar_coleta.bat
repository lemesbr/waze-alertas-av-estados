@echo off
:: Coleta Waze - Agendador de Tarefas Windows
:: Coloca o diretório do script como pasta de trabalho

cd /d "%~dp0"

:: Verifica se Python está disponível
where python >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado no PATH.
    exit /b 1
)

:: Instala dependencias se necessário (silencioso)
python -m pip install requests pandas --quiet --disable-pip-version-check >nul 2>&1

:: Executa a coleta e grava log com timestamp
set LOGFILE=%~dp0dados\coleta.log
echo [%DATE% %TIME%] Iniciando coleta >> "%LOGFILE%"
python "%~dp0coleta_waze.py" >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] Concluido (exit: %ERRORLEVEL%) >> "%LOGFILE%"
