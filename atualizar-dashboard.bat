@echo off
REM ============================================================
REM  Atualiza dados do dashboard e faz push para o GitHub
REM  Agendado para rodar diariamente as 4:30am
REM ============================================================

cd /d "C:\Users\maria\Projetos\Ideia-vesti"

echo [%date% %time%] Iniciando atualizacao... >> atualizar.log

REM 1. Reagregar dados dos CSVs
echo [%date% %time%] Executando build-data.js... >> atualizar.log
node build-data.js >> atualizar.log 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] ERRO ao executar build-data.js >> atualizar.log
    exit /b 1
)

REM 2. Verificar se dados.js mudou
git diff --quiet dados.js
if %errorlevel% equ 0 (
    echo [%date% %time%] Sem alteracoes em dados.js, nada a fazer. >> atualizar.log
    exit /b 0
)

REM 3. Commit e push
echo [%date% %time%] Commitando alteracoes... >> atualizar.log
git add dados.js
git commit -m "Atualizacao automatica dados %date%" >> atualizar.log 2>&1
git push origin main >> atualizar.log 2>&1

if %errorlevel% equ 0 (
    echo [%date% %time%] Push realizado com sucesso! >> atualizar.log
) else (
    echo [%date% %time%] ERRO no push >> atualizar.log
)

echo [%date% %time%] Concluido. >> atualizar.log
echo. >> atualizar.log
