@echo off
REM ============================================================
REM  Atualiza dados do dashboard CS-Painel e faz push para o GitHub
REM  Agendado para rodar diariamente as 4:30am
REM ============================================================

cd /d "C:\Users\maria\Projetos\Ideia-vesti\CS-Painel"

echo [%date% %time%] Iniciando atualizacao CS-Painel... >> atualizar.log

REM 1. Reagregar dados dos CSVs
echo [%date% %time%] Executando build-data.js... >> atualizar.log
node build-data.js >> atualizar.log 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] ERRO ao executar build-data.js >> atualizar.log
    exit /b 1
)

REM 2. Verificar se dados.js mudou
cd /d "C:\Users\maria\Projetos\Ideia-vesti"
git diff --quiet CS-Painel/dados.js
if %errorlevel% equ 0 (
    echo [%date% %time%] Sem alteracoes em dados.js, nada a fazer. >> CS-Painel\atualizar.log
    exit /b 0
)

REM 3. Commit e push
echo [%date% %time%] Commitando alteracoes... >> CS-Painel\atualizar.log
git add CS-Painel/dados.js
git commit -m "Atualizacao automatica CS-Painel %date%" >> CS-Painel\atualizar.log 2>&1
git push origin main >> CS-Painel\atualizar.log 2>&1

if %errorlevel% equ 0 (
    echo [%date% %time%] Push realizado com sucesso! >> CS-Painel\atualizar.log
) else (
    echo [%date% %time%] ERRO no push >> CS-Painel\atualizar.log
)

echo [%date% %time%] Concluido. >> CS-Painel\atualizar.log
echo. >> CS-Painel\atualizar.log
