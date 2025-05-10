@echo off
setlocal enabledelayedexpansion


pushd "%~dp0.."
set "ROOT_DIR=!CD!"
popd


for /l %%i in (1,1,5) do (
  start "Node%%i" cmd /k ^"cd /d "!ROOT_DIR!" ^&^& go run main.go --config "clusters\node%%i\config.yaml"^"
)