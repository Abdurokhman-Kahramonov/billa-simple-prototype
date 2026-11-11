@echo off
cd /d "%~dp0.."
set "JAVA=C:\Program Files\Java\jdk-21\bin\java.exe"
set "JAR=target\loadTestBilla-0.0.1-SNAPSHOT.jar"

start "server-a (8081)" cmd /k ""%JAVA%" -Xmx1g -jar "%JAR%" --server.port=8081 --game.instance-id=a"
start "server-b (8082)" cmd /k ""%JAVA%" -Xmx1g -jar "%JAR%" --server.port=8082 --game.instance-id=b"
