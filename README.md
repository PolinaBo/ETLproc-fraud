# ETLproc-fraud
Скрипт main.ddl содержит код для создания таблиц в целевом DWH. Хранение таблиц измерений в SCD1 формате.
скрипт main.py содержит код для подключения к source DWH, выгрузке таблиц из него и загрузку в target DWH. Написан обработчик файлов.
Помимо этого, создан скрипт sql для формирования автоматического отчета по транзакциям,имеющим признаки подозрительных. Витрина строится накоплением.

