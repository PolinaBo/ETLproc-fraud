#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import shutil
import re

### Источник
# Создание подключения к PostgreSQL
conn_src = psycopg2.connect(database = "bank",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "bank_etl",
                            password = "bank_etl_password",
                            port =     "5432")
# Отключение автокоммита
conn_src.autocommit = False
# Создание курсора
cursor_src = conn_src.cursor()
### Data Warehouse
# Создание подключения к PostgreSQL
conn_dwh = psycopg2.connect(database = "edu",
                            host =     "de-edu-db.chronosavant.ru",
                            user =     "deaise",
                            password = "meriadocbrandybuck",
                            port =     "5432")
# Отключение автокоммита
conn_dwh.autocommit = False
# Создание курсора
cursor_dwh = conn_dwh.cursor()
########################################################################
#1.Очистка stage
cursor_dwh.execute("""DELETE FROM deaise.bopo_stg_terminals;
                      DELETE FROM deaise.bopo_stg_blacklist;
                      DELETE FROM deaise.bopo_stg_transactions;
                      DELETE FROM deaise.bopo_stg_cards;
                      DELETE FROM deaise.bopo_stg_accounts;
                      DELETE FROM deaise.bopo_stg_clients""")
#2.Загрузка данных в Data Warehouse
#2.1Выгрузка данных из источника
cursor_src.execute( """ SELECT client_id, last_name, first_name, patronymic, date_of_birth, passport_num,
                               passport_valid_to, phone, create_dt, update_dt
                        FROM info.clients """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )
#Запись в stage
cursor_dwh.executemany("""INSERT INTO deaise.bopo_stg_clients(client_id, last_name, first_name, patronimic,
                                 date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
                      VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""", df.values.tolist() )
#Повторим операцию для остальных таблиц
cursor_src.execute( """ SELECT  account, valid_to, client, create_dt, update_dt
                        FROM info.accounts """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )
cursor_dwh.executemany("""INSERT INTO deaise.bopo_stg_accounts(account_num, valid_to, client, create_dt, update_dt)
                      VALUES( %s, %s, %s, %s,%s )""", df.values.tolist() )
cursor_src.execute( """ SELECT card_num, account, create_dt, update_dt
                        FROM info.cards """ )

records = cursor_src.fetchall()
df = pd.DataFrame( records )
cursor_dwh.executemany("""INSERT INTO deaise.bopo_stg_cards(card_num, account_num, create_dt, update_dt)
                      VALUES( %s, %s, %s, %s )""", df.values.tolist() )


#2.2 Загрузка данных с сервера ETL

#Указываем путь к файлам и архиву. Забираем список файлов.
source_dir = '/home/deaise/bopo/project'
arch_dir = '/home/deaise/bopo/project/archive'
files = os.listdir(source_dir)

#Цикл для обработки и перемещения файлов

for file in files:
    if file.startswith('passport'):
        passport = file
        df_pass = pd.read_excel(passport, sheet_name='blacklist', header=0, index_col=None)
       
        cursor_dwh.executemany ("""INSERT INTO deaise.bopo_stg_blacklist(entry_dt, passport_num)
                                   VALUES( %s, %s)""", df_pass.values.tolist())
        file_path = os.path.join(source_dir, passport)
        new_fname = file.replace('.xlsx', '.xlsx.backup')
        new_file_path = os.path.join(arch_dir, new_fname)
        shutil.move(file_path, new_file_path)
    if file.startswith('termin'):
        termin = file
        df_term = pd.read_excel(termin, sheet_name='terminals', header=0, index_col=None)
        match = re.search(r'_(\d{8})\.xlsx', termin)
        date_str = match.group(1)
        update_dt = pd.to_datetime(date_str, format='%d%m%Y')
        df_term['update_dt'] = update_dt
        cursor_dwh.executemany ("""INSERT INTO deaise.bopo_stg_terminals(terminal_id, terminal_type, terminal_city,
                                                                         terminal_address,update_dt)
                                   VALUES( %s, %s, %s, %s, %s)""", df_term.values.tolist())
        file_path = os.path.join(source_dir, termin)
        new_fname = file.replace('.xlsx', '.xlsx.backup')
        new_file_path = os.path.join(arch_dir, new_fname)
        shutil.move(file_path, new_file_path)
    if file.startswith('transac'):
        transac = file
        df_trans = pd.read_csv(transac, sep=';', decimal=',')
        match = re.search(r'_(\d{8})\.txt', transac)
        date_str = match.group(1)
        update_dt = pd.to_datetime(date_str, format='%d%m%Y')
        df_trans['update_dt'] = update_dt
        cursor_dwh.executemany ("""INSERT INTO deaise.bopo_stg_transactions(trans_id, trans_date, amt,card_num,
                                                                         oper_type, oper_result, terminal, 
                                                                         update_dt)
                                   VALUES( %s, %s, %s, %s, %s, %s, %s, %s)""", df_trans.values.tolist())
        file_path = os.path.join(source_dir, transac)
        new_fname = file.replace('.txt', '.txt.backup')
        new_file_path = os.path.join(arch_dir, new_fname)
        shutil.move(file_path, new_file_path)
print ('Запись в stage закончена')

#2.3 Формирование фактовых таблиц

cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_fact_passport_blacklist (entry_dt, passport_num)
                         SELECT entry_dt, passport_num
                         FROM deaise.bopo_stg_blacklist""")
                         
cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_fact_transactions (trans_id ,trans_date, card_num ,
oper_type ,amt,oper_result ,terminal)
                         SELECT trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal
                         FROM deaise.bopo_stg_transactions""")
                         
#2.4 Заполним таблицы измерений
#2.4.1 CLIENTS
#--  Загрузка в приемник "вставок" на источнике (формат SCD1)
cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_dim_clients (client_id, last_name,first_name,
patronimic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt
)
                        SELECT stg.client_id, stg.last_name, stg.first_name, stg.patronimic, stg.date_of_birth,
stg.passport_num, stg.passport_valid_to,stg.phone,stg.create_dt, stg.update_dt
                         FROM deaise.bopo_stg_clients stg
                          left join deaise.bopo_dwh_dim_clients tgt
                            on 1=1
                            and stg.client_id = tgt.client_id
                         WHERE tgt.client_id is null """)
#--  Обновление в приемнике "обновлений" на источнике (формат SCD1).                    
cursor_dwh.execute ( """ UPDATE deaise.bopo_dwh_dim_clients tgt
                        	 set   last_name           = tmp.last_name,
                        	       first_name          = tmp.first_name,
                        	       patronimic          = tmp.patronimic,
                        	       date_of_birth       = tmp.date_of_birth,
                        	       passport_num        = tmp.passport_num,
                        	       passport_valid_to   = tmp.passport_valid_to,
                        	       phone               = tmp.phone,
                        	       update_dt           = tmp.update_dt
                         FROM (
                            SELECT
                               stg.client_id,
                           	   stg.last_name,
                           	   stg.first_name,
                           	   stg.patronimic,
                           	   stg.date_of_birth,
                           	   stg.passport_num,
                           	   stg.passport_valid_to,
                           	   stg.phone,
                           	   stg.update_dt
                            FROM deaise.bopo_stg_clients stg
                             inner join deaise.bopo_dwh_dim_clients tgt
                               on 1=1
                               and stg.client_id = tgt.client_id
                            WHERE ( 1=0
                                or stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null) or (stg.last_name is not null and tgt.last_name is null)
                                or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null) or (stg.first_name is not null and tgt.first_name is null)
                                or stg.patronimic <> tgt.patronimic or (stg.patronimic is null and tgt.patronimic is not null) or (stg.patronimic is not null and tgt.patronimic is null)
                                or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null) or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null) or (stg.passport_num is not null and tgt.passport_num is null)
                                or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null) or (stg.phone is not null and tgt.phone is null)
                            ) ) tmp
                        WHERE tgt.client_id = tmp.client_id """ )
#--  Обработка удалений в приемнике (формат SCD1).
#cursor_dwh.execute ( """ DELETE FROM deaise.bopo_dwh_dim_clients
                        # WHERE client_id in (
                         #   SELECT tgt.client_id
                          #  FROM deaise.bopo_dwh_dim_clients tgt
                           #  left join deaise.bopo_stg_clients_del stg
                            #    on 1=1
                             #   and stg.client_id = tgt.client_id
                           # WHERE stg.client_id is null ) """)

#2.4.2 ACCOUNTS
#--  Загрузка в приемник "вставок" на источнике (формат SCD1
cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_dim_accounts (
                            account_num,
                        	valid_to,
                        	client,
                        	create_dt,
                        	update_dt)
                         SELECT
                            stg.account_num,
                        	stg.valid_to,
                        	stg.client,
                        	stg.create_dt,
                        	stg.update_dt
                         FROM deaise.bopo_stg_accounts stg
                          left join deaise.bopo_dwh_dim_accounts tgt
                            on 1=1
                            and stg.account_num = tgt.account_num
                         WHERE tgt.account_num is null """)

#-- Обновление в приемнике "обновлений" на источнике (формат SCD1).
cursor_dwh.execute ( """ UPDATE deaise.bopo_dwh_dim_accounts tgt
                        	 set   valid_to        = tmp.valid_to,
                        	       client          = tmp.client,
                        	       update_dt       = tmp.update_dt
                         FROM (
                            SELECT
                               stg.account_num,
                           	   stg.valid_to,
                           	   stg.client,
                           	   stg.update_dt
                            FROM deaise.bopo_stg_accounts stg
                             inner join deaise.bopo_dwh_dim_accounts tgt
                               on 1=1
                               and stg.account_num = tgt.account_num
                            WHERE ( 1=0
                                or stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null) or (stg.valid_to is not null and tgt.valid_to is null)
                                or stg.client <> tgt.client or (stg.client is null and tgt.client is not null) or (stg.client is not null and tgt.client is null)
                            ) ) tmp
                        WHERE tgt.account_num = tmp.account_num """ )
#--  Обработка удалений в приемнике (формат SCD1
#cursor_dwh.execute ( """ DELETE FROM deaise.bopo_dwh_dim_accounts
 #                        WHERE account_num in (
  #                          SELECT tgt.account_num
   #                         FROM deaise.bopo_dwh_dim_accounts tgt
    #                         left join deaise.bopo_stg_accounts_del stg
     #                           on 1=1
      #                          and stg.account = tgt.account_num
       #                     WHERE stg.account is null ) """)
#2.4.3 CARDS
#--  Загрузка в приемник "вставок" на источнике (формат SCD1
cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_dim_cards (
                            card_num,
                        	account_num,
                        	create_dt,
                        	update_dt
                         )
                         SELECT
                            stg.card_num,
                        	stg.account_num,
                        	stg.create_dt,
                        	stg.update_dt
                         FROM deaise.bopo_stg_cards stg
                          left join deaise.bopo_dwh_dim_cards tgt
                            on 1=1
                            and stg.card_num = tgt.card_num
                         WHERE tgt.card_num is null """)
#-- Обновление в приемнике "обновлений" на источнике (формат SCD1).
cursor_dwh.execute ( """UPDATE deaise.bopo_dwh_dim_cards tgt
                        	 set   account_num     = tmp.account_num,
                        	       update_dt       = tmp.update_dt
                         FROM (
                            SELECT
                               stg.card_num,
                           	   stg.account_num,
                           	   stg.update_dt
                            FROM deaise.bopo_stg_cards stg
                             inner join deaise.bopo_dwh_dim_cards tgt
                               on 1=1
                               and stg.card_num = tgt.card_num
                            WHERE ( 1=0
                                or stg.account_num <> tgt.account_num or (stg.account_num is null and tgt.account_num is not null) or (stg.account_num is not null and tgt.account_num is null)
                            ) ) tmp
                        WHERE tgt.card_num = tmp.card_num """ )
#-- Обработка удалений в приемнике (формат SCD1).
#cursor_dwh.execute(""" DELETE FROM deaise.bopo_dwh_dim_cards
#                         WHERE card_num in (
#                            SELECT tgt.card_num
#                            FROM deaise.bopo_dwh_dim_cards tgt
#                             left join deaise.bopo_stg_cards_del stg
#                                on 1=1
#                                and stg.card_num = tgt.card_num
#                            WHERE stg.card_num is null ) """)

cursor_dwh.execute ('''UPDATE deaise.bopo_dwh_dim_cards
SET card_num = RTRIM(card_num)''')

#2.4.4 TERMINALS
#--  Загрузка в приемник "вставок" на источнике (формат SCD1
cursor_dwh.execute ( """ INSERT INTO deaise.bopo_dwh_dim_terminals (
                            terminal_id,
                        	terminal_type,
                        	terminal_city,
                        	terminal_address,
                        	update_dt
                         )
                         SELECT
                            stg.terminal_id,
                        	stg.terminal_type,
                        	stg.terminal_city,
                        	stg.terminal_address,
                        	stg.update_dt
                         FROM deaise.bopo_stg_terminals stg
                          left join deaise.bopo_dwh_dim_terminals tgt
                            on 1=1
                            and stg.terminal_id = tgt.terminal_id
                         WHERE tgt.terminal_id is null """)
#-- Обновление в приемнике "обновлений" на источнике (формат SCD1).
cursor_dwh.execute ( """ UPDATE deaise.bopo_dwh_dim_terminals tgt
                        	 set   terminal_type     = tmp.terminal_type,
                                   terminal_city     = tmp.terminal_city,
                                   terminal_address  = tmp.terminal_address,
                        	       update_dt         = tmp.update_dt
                         FROM (
                            SELECT
                               stg.terminal_id,
                               stg.terminal_type,
                               stg.terminal_city,
                               stg.terminal_address,
                           	   stg.update_dt
                            FROM deaise.bopo_stg_terminals stg
                             inner join deaise.bopo_dwh_dim_terminals tgt
                               on 1=1
                               and stg.terminal_id = tgt.terminal_id
                            WHERE ( 1=0
                                or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
                                or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
                                or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)
                            ) ) tmp
                        WHERE tgt.terminal_id = tmp.terminal_id """ )
#-- Обработка удалений в приемнике (формат SCD1).
cursor_dwh.execute ( """ DELETE FROM deaise.bopo_dwh_dim_terminals
                         WHERE terminal_id in (
                            SELECT tgt.terminal_id
                            FROM deaise.bopo_dwh_dim_terminals tgt
                             left join deaise.bopo_stg_terminals stg
                                on 1=1
                                and stg.terminal_id = tgt.terminal_id
                            WHERE stg.terminal_id is null ) """)

print('заполнение таблиц фактов и измерений завершено')
#3 Формирование отчета по фроду

#3.1 for event_type 1 and 2

cursor_dwh.execute ( '''INSERT INTO deaise.bopo_rep_fraud (
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt
)
select * from (with client_info as (SELECT
                                *
                            FROM deaise.bopo_dwh_dim_cards cards
                               left join deaise.bopo_dwh_dim_accounts acc
                                 on cards.account_num = acc.account_num
                                left join deaise.bopo_dwh_dim_clients client
                                 on acc.client = client.client_id
								 )
SELECT fact_t.trans_date as event_dt,
	   client_info.passport_num as passport,
	   client_info.last_name||' '||client_info.first_name||' '||client_info.patronimic as fio,
	   client_info.phone phone,
	   case
	        when (black.passport_num is not null and fact_t.trans_date::date >= black.entry_dt) or 
			     (fact_t.trans_date::date > client_info.passport_valid_to) THEN '1'
			when (fact_t.trans_date::date > client_info.valid_to) THEN '2'   
			   
	   end as event_type,
	   (now() - interval '1 day')::date as report_dt
FROM deaise.bopo_dwh_fact_transactions fact_t
                            left join client_info
                             on fact_t.card_num = client_info.card_num
							 left join deaise.bopo_dwh_fact_passport_blacklist black
                             on black.passport_num = client_info.passport_num) as one
where event_type is not null''')

#3.1 for event_type 3

cursor_dwh.execute ( '''INSERT INTO deaise.bopo_rep_fraud (
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt
)
with term_info as
(select trans_id, trans_date, card_num, terminal, terminal_city
FROM deaise.bopo_dwh_fact_transactions fact_t
							left join deaise.bopo_dwh_dim_terminals term
							on fact_t.terminal = term.terminal_id),
sorted_t AS (
  SELECT
    trans_id,
    card_num,
    terminal_city,
    trans_date,
    LAG(terminal_city) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_city,
    LAG(trans_date) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_time
  FROM term_info),
client_info as (SELECT
                                *
                            FROM deaise.bopo_dwh_dim_cards cards
                                left join deaise.bopo_dwh_dim_accounts acc
                                 on cards.account_num = acc.account_num
                                left join deaise.bopo_dwh_dim_clients client
                                 on acc.client = client.client_id)
SELECT 
  trans_date as event_dt,
  client_info.passport_num as passport,
  client_info.last_name||' '||client_info.first_name||' '||client_info.patronimic as fio,
  client_info.phone as phone,
  '3' as event_type,
  now():: date as report_dt
FROM sorted_t
left join client_info
on sorted_t.card_num = client_info.card_num
WHERE terminal_city <> prev_city
  AND EXTRACT(EPOCH FROM (trans_date - prev_time)) <= 3600;''')
  
#3.1 for event_type 4

cursor_dwh.execute('''INSERT INTO deaise.bopo_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt) 
with ranked_t AS (
  SELECT
    trans_id,
    trans_date,
	card_num,
	oper_type,
	amt,
	oper_result,
    LAG(trans_date, 1) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_time,
	LAG(amt,1) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_amt,
	LAG(oper_result, 1) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_res,
	LAG(trans_date, 2) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_time_2,
	LAG(amt,2) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_amt_2,
	LAG(oper_result, 2) OVER (PARTITION BY card_num ORDER BY trans_date) AS prev_res_2
  FROM deaise.bopo_dwh_fact_transactions),
client_info as (SELECT
                                *
                            FROM deaise.bopo_dwh_dim_cards cards
                                left join deaise.bopo_dwh_dim_accounts acc
                                 on cards.account_num = acc.account_num
                                left join deaise.bopo_dwh_dim_clients client
                                 on acc.client = client.client_id)
SELECT 
  
  trans_date as event_dt,
  client_info.passport_num as passport,
  client_info.last_name||' '||client_info.first_name||' '||client_info.patronimic as fio,
  client_info.phone as phone,
  '4' as event_type,
  now():: date as report_dt
FROM ranked_t
left join client_info
on ranked_t.card_num = client_info.card_num
WHERE oper_result = 'SUCCESS' AND prev_res = 'REJECT' AND prev_res_2 = 'REJECT'
  AND amt < prev_amt and prev_amt < prev_amt_2
  AND EXTRACT(EPOCH FROM (trans_date - prev_time_2)) <= 1200;''')

print ('отчет загружен')
conn_dwh.commit()
print(' Закрытие подключений')
cursor_src.close() 
cursor_dwh.close()
conn_src.close()
conn_dwh.close()
