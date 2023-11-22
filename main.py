#!/usr/bin/env python3

import psycopg2
import pandas as pd
import os
from datetime import datetime

# Открываем лог для записи событий
log_events = open("events.log", "a+")

# Подключаемся к БД bank
try:
    conn = psycopg2.connect(database = "bank",
                            host =     "***",
                            user =     "***",
                            password = "***",
                            port =     "5432")
    log_events.write(f"{datetime.now()} - Успешное подключение к БД Bank\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Подключение к БД Bank не удалось!: {e}\n")

conn.autocommit = False
cursor = conn.cursor()

# Функция, предназначенная для захвата данных
def fetch_data_to_dataframe(cursor, query):
    cursor.execute(query)
    records = cursor.fetchall()
    names = [x[0] for x in cursor.description]
    return pd.DataFrame(records, columns=names)

# Получение данные о счетах
df_accounts = fetch_data_to_dataframe(cursor, "SELECT * FROM bank.info.accounts")

# Получение данные о картах
df_cards = fetch_data_to_dataframe(cursor, "SELECT * FROM bank.info.cards")

# Получение данные о клиентах
df_clients = fetch_data_to_dataframe(cursor, "SELECT * FROM bank.info.clients")

# Подключение к БД edu
try:
    conn = psycopg2.connect(database = "edu",
                            host =     "***",
                            user =     "***",
                            password = "***",
                            port =     "5432")
    log_events.write(f"{datetime.now()} - Успешное подключение к БД edu\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Подключение к БД edu не удалось!: {e}\n")                            
cursor = conn.cursor()

###############################################################################
# Очистка staging
###############################################################################
try:
    cursor.execute("""
    DELETE FROM deaian.ptrv_stg_accounts;
    DELETE FROM deaian.ptrv_stg_cards;
    DELETE FROM deaian.ptrv_stg_clients;
    DELETE FROM deaian.ptrv_stg_terminals;
    DELETE FROM deaian.ptrv_stg_passport_blacklist;
    DELETE FROM deaian.ptrv_stg_transactions;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Очистка стейджинговых таблиц прошла успешно\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Стейджинг таблицы не очищены: {e}\n")  

###############################################################################
# Загрузка данных 
###############################################################################


# Проверка наличия в meta-таблице данных о первоначальной загрузке
###############################################################################

# Транзакции
cursor.execute("""
INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
SELECT 
    'deaian' as schema_name,
    'deaian.ptrv_stg_terminals' as table_name,
    TO_DATE('1900-01-01','YYYY-MM-DD') as update_dt
WHERE NOT EXISTS (
    SELECT 1 
    FROM deaian.ptrv_meta_transactions
)
""")

# Счета
cursor.execute("""
INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
SELECT 
    'deaian' as schema_name,
    'deaian.ptrv_stg_accounts' as table_name,
    TO_DATE('1900-01-01','YYYY-MM-DD') as update_dt
WHERE NOT EXISTS (
    SELECT 1 
    FROM deaian.ptrv_meta_transactions
)
""")

# Карты
cursor.execute("""
INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
SELECT 
    'deaian' as schema_name,
    'deaian.ptrv_stg_cards' as table_name,
    TO_DATE('1900-01-01','YYYY-MM-DD') as update_dt
WHERE NOT EXISTS (
    SELECT 1 
    FROM deaian.ptrv_meta_transactions
)
""")

cursor.execute("""
INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
SELECT 
    'deaian' as schema_name,
    'deaian.ptrv_stg_terminals' as table_name,
    TO_DATE('1900-01-01','YYYY-MM-DD') as update_dt
WHERE NOT EXISTS (
    SELECT 1 
    FROM deaian.ptrv_meta_transactions
)
""")

# Загрузка в стейджинг
###############################################################################

# Счета
try:
    cursor.executemany( "INSERT INTO deaian.ptrv_stg_accounts( account_num, valid_to, client, create_dt, update_dt, processed_dt) VALUES( %s, %s, %s, %s, %s, now())", df_accounts.values.tolist() )
    cursor.execute("""
    INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
    SELECT 
        'deaian' as schema_name,
        'deaian.ptrv_stg_accounts' as table_name,
        now() as update_dt
    """)
    conn.commit()   
    log_events.write(f"{datetime.now()} -Успешная вставка данных в таблицу deaian.ptrv_stg_accounts\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_accounts: {e}\n")
    
# Карты
try:
    cursor.executemany( "INSERT INTO deaian.ptrv_stg_cards( card_num, account_num, create_dt, update_dt, processed_dt) VALUES( %s, %s, %s, %s, now())", df_cards.values.tolist() )
    cursor.execute("""
    INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
    SELECT 
        'deaian' as schema_name,
        'deaian.ptrv_stg_cards' as table_name,
        now() as update_dt
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} -Успешная вставка данных в таблицу deaian.ptrv_stg_cards\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_cards: {e}\n")

# Клиенты
try:
    cursor.executemany( "INSERT INTO deaian.ptrv_stg_clients( client_id, lastname, firstname, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt, processed_dt) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())", df_clients.values.tolist() )
    cursor.execute("""
    INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
    SELECT 
        'deaian' as schema_name,
        'deaian.ptrv_stg_clients' as table_name,
        now() as update_dt
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} -Успешная вставка данных в таблицу deaian.ptrv_stg_clients\n")
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_clients: {e}\n")

   

# Терминалы
files = os.listdir()
matching_files = [file for file in files if file.startswith('terminals') and file.endswith('.xlsx')]
if matching_files:
    file_name = matching_files[0]
    date_string = matching_files[0].split('_')[1].split('.')[0]
    date_terminals = datetime.strptime(date_string, '%d%m%Y').date()
    df_terminals = pd.read_excel(file_name, sheet_name='terminals', header=0, index_col=None)
    log_events.write(f"{datetime.now()} - Успешное считывание данных из {file_name}\n")
    os.rename(f'/home/deaian/ptrv/project/{file_name}', f'/home/deaian/ptrv/project/archive/{file_name}.backup')
else:
    log_events.write(f"{datetime.now()} - Файл terminals не найден!\n")

try:
    values = [(value1, value2, value3, value4, date_terminals, datetime.now()) for value1, value2, value3, value4 in df_terminals.values]
    cursor.execute("SELECT max(update_dt) FROM deaian.ptrv_meta_transactions WHERE schema_name = 'deaian' AND table_name = 'deaian.ptrv_stg_terminals'")
    max_update_dt = cursor.fetchone()[0]
    max_update_dt = max_update_dt.date()
    date_terminals_str = date_terminals.strftime('%Y-%m-%d %H:%M:%S')
    if date_terminals > max_update_dt:
        cursor.executemany("INSERT INTO deaian.ptrv_stg_terminals ( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, processed_dt) VALUES ( %s, %s, %s, %s, %s, %s )", values)   
        cursor.execute("""
        INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
        SELECT
            'deaian' as schema_name,
            'deaian.ptrv_stg_terminals' as table_name,
            %s as update_dt
        """, (date_terminals_str,))
        conn.commit()
        log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_stg_terminals\n")
    else:
        log_events.write(f"{datetime.now()} - Дата данных о терминалах в хранилище более актуальна, чем дата файла с информацией о терминалах\n")
        
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_terminals: {e}\n")

# Blacklist паспортов
files = os.listdir()
matching_files = [file for file in files if file.startswith('passport_blacklist') and file.endswith('.xlsx')]
if matching_files:
    file_name = matching_files[0]
    date_string = matching_files[0].split('_')[2].split('.')[0]
    date_passports = datetime.strptime(date_string, '%d%m%Y').date()
    df_passports = pd.read_excel(file_name, sheet_name='blacklist', header=0, index_col=None)
    log_events.write(f"{datetime.now()} - Успешное считывание данных из {file_name}\n")
    os.rename(f'/home/deaian/ptrv/project/{file_name}', f'/home/deaian/ptrv/project/archive/{file_name}.backup')
else:
    log_events.write(f"{datetime.now()} - Файл passport_blacklist не найден!\n")

try:
    values = [(value1, value2, date_passports, datetime.now()) for value1, value2 in df_passports.values]
    cursor.executemany( "INSERT INTO deaian.ptrv_stg_passport_blacklist( date, passport_num, create_dt, processed_dt ) VALUES( %s, %s, %s, %s )", values )
    cursor.execute("""
    INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
    SELECT 
        'deaian' as schema_name,
        'deaian.ptrv_stg_passport_blacklist' as table_name,
        now() as update_dt
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_stg_passport_blacklist\n")    
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_passport_blacklist: {e}\n")

# Транзакции
files = os.listdir()
matching_files = [file for file in files if file.startswith('transactions') and file.endswith('.txt')]
if matching_files:
    file_name = matching_files[0]
    date_string = matching_files[0].split('_')[1].split('.')[0]
    date_transactions = datetime.strptime(date_string, '%d%m%Y').date()
    df_transactions = pd.read_csv(file_name, sep=';')
    log_events.write(f"{datetime.now()} - Успешное считывание данных из {file_name}\n")
    os.rename(f'/home/deaian/ptrv/project/{file_name}', f'/home/deaian/ptrv/project/archive/{file_name}.backup')
else:
    log_events.write(f"{datetime.now()} - Файл transactions не найден!\n")

try:
    df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
    df_transactions['amount'] = pd.to_numeric(df_transactions['amount'].str.replace(',', '.'), errors='coerce')
    values = [(value1, value2, value3, value4, value5, value6, value7, date_transactions, datetime.now()) for value1, value2, value3, value4, value5, value6, value7 in df_transactions.values]
    cursor.executemany( "INSERT INTO deaian.ptrv_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal, create_dt,  processed_dt ) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s)", values )
    cursor.execute("""
    INSERT INTO deaian.ptrv_meta_transactions (schema_name, table_name, update_dt)
    SELECT 
        'deaian' as schema_name,
        'deaian.ptrv_stg_transactions' as table_name,
        now() as update_dt
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_stg_transactions\n")    
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_stg_transactions: {e}\n") 

###############################################################################
# Применение данных в приемник - счета
###############################################################################

# Вставка
try:
    cursor.execute("""
        insert into deaian.ptrv_dwh_dim_accounts_hist (account_num, valid_to, client, effective_from, effective_to, deleted_flg, processed_dt)
        select
            psa.account_num, 
            psa.valid_to, 
            psa.client, 
            psa.create_dt,
            TO_DATE('2999-12-31', 'YYYY-MM-DD'),
            'N', 
            now() 
        from deaian.ptrv_stg_accounts psa
        left join deaian.ptrv_dwh_dim_accounts_hist pddah
        on psa.account_num = pddah.account_num
        where pddah.account_num is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_dwh_dim_accounts_hist \n")    
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_dwh_dim_accounts_hist: {e}\n") 

# Обновление записей
try:
    cursor.execute("""
        UPDATE deaian.ptrv_dwh_dim_accounts_hist
        SET 
            effective_to = tmp.update_dt,
            processed_dt = NOW()
        FROM (
            SELECT
                stg.account_num, 
                stg.valid_to,
                stg.client,
                stg.update_dt 
            FROM deaian.ptrv_stg_accounts stg
            LEFT JOIN deaian.ptrv_dwh_dim_accounts_hist tgt
                ON stg.account_num = tgt.account_num
            WHERE (stg.update_dt is not NULL) AND (tgt.effective_to = '2999-12-31 00:00:00.000')
            ) tmp
        WHERE 
            deaian.ptrv_dwh_dim_accounts_hist.account_num = tmp.account_num 
            AND deaian.ptrv_dwh_dim_accounts_hist.effective_to = '2999-12-31 00:00:00.000';
        
        INSERT INTO deaian.ptrv_dwh_dim_accounts_hist (account_num, valid_to, client, effective_from, effective_to, deleted_flg, processed_dt)
        SELECT
            stg.account_num, 
            stg.valid_to,
            stg.client,
            stg.update_dt + INTERVAL '1 second',
            TO_DATE('2999-12-31', 'YYYY-MM-DD'),
            'N',
            NOW()
        FROM deaian.ptrv_stg_accounts stg
        LEFT JOIN deaian.ptrv_dwh_dim_accounts_hist tgt ON stg.account_num = tgt.account_num
        WHERE 
            (stg.update_dt is not NULL) and (stg.update_dt > tgt.effective_from);
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное обновление данных в таблице deaian.ptrv_dwh_dim_accounts_hist \n")    
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при обновлении данных в таблице deaian.ptrv_dwh_dim_accounts_hist: {e}\n") 
    
# Удаление записей
try:
    cursor.execute("""
        update deaian.ptrv_dwh_dim_accounts_hist t
        set
            processed_dt = now(),
            effective_to = del.create_dt
        from deaian.ptrv_stg_accounts del
        where t.account_num in (
            select tgt.account_num
            from deaian.ptrv_dwh_dim_accounts_hist tgt
            left join deaian.ptrv_stg_accounts stg on tgt.account_num = stg.account_num
            where stg.account_num is null
        ) and t.deleted_flg = 'Y' and t.effective_to = TO_DATE('2999-12-31','YYYY-MM-DD');
        
        insert into deaian.ptrv_dwh_dim_accounts_hist (
            account_num
            , valid_to
            , client
            , effective_from
            , effective_to
            , deleted_flg
            , processed_dt
        )
        select
            tgt.account_num
            , tgt.valid_to
            , tgt.client
            , tgt.effective_to + INTERVAL '1 second'
            , TO_DATE('2999-12-31', 'YYYY-MM-DD')
            , 'Y'
            , NOW()
        from deaian.ptrv_dwh_dim_accounts_hist tgt
        where 
        tgt.account_num in (
            select tgt.account_num
            from deaian.ptrv_dwh_dim_accounts_hist tgt
            left join deaian.ptrv_stg_accounts stg on tgt.account_num = stg.account_num
            where stg.account_num is null)
        and tgt.account_num not in (
            select tgt.account_num
            from deaian.ptrv_dwh_dim_accounts_hist tgt
            WHERE tgt.deleted_flg = 'Y')
        and exists (
            select 1
            from deaian.ptrv_stg_accounts
            limit 1 );
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное удаление данных в таблице deaian.ptrv_dwh_dim_accounts_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при удалении данных в таблице deaian.ptrv_dwh_dim_accounts_hist: {e}\n")

###############################################################################
# Применение данных в приемник - карты
###############################################################################

# Вставка
try:
    cursor.execute("""insert into deaian.ptrv_dwh_dim_cards_hist (card_num, account_num, effective_from, effective_to, deleted_flg, processed_dt)
    select
        psc.card_num,
        psc.account_num, 
        psc.create_dt,
        TO_DATE('2999-12-31', 'YYYY-MM-DD'), 
        'N',
        now() 
    from deaian.ptrv_stg_cards psc
    left join deaian.ptrv_dwh_dim_cards_hist pddch
    on psc.card_num = pddch.card_num
    where pddch.account_num is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_dwh_dim_cards_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_dwh_dim_cards_hist: {e}\n")

# Обновление записей
try:
    cursor.execute("""
        UPDATE deaian.ptrv_dwh_dim_cards_hist
        SET 
            effective_to = tmp.update_dt,
            processed_dt = NOW()
        FROM (
            SELECT
                stg.card_num, 
                stg.account_num,
                stg.update_dt 
            FROM deaian.ptrv_stg_cards stg
            LEFT JOIN deaian.ptrv_dwh_dim_cards_hist tgt
                ON stg.card_num = tgt.card_num
            WHERE (stg.update_dt is not NULL) AND (tgt.effective_to = '2999-12-31 00:00:00.000')
            ) tmp
        WHERE 
            deaian.ptrv_dwh_dim_cards_hist.card_num = tmp.card_num 
            AND deaian.ptrv_dwh_dim_cards_hist.effective_to = '2999-12-31 00:00:00.000';
        
        INSERT INTO deaian.ptrv_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg, processed_dt)
        SELECT
            stg.card_num,
            stg.account_num, 
            stg.update_dt + INTERVAL '1 second',
            TO_DATE('2999-12-31', 'YYYY-MM-DD'),
            'N',
            NOW()
        FROM deaian.ptrv_stg_cards stg
        LEFT JOIN deaian.ptrv_dwh_dim_cards_hist tgt ON stg.card_num = tgt.card_num
        WHERE 
            (stg.update_dt is not NULL) and (stg.update_dt > tgt.effective_from);
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное обновление данных в таблице deaian.ptrv_dwh_dim_cards_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при обновлении данных в таблице deaian.ptrv_dwh_dim_cards_hist: {e}\n")

# Удаление записей
try:
    cursor.execute("""
        update deaian.ptrv_dwh_dim_cards_hist t
        set
            processed_dt = now(),
            effective_to = del.create_dt
        from deaian.ptrv_stg_cards del
        where t.card_num in (
            select tgt.card_num
            from deaian.ptrv_dwh_dim_cards_hist tgt
            left join deaian.ptrv_stg_cards stg on tgt.card_num = stg.card_num
            where stg.account_num is null
        ) and t.deleted_flg = 'Y' and t.effective_to = TO_DATE('2999-12-31','YYYY-MM-DD');
        
        insert into deaian.ptrv_dwh_dim_cards_hist (
            card_num
            ,account_num
            , effective_from
            , effective_to
            , deleted_flg
            , processed_dt
        )
        select
            tgt.card_num
            , tgt.account_num
            , tgt.effective_to + INTERVAL '1 second'
            , TO_DATE('2999-12-31', 'YYYY-MM-DD')
            , 'Y'
            , NOW()
        from deaian.ptrv_dwh_dim_cards_hist tgt
        where 
        tgt.card_num in (
            select tgt.card_num
            from deaian.ptrv_dwh_dim_cards_hist tgt
            left join deaian.ptrv_stg_cards stg on tgt.card_num = stg.card_num
            where stg.card_num is null)
        and tgt.card_num not in (
            select tgt.card_num
            from deaian.ptrv_dwh_dim_cards_hist tgt
            WHERE tgt.deleted_flg = 'Y')
        and exists (
            select 1
            from deaian.ptrv_stg_cards
            limit 1 );
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное удаление данных в таблице deaian.ptrv_dwh_dim_cards_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при удалении данных в таблице deaian.ptrv_dwh_dim_cards_hist: {e}\n")
    
###############################################################################
# Применение данных в приемник - клиенты
###############################################################################

# Вставка
try:
    cursor.execute("""
        insert into deaian.ptrv_dwh_dim_clients_hist (client_id, lastname, firstname,patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg, processed_dt)
        select
            pscl.client_id,
            pscl.lastname,
            pscl.firstname,
            pscl.patronymic,
            pscl.date_of_birth,
            pscl.passport_num,
            pscl.passport_valid_to,
            pscl.phone,
            pscl.create_dt,
            TO_DATE('2999-12-31', 'YYYY-MM-DD'), 
            'N',
            now() 
        from deaian.ptrv_stg_clients pscl
        left join deaian.ptrv_dwh_dim_clients_hist pddclh
        on pscl.client_id = pddclh.client_id
        where pddclh.client_id is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_dwh_dim_clients_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_dwh_dim_clients_hist: {e}\n")

# Обновление записей
try:
    cursor.execute("""
        UPDATE deaian.ptrv_dwh_dim_clients_hist
        SET 
            effective_to = tmp.update_dt,
            processed_dt = NOW()
        FROM (
            SELECT
                stg.client_id,
                stg.update_dt
            FROM deaian.ptrv_stg_clients stg
            LEFT JOIN deaian.ptrv_dwh_dim_clients_hist tgt
                ON stg.client_id = tgt.client_id
            WHERE (stg.update_dt is not NULL) AND (tgt.effective_to = '2999-12-31 00:00:00.000')
            ) tmp
        WHERE 
            deaian.ptrv_dwh_dim_clients_hist.client_id = tmp.client_id 
            AND deaian.ptrv_dwh_dim_clients_hist.effective_to = '2999-12-31 00:00:00.000';
        
        INSERT INTO deaian.ptrv_dwh_dim_clients_hist ( client_id, lastname, firstname,patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg, processed_dt)
        SELECT
            stg.client_id, 
            stg.lastname, 
            stg.firstname,
            stg.patronymic, 
            stg.date_of_birth, 
            stg.passport_num, 
            stg.passport_valid_to, 
            stg.phone, 
            stg.update_dt + INTERVAL '1 second',
            TO_DATE('2999-12-31', 'YYYY-MM-DD'),
            'N',
            NOW()
        FROM deaian.ptrv_stg_clients stg
        LEFT JOIN deaian.ptrv_dwh_dim_clients_hist tgt ON stg.client_id = tgt.client_id
        WHERE 
            (stg.update_dt is not NULL) and (stg.update_dt > tgt.effective_from);
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное обновление данных в таблице deaian.ptrv_dwh_dim_clients_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при обновлении данных в таблице deaian.ptrv_dwh_dim_clients_hist: {e}\n")

# Удаление записей
try:
    cursor.execute("""
        update deaian.ptrv_dwh_dim_clients_hist t
        set
            processed_dt = now(),
            effective_to = del.create_dt
        from deaian.ptrv_stg_clients del
        where t.client_id in (
            select tgt.client_id
            from deaian.ptrv_dwh_dim_clients_hist tgt
            left join deaian.ptrv_stg_clients stg on tgt.client_id = stg.client_id
            where stg.client_id is null
        ) and t.deleted_flg = 'Y' and t.effective_to = TO_DATE('2999-12-31','YYYY-MM-DD');
        
        insert into deaian.ptrv_dwh_dim_clients_hist (client_id, lastname, firstname,patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg, processed_dt
        )
        select
            tgt.client_id
            , tgt.lastname
            , tgt.firstname
            , tgt.patronymic
            , tgt.date_of_birth
            , tgt.passport_num
            , tgt.passport_valid_to
            , tgt.phone
            , tgt.effective_to + INTERVAL '1 second'
            , TO_DATE('2999-12-31', 'YYYY-MM-DD')
            , 'Y'
            , NOW()
        from deaian.ptrv_dwh_dim_clients_hist tgt
        where 
        tgt.client_id in (
            select tgt.client_id
            from deaian.ptrv_dwh_dim_clients_hist tgt
            left join deaian.ptrv_stg_clients stg on tgt.client_id = stg.client_id
            where stg.client_id is null)
        and tgt.client_id not in (
            select tgt.client_id
            from deaian.ptrv_dwh_dim_clients_hist tgt
            WHERE tgt.deleted_flg = 'Y')
        and exists (
            select 1
            from deaian.ptrv_stg_clients
            limit 1 );
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное удаление данных в таблице deaian.ptrv_dwh_dim_clients_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при удалении данных в таблице deaian.ptrv_dwh_dim_clients_hist: {e}\n")
    
###############################################################################
# Применение данных в приемник - терминалы
###############################################################################

# Вставка
try:
    cursor.execute("""
        insert into deaian.ptrv_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg, processed_dt)
        select
            pst.terminal_id,
            pst.terminal_type,
            pst.terminal_city,
            pst.terminal_address,
            pst.create_dt,
            TO_DATE('2999-12-31', 'YYYY-MM-DD'), 
            'N',
            now() 
        from deaian.ptrv_stg_terminals pst
        left join deaian.ptrv_dwh_dim_terminals_hist pddth
        on pst.terminal_id = pddth.terminal_id
        where pddth.terminal_id is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешная вставка данных в таблицу deaian.ptrv_dwh_dim_terminals_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при вставке данных в таблицу deaian.ptrv_dwh_dim_terminals_hist: {e}\n")
    
# Обновление записей
try:
    cursor.execute("""
    UPDATE deaian.ptrv_dwh_dim_terminals_hist
    SET 
    effective_to = tmp.create_dt,
    processed_dt = NOW()
    FROM (
    SELECT
        stg.terminal_id, 
        stg.terminal_city,
        stg.terminal_address,
        stg.create_dt 
    FROM deaian.ptrv_stg_terminals stg
    LEFT JOIN deaian.ptrv_dwh_dim_terminals_hist tgt
        ON stg.terminal_id = tgt.terminal_id
    WHERE (stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)) OR (stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)) AND (tgt.effective_to = '2999-12-31 00:00:00.000')
    ) tmp
    WHERE 
    ptrv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id 
    AND ptrv_dwh_dim_terminals_hist.effective_to = '2999-12-31 00:00:00.000';
    
    INSERT INTO deaian.ptrv_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg, processed_dt)
    SELECT
    stg.terminal_id, 
    stg.terminal_type, 
    stg.terminal_city, 
    stg.terminal_address, 
    stg.create_dt + INTERVAL '1 second',
    TO_DATE('2999-12-31', 'YYYY-MM-DD'),
    'N',
    NOW()
    FROM deaian.ptrv_stg_terminals stg
    LEFT JOIN deaian.ptrv_dwh_dim_terminals_hist tgt
    ON stg.terminal_id = tgt.terminal_id
    WHERE (stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)) OR (stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)) AND (stg.create_dt = tgt.effective_to);
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное обновление данных в таблице deaian.ptrv_dwh_dim_terminals_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при обновлении данных в таблице deaian.ptrv_dwh_dim_terminals_hist: {e}\n")

# Удаление записей
try:
    cursor.execute("""
    UPDATE deaian.ptrv_dwh_dim_terminals_hist t
    SET
        processed_dt = NOW(),
        effective_to = del.create_dt
    FROM deaian.ptrv_stg_terminals del
    WHERE t.terminal_id IN (
        SELECT tgt.terminal_id
        FROM deaian.ptrv_dwh_dim_terminals_hist tgt
        LEFT JOIN deaian.ptrv_stg_terminals stg
        ON tgt.terminal_id = stg.terminal_id
        WHERE stg.terminal_id IS NULL
    ) AND t.deleted_flg = 'N' AND t.effective_to = TO_DATE('2999-12-31','YYYY-MM-DD');
    
    INSERT INTO deaian.ptrv_dwh_dim_terminals_hist (
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from,
        effective_to,
        deleted_flg,
        processed_dt
    )
    SELECT
        tgt.terminal_id,
        tgt.terminal_type,
        tgt.terminal_city,
        tgt.terminal_address,
        tgt.effective_to + INTERVAL '1 second',
        TO_DATE('2999-12-31', 'YYYY-MM-DD'),
        'Y',
        NOW()
    FROM deaian.ptrv_dwh_dim_terminals_hist tgt
    WHERE tgt.terminal_id IN (
        SELECT tgt.terminal_id
        FROM deaian.ptrv_dwh_dim_terminals_hist tgt
        LEFT JOIN deaian.ptrv_stg_terminals stg ON tgt.terminal_id = stg.terminal_id
        WHERE stg.terminal_id IS NULL
    ) AND tgt.terminal_id NOT IN (
        SELECT tgt.terminal_id
        FROM deaian.ptrv_dwh_dim_terminals_hist tgt
        WHERE tgt.deleted_flg = 'Y'
    ) AND EXISTS (
        SELECT 1
        FROM deaian.ptrv_stg_terminals
        LIMIT 1
    );
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное удаление данных в таблице deaian.ptrv_dwh_dim_terminals_hist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при удалении данных в таблице deaian.ptrv_dwh_dim_terminals_hist: {e}\n")

###############################################################################
# Пополнение таблицы фактов - паспорта в черном списке
###############################################################################

try:
    cursor.execute("""
        insert into deaian.ptrv_dwh_fact_passport_blacklist ( entry_dt, passport_num, create_dt, update_dt, processed_dt)
        select
            pspb.date,
            pspb.passport_num,
            pspb.create_dt,
            null, 
            now() 
        from deaian.ptrv_stg_passport_blacklist pspb
        left join deaian.ptrv_dwh_fact_passport_blacklist pddpb
        on pspb.passport_num = pddpb.passport_num
        where pddpb.passport_num is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное пополнение таблицы фактов deaian.ptrv_dwh_fact_passport_blacklist \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при пополнении таблицы фактов deaian.ptrv_dwh_fact_passport_blacklist: {e}\n")
    
###############################################################################
# Пополнение таблицы фактов - транзации
###############################################################################

try:
    cursor.execute("""
        insert into deaian.ptrv_dwh_fact_transactions ( trans_id, card_num, trans_date, oper_type, amt, oper_result, terminal, create_dt, update_dt, processed_dt)
        select
            pst.trans_id,
            pst.card_num,
            pst.trans_date,
            pst.oper_type,
            pst.amt,
            pst.oper_result,
            pst.terminal,
            pst.create_dt, 
            null, 
            now()
        from deaian.ptrv_stg_transactions pst
        left join deaian.ptrv_dwh_fact_transactions pdft
        on pst.trans_id = pdft.trans_id
        where pdft.trans_id is null;
    """)
    conn.commit()
    log_events.write(f"{datetime.now()} - Успешное пополнение таблицы фактов deaian.ptrv_dwh_fact_transactions \n")  
except Exception as e:
    log_events.write(f"{datetime.now()} - Ошибка при пополнении таблицы фактов deaian.ptrv_dwh_fact_transactions: {e}\n")

###############################################################################
# Ищем мошеннические операции и наполняем витрину deaian.ptrv_rep_fraud
###############################################################################

#1 - просроченный паспорт или в черном списке  
cursor.execute("""insert into deaian.ptrv_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
select 
	pst.trans_date,
	pddclh.passport_num,
	pddclh.lastname || ' ' || pddclh.firstname || ' ' || pddclh.patronymic as fio,
	pddclh.phone,
	'1',
	%s
from deaian.ptrv_stg_transactions pst 
left join deaian.ptrv_dwh_dim_cards_hist pddch on pst.card_num = pddch.card_num
left join deaian.ptrv_dwh_dim_accounts_hist pddah on pddah.account_num = pddch.account_num
left join deaian.ptrv_dwh_dim_clients_hist pddclh on pddclh.client_id = pddah.client
left join deaian.ptrv_dwh_fact_passport_blacklist pdfpb on pdfpb.passport_num = pddclh.passport_num
where pddclh.passport_valid_to < pst.trans_date or pdfpb.passport_num IS NOT null;
""", (date_transactions,))
conn.commit()

#2 - операции при недействующем договоре
cursor.execute("""insert into deaian.ptrv_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
select 
	pst.trans_date,
	pddclh.passport_num,
	pddclh.lastname || ' ' || pddclh.firstname || ' ' || pddclh.patronymic as fio,
	pddclh.phone,
	'2' as event_type,
	%s
from deaian.ptrv_stg_transactions pst 
left join deaian.ptrv_dwh_dim_cards_hist pddch on pst.card_num = pddch.card_num
left join deaian.ptrv_dwh_dim_accounts_hist pddah on pddah.account_num = pddch.account_num
left join deaian.ptrv_dwh_dim_clients_hist pddclh on pddclh.client_id = pddah.client
where pddah.valid_to < pst.trans_date;
""", (date_transactions,))
conn.commit()

#3 - совершение операций в разных городах в течение 1 часа
cursor.execute("""insert into deaian.ptrv_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
with cte_fraud as (SELECT 
	pst.trans_date 
	,pddclh.passport_num
    ,pddclh.lastname || ' ' || pddclh.firstname || ' ' || pddclh.patronymic as fio
    ,pddclh.phone
    , '3'
    ,pddth.terminal_city
    , lag(pddth.terminal_city) over (partition by pst.card_num order by pst.trans_date) as last_city
    , lead(pddth.terminal_city) over (partition by pst.card_num order by pst.trans_date) as next_city
    , lag(pst.trans_date) over (partition by pst.card_num order by pst.trans_date) as last_time
    , lead(pst.trans_date) over (partition by pst.card_num order by pst.trans_date) as next_time
    ,now()
FROM deaian.ptrv_dwh_fact_transactions pst 
LEFT JOIN deaian.ptrv_dwh_dim_cards_hist pddch ON pst.card_num = pddch.card_num
LEFT JOIN deaian.ptrv_dwh_dim_accounts_hist pddah ON pddah.account_num = pddch.account_num
LEFT JOIN deaian.ptrv_dwh_dim_clients_hist pddclh ON pddclh.client_id = pddah.client
LEFT JOIN deaian.ptrv_dwh_dim_terminals_hist pddth ON pddth.terminal_id = pst.terminal)

select 
	cte_fraud.trans_date,
	cte_fraud.passport_num,
	cte_fraud.fio,
	cte_fraud.phone,
	'3',
	%s
from cte_fraud
left join deaian.ptrv_rep_fraud on cte_fraud.trans_date = deaian.ptrv_rep_fraud.event_dt 
where (cte_fraud.terminal_city <> cte_fraud.last_city or cte_fraud.terminal_city <> cte_fraud.next_city) and (EXTRACT(EPOCH FROM (cte_fraud.next_time - cte_fraud.last_time)) / 3600 <= 1) and (ptrv_rep_fraud.event_dt is null);
""", (date_transactions,))
conn.commit()

# 4 - попытка подбора суммы
cursor.execute("""insert into deaian.ptrv_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
with cte_fraud as (SELECT 
	pst.trans_date 
	,pddclh.passport_num
    ,pddclh.lastname || ' ' || pddclh.firstname || ' ' || pddclh.patronymic as fio
    ,pddclh.phone
    , '3'
    ,pddth.terminal_city
    , lag(pst.trans_date, 3) over (partition by pst.card_num order by pst.trans_date) as third_last_time
    , lag(pst.oper_result, 3) over (partition by pst.card_num order by pst.trans_date) as third_last_result
    , lag(pst.amt, 3) over (partition by pst.card_num order by pst.trans_date) as third_last_amt
    , lag(pst.trans_date, 2) over (partition by pst.card_num order by pst.trans_date) as second_last_time
    , lag(pst.oper_result, 2) over (partition by pst.card_num order by pst.trans_date) as second_last_result
    , lag(pst.amt, 2) over (partition by pst.card_num order by pst.trans_date) as second_last_amt
    , lag(pst.trans_date) over (partition by pst.card_num order by pst.trans_date) as first_last_time
    , lag(pst.oper_result) over (partition by pst.card_num order by pst.trans_date) as first_last_result
    , lag(pst.amt) over (partition by pst.card_num order by pst.trans_date) as first_last_amt
    , pst.oper_result  as oper_result 
    , pst.amt 
    ,now()
FROM deaian.ptrv_dwh_fact_transactions pst 
LEFT JOIN deaian.ptrv_dwh_dim_cards_hist pddch ON pst.card_num = pddch.card_num
LEFT JOIN deaian.ptrv_dwh_dim_accounts_hist pddah ON pddah.account_num = pddch.account_num
LEFT JOIN deaian.ptrv_dwh_dim_clients_hist pddclh ON pddclh.client_id = pddah.client
LEFT JOIN deaian.ptrv_dwh_dim_terminals_hist pddth ON pddth.terminal_id = pst.terminal)

select 
	cte_fraud.trans_date,
	cte_fraud.passport_num,
	cte_fraud.fio,
	cte_fraud.phone,
	'4',
	%s
from cte_fraud
left join deaian.ptrv_rep_fraud on cte_fraud.trans_date = deaian.ptrv_rep_fraud.event_dt 
where (third_last_amt > second_last_amt and second_last_amt > first_last_amt and first_last_amt > amt) and (third_last_result = 'REJECT' and second_last_result = 'REJECT' and first_last_result = 'REJECT' and oper_result = 'SUCCESS') and (EXTRACT(EPOCH FROM (cte_fraud.trans_date - cte_fraud.third_last_time)) <= 1200) and (ptrv_rep_fraud.event_dt is null);
""", (date_transactions,))
conn.commit()

log_events.write(f"{datetime.now()} - Отработаны скрипты по пополнению витрины данных deaian.ptrv_rep_fraud \n") 

# Закрываем лог, курсор и соединение
log_events.close()
cursor.close()
conn.close()


