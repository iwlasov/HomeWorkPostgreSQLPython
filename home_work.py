import psycopg2
from psycopg2.sql import SQL, Identifier

# Функция, создающая структуру БД (таблицы)
def create_db(conn):

    with conn.cursor() as cur:

        cur.execute("""
        DROP TABLE phones;
        DROP TABLE persons;
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS persons(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(20) NOT NULL,
            second_name VARCHAR(20) NOT NULL,  
            email VARCHAR(20) NOT NULL UNIQUE
        );
        """)

        cur.execute(""" 
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            phone VARCHAR(12),
            person_id INTEGER not null REFERENCES person(id)
        );
        """)

        conn.commit()  # фиксируем в БД

# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, second_name, email):

    with conn.cursor() as cur:

        cur.execute("""
            INSERT INTO persons (first_name, second_name, email)
            VALUES (%s,%s,%s)
            RETURNING id, first_name, second_name, email;
            """, (first_name, second_name, email))

        return cur.fetchone()

# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, phone, person_id):

    with conn.cursor() as cur:

        cur.execute("""
            INSERT INTO phones(phone, person_id)
            VALUES(%s, %s)
            RETURNING phone, person_id;
            """, (phone, person_id))

        return cur.fetchone()

# Функция, позволяющая изменить данные о клиенте
def change_client(conn, id, first_name = None, second_name = None, email = None):

    with conn.cursor() as cur:

        arg_list = {'first_name': first_name, 'second_name': second_name, 'email': email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('UPDATE persons SET {}=%s WHERE id = %s').format(Identifier(key)), (arg, id))

        cur.execute("""
            SELECT * FROM persons
            WHERE id = %s;
            """, id)

        return cur.fetchall()

# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, phone):

    with conn.cursor() as cur:

        cur.execute("""
            DELETE FROM phones
            WHERE phone=%s
            RETURNING person_id
            """, (phone,))

        return cur.fetchone()

# Функция, позволяющая удалить существующего клиента
def delete_client(conn, id):

    with conn.cursor() as cur:

        cur.execute("""
            DELETE FROM persons
            WHERE id = %s
            """, (id,))

        conn.commit()  # фиксируем в БД

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
def find_client(conn, first_name = None, second_name = None, email = None, phone = None):

    with conn.cursor() as cur:

        cur.execute("""
            SELECT * FROM persons
            JOIN phones ON persons.id = phones.person_id
            WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
            AND (second_name = %(second_name)s OR %(second_name)s IS NULL)
            AND (email = %(email)s OR %(email)s IS NULL)
            AND (phone = %(phone)s OR %(phone)s IS NULL);
            """, {'first_name': first_name, 'second_name': second_name, 'email': email, 'phone': phone})

        return cur.fetchall()

if __name__ == '__main__':

    with psycopg2.connect(database='netology_db', user='postgres', password='b2u18') as conn:

        create_db(conn)

        print(add_client(conn, 'Vadim', 'Vlasov', 'iwlasov@gmail.com'))
        print(add_client(conn, 'Alex', 'Germanov', 'germanov@mail.ru'))

        print(add_phone(conn, '89119315985', '1'))
        print(add_phone(conn, '89119315986', '1'))
        print(add_phone(conn, '89213409495', '2'))

        print(find_client(conn, 'Vadim'))
        print(find_client(conn, 'Vadim', 'lasov'))
        print(find_client(conn, second_name = 'Vlasov'))
        print(find_client(conn, phone = '89119315986'))
        print(find_client(conn, email = 'germanov@mail.ru'))

        print(change_client(conn, '1', 'Самарий', 'Бантиков'))
        print(change_client(conn, '2', email = 'AG@yandex.ru'))

        print(delete_phone(conn, '89119315985'))
        print(delete_phone(conn, '89119315986'))
        print(delete_phone(conn, '89213409495'))

        print(delete_client(conn, 1))
        print(delete_client(conn, 2))

    conn.close()