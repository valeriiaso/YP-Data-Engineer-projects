# 1.3. Качество данных

## Оцените, насколько качественные данные хранятся в источнике.
1. Определение дублей
Для таблиц из схемы production были написаны запросы с определением кол-ва уникальных записей vs общее кол-во записей для ключа таблицы
    - таблица orders ->  дубли отсутствуют 
    - таблица orderitems ->  дубли отсутствуют
    - таблица orderstatuses ->  дубли отсутствуют
    - таблица orderstatuslog ->  дубли отсутствуют
    - таблица products ->  дубли отсутствуют
    - таблица users ->  дубли отсутствуют
Пример запроса:
select count(id) as total, count(distinct id) as uniq
from users

2. Поиск пропусков
Для тех полей, которые необходиы для расчета метрик, был произведен поиск пропусков. Таким образом, получились следующие результаты:
    - для поля order_ts отсутсвуют строки, где значение данного поля равно NULL
    - для поля order_id отсутсвуют строки, где значение данного поля равно NULL
    - для поля payment отсутсвуют строки, где значение данного поля равно NULL
Пример запроса:
select *
from orders
where payment is null

## Укажите, какие инструменты обеспечивают качество данных в источнике.
Ответ запишите в формате таблицы со следующими столбцами:
- `Наименование таблицы` - наименование таблицы, объект которой рассматриваете.
- `Объект` - Здесь укажите название объекта в таблице, на который применён инструмент. Например, здесь стоит перечислить поля таблицы, индексы и т.д.
- `Инструмент` - тип инструмента: первичный ключ, ограничение или что-то ещё.
- `Для чего используется` - здесь в свободной форме опишите, что инструмент делает.

Пример ответа:

| Таблицы             | Объект                      | Инструмент      | Для чего используется |
| ------------------- | --------------------------- | --------------- | --------------------- |
| production.Products | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о пользователях |
| production.Products | price NOT NULL DEFAULT 0    | Ограничение NOT NULL | Обеспечивает заполненность поля price; если его значение равно NULL, то оно конвертируется в 0 |
| production.Products | products_price_check CHECK  | Ограничение - проверка | Проверяет, что занчение поля не является отрицательным |
| production.Users | id int NOT NULL PRIMARY KEY | Первичный ключ | Обеспечивает уникальность записей о пользователях |
| production.Users | login NOT NULL | Ограничение NOT NULL | Обеспечивает заполненность поля |
| production.orderstatuslog | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей |
| production.orderstatuslog | order_id int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderstatuslog | status_id int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderstatuslog | dttm timestamp NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderstatuslog | orderstatuslog_order_id_status_id_key UNIQUE | Ограничение уникальности | Гарантирует, что в таблицу попадут только строки с уникальным сочетанием столбцов order_id, status_id |
| production.orderstatuslog | orderstatuslog_order_id_fkey FOREIGN KEY | Ограничение внешнего ключа | Гарантирует, что в таблицу попадут только строки с ключом order_id, которые есть в таблице orders |
| production.orderstatuslog | orderstatuslog_status_id_fkey FOREIGN KEY | Ограничение внешнего ключа | Гарантирует, что в таблицу попадут только строки с ключом status_id, которые есть в таблице orderstatuses |
| production.orderstatuses | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей |
| production.orderstatuses | key varchar NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orders | order_id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей |
| production.orders | order_ts timestamp NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orders | user_id int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orders | bonus_payment NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
| production.orders | payment NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
| production.orders | cost NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
| production.orders | bonus_grant NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
| production.orders | status int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orders | orders_check CHECK | Ограничение - проверка | Определяет, что значение столбца cost должно быть равным payment + bonus_payment |
| production.orderitems | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей |
| production.orderitems | product_id int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderitems | order_id int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderitems | name varchar NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderitems | price NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
| production.orderitems | discount NOT NULL DEFAULT 0 | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL; если его значение равно NULL, то оно конвертируется в 0 |
уникальность записей |
| production.orderitems | quantity int NOT NULL | Ограничение NOT NULL | Определяет, что столбец не может принимать значение NULL |
| production.orderitems | orderitems_check CHECK CHECK | Ограничение - проверка | Определяет, что значение столбца discount не должно быть отрицательным, и роле price должно быть больше (или равным) полю discount |
| production.orderitems | orderitems_order_id_product_id_key UNIQUE | Ограничение уникальности | Гарантирует, что в таблицу попадут только строки с уникальным сочетанием столбцов order_id, status_id |
| production.orderitems | orderitems_price_check CHECK CHECK | Ограничение - проверка | Определяет, что значение столбца price не должно быть отрицательным |
| production.orderitems | orderitems_quantity_check CHECK CHECK | Ограничение - проверка | Определяет, что значение столбца quantity не должно быть меньше 0 |
| production.orderitems | orderitems_order_id_fkey FOREIGN KEY | Ограничение внешнего ключа | Гарантирует, что в таблицу попадут только строки с ключом order_id, которые есть в таблице orders |
| production.orderitems | orderitems_product_id_fkey FOREIGN KEY | Ограничение внешнего ключа | Гарантирует, что в таблицу попадут только строки с ключом product_id, которые есть в таблице products |