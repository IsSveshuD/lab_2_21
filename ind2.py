#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse
import sqlite3
import typing as t
from pathlib import Path
import psycopg2


def connect():
    conn = psycopg2.connect(
        user="postgres",
        password="123321",
        host="127.0.0.1",
        port="5432",
        database="postgres"
    )

    return conn


def display_route(routes: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить списко маршрутов
    """
    if routes:
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 10
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^10} |'.format(
                "№",
                "Начальный пункт",
                "Конечный пункт",
                "№ маршрута"
            )
        )
        print(line)

        for idx, worker in enumerate(routes, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>10} |'.format(
                    idx,
                    worker.get('start', ''),
                    worker.get('finish', ''),
                    worker.get('number', 0)
                )
            )
        print(line)
    else:
        print("Список маршрутов пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Создать таблицу с информацией о маршрутах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS numbers (
            number_id INTEGER PRIMARY KEY AUTOINCREMENT,
            number_title TEXT NOT NULL
        )
        """
    )
    # Создать таблицу с информацией о маршрутах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS routes (
        routes_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start TEXT NOT NULL,
        finish INTEGER NOT NULL,
        number_id INTEGER NOT NULL,
        FOREIGN KEY(number_id) REFERENCES numbers(number_id)
        )
        """
    )
    conn.close()


def add_route(
        database_path: Path,
        start: str,
        finish: str,
        number: int
) -> None:
    """
    Добавить маршрут в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Получить индентификатор маршрута в базе данных.
    # Если такого нет, то добавить информацию о новом маршруте.
    cursor.execute(
        """
        SELECT number_id FROM numbers WHERE number_title = ?
        """,
        (number,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO numbers (number_title) VALUES (?)
            """,
            (number,)
        )
        number_id = cursor.lastrowid

    else:
        number_id = row[0]

    # Добавить информацию о новом работнике.
    cursor.execute(
        """
        INSERT INTO routes (start, finish, number_id)
        VALUES (?, ?, ?)
        """,
        (start, finish, number_id)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех работников.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT routes.start, routes.finish, numbers.number_title
        FROM routes
        INNER JOIN numbers ON numbers.number_id = routes.number_id
        """
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "start": row[0],
            "finish": row[1],
            "number": row[2],
        }
        for row in rows
    ]


def select_by_period(
    database_path: Path, period: int
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех работников с периодом работы больше заданного.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT routes.start, routes.finish, numbers.number_title
        FROM routes
        INNER JOIN numbers ON numbers.number_id = routes.number_id
        WHERE numbers.number_title == ?
        """,
        (period,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "start": row[0],
            "finish": row[1],
            "number": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "workers.db"),
        help="The data file name"
    )
    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("routes")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")
    # Создать субпарсер для добавления маршрута.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new route"
    )
    add.add_argument(
        "-s",
        "--start",
        action="store",
        required=True,
        help="The start of the route"
    )
    add.add_argument(
        "-f",
        "--finish",
        action="store",
        help="The finish of the route"
    )
    add.add_argument(
        "-n",
        "--number",
        action="store",
        type=int,
        required=True,
        help="The number of the route"
    )
    # Создать субпарсер для отображения всех маршрутов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all routes"
    )
    # Создать субпарсер для выбора маршрута.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the route"
    )
    select.add_argument(
        "-N",
        "--period",
        action="store",
        type=int,
        required=True,
        help="The route"
    )
    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)
    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить маршрут.
    if args.command == "add":
        add_route(db_path, args.start, args.finish, args.number)

    # Отобразить все маршруты.
    elif args.command == "display":
        display_route(select_all(db_path))

    # Выбрать требуемые маршруты.
    elif args.command == "select":
        display_route(select_by_period(db_path, args.period))
        pass


if __name__ == '__main__':
    main()
