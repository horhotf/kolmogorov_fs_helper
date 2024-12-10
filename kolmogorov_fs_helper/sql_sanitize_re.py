import re

def sanitize_fields(fields):
    # Разрешить только алфавитно-цифровые символы, подчеркивания, звездочку, запятые и пробелы
    if not re.match(r'^[a-zA-Z0-9_*,\s]+$', fields):
        raise ValueError("Invalid fields provided")
    return fields

def sanitize_table_name(table_name):
    # Разрешить только алфавитно-цифровые символы и подчеркивания
    if not re.match(r'^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)?$', table_name):
        raise ValueError("Invalid table name provided")
    return table_name

def sanitize_filtred_params(filtred_params):
    # Разрешить фильтры: комбинации WHERE, операторы (=, <, >, <=, >=, !=), логические операторы (AND, OR)
    # и ключевые слова ORDER BY, GROUP BY, ASC, DESC, LIMIT, и прочее.
    pattern = r"""
        ^\s*                                           # Начало строки с пробелами
        (WHERE\s+                                      # Условие WHERE
        [a-zA-Z0-9_.]+                                 # Поле (буквы, цифры, подчеркивания, точки)
        \s*(=|!=|>|<|>=|<=)\s*                         # Оператор сравнения
        [a-zA-Z0-9_'\"]+                               # Значение (буквы, цифры, подчеркивания, кавычки)
        (\s+(AND|OR)\s+[a-zA-Z0-9_.]+\s*(=|!=|>|<|>=|<=)\s*[a-zA-Z0-9_'\"]+)*  # Допустимы AND/OR с условиями
        )?\s*                                          # Условие WHERE опционально
        (ORDER\s+BY\s+[a-zA-Z0-9_.\s,]+\s*(ASC|DESC)?\s*)?  # ORDER BY с ASC/DESC (опционально)
        (GROUP\s+BY\s+[a-zA-Z0-9_.\s,]+\s*)?           # GROUP BY (опционально)
        (LIMIT\s+\d+\s*)?                              # LIMIT <число> (опционально)
        (OFFSET\s+\d+\s*)?                             # OFFSET <число> (опционально)
        $                                              # Конец строки
    """
    if not re.match(pattern, filtred_params, flags=re.IGNORECASE | re.VERBOSE):
        raise ValueError("Invalid filtered parameters provided")
    return filtred_params