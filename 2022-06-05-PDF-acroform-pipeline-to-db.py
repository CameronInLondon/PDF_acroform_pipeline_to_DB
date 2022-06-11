# Extract from multiple acroforms to DB.


# from ast import Str
import traceback
import sys
import json
import sqlite3
import pandas as pd
# from collections import defaultdict
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdftypes import resolve1
from pdfminer.psparser import PSLiteral, PSKeyword
from pdfminer.utils import decode_text
# from pdfminer.high_level import extract_text
import os
from pathlib import Path
# import re
from glob import iglob
print(os.getcwd())
print(os.path.normpath(os.path.join(os.getcwd(), r'Blog\2022-06-05-PDF-acroform-pipeline-to-db')))
# print(sqlite3.sqlite_version)

folder_path = r'C:\Users\goldsby_c\OneDrive - Pearson PLC\myPython(onedrive)\GitHub\Blog\2022-06-05-PDF-acroform-pipeline-to-db'
# folder_path = os.path.normpath(os.path.join(
#     os.getcwd(), r'Blog\2022-06-05-PDF-acroform-pipeline-to-db'))

# ------ Global vars ------ #

list_of_nested_dicts = []


def error_msg(er):
    print('----error item start----')
    print('SQLite error: %s' % (' '.join(er.args)))
    print("Exception class is: ", er.__class__)
    print('SQLite traceback: ')
    exc_type, exc_value, exc_tb = sys.exc_info()
    print(traceback.format_exception(exc_type, exc_value, exc_tb))
    print('----error item end----')


def decode_value(value):
    # decode PSLiteral, PSKeyword
    if isinstance(value, (PSLiteral, PSKeyword)):
        value = value.name
    # decode bytes
    if isinstance(value, bytes):
        value = decode_text(value)
    return value


def extract_pdf_text(the_path):
    with open(the_path, 'rb') as fp:
        parser = PDFParser(fp)

        doc = PDFDocument(parser)
        res = resolve1(doc.catalog)

        if 'AcroForm' not in res:
            raise ValueError("No AcroForm Found")

        fields = resolve1(doc.catalog['AcroForm'])['Fields']  # may need further resolving

        # print file name
        p = Path(the_path)
        print(f'Form name# {p.stem}')
        data = {}
        for f in fields:
            # extract normal text
            # text = extract_text(file_path)
            # print(text)

            field = resolve1(f)
            name, values = field.get('T'), field.get('V')
            # decode name
            name = decode_text(name)
            # replace two spaces with one, then replace space with underscore
            name = name.replace("  ", " ").replace(" ", "_")
            # resolve indirect obj
            values = resolve1(values)
            # decode value(s)
            if isinstance(values, list):
                values = [decode_value(v) for v in values]
            else:
                values = decode_value(values)

            data.update({name: values})

        list_of_nested_dicts.append(data)
        return list_of_nested_dicts


def standardise_details_dict():
    """standardise the the details key values to DB schema.
        If PDF forms have differing fields this prevents error when inserting into DB.
        Note this only adds missing values, it does NOT update current values.
    """
    defaults2 = {
        'Field_1': None,
        'Field_2': None,
        'Field_3': None,
    }

    # loop through list of dicts. Each dicts is a paper.
    for form in list_of_nested_dicts:
        # extract from dict key, values
        for dic_k, dic_v in list(form.items()):
            # extract key values from the default dict
            for def_k, def_v in defaults2.items():
                # add to dict if there are empty fields
                form.setdefault(def_k, defaults2[def_k])


def create_and_insert_db(db_path):
    """create tables in DB
    Insert content in DB
    """

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = 0")  # only needed if you have more than one table
    conn.commit()
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS detail(
                    DetailID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Field_1 TEXT,
                    Field_2 TEXT,
                    Field_3 TEXT,
                    DetailKey TEXT GENERATED ALWAYS AS (IFNULL(Field_1, '') || IFNULL(Field_2, '') || IFNULL(Field_3, '')),
                    unique (DetailKey)
                    )''')
    conn.commit()

    for form in list_of_nested_dicts:  # iterate one form at a time.
        try:
            cursor.execute('''INSERT INTO detail(
                                Field_1,
                                Field_2,
                                Field_3)
                            VALUES (
                                :Field_1,
                                :Field_2,
                                :Field_3
                                );''', form)
        except sqlite3.Error as er:
            error_msg(er)

    conn.commit()


def loop_files():
    """loop through all PDF files in the folder"""
    for i in iglob(folder_path + '\*'):
        print(i)
        if Path(i).suffix == '.pdf':
            extract_pdf_text(i)


if __name__ == "__main__":
    loop_files()
    standardise_details_dict()
    print(json.dumps(list_of_nested_dicts, sort_keys=True, indent=4))
    db_path = (os.path.join(os.getcwd(), r'Blog\2022-06-05-PDF-acroform-pipeline-to-db\PDF-acroform-pipeline-to-db.db'))
    print(db_path)
    print('---------------')
    create_and_insert_db(db_path)
    # check DB
    conn = sqlite3.connect(db_path)
    print(pd.read_sql_query("SELECT * FROM detail LIMIT 20", conn))
    print('------------------')
    conn.close()
