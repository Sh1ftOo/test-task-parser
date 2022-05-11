from __future__ import annotations

import os
import uuid
import shutil
import string
import pandas as pd
from typing import NoReturn
from multiprocessing import Pool
import xml.etree.ElementTree as ET
from random import randint, SystemRandom

'''
Написать программу на Python, которая делает следующие действия:

1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:

<root>
    <var name=’id’ value=’<случайное уникальное строковое значение>’/>
    <var name=’level’ value=’<случайное число от 1 до 100>’/>
    <objects>
        <object name=’<случайное строковое значение>’/>
        <object name=’<случайное строковое значение>’/>
        …
    </objects>
</root>

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
Первый: id, level - по одной строке на каждый xml файл
Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на каждый xml файл)

Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора.
'''

ZIP_COUNT = 50
XML_COUNT = 100
LETTER_COUNT = 10
MIN_RANDOM_VALUE = 1
MAX_RANDOM_VALUE = 100


def get_string() -> str:
    """Get string of random letters"""
    return ''.join(SystemRandom().choice(string.ascii_letters) for _ in range(LETTER_COUNT))


def remove_files_in_dir(path: str) -> NoReturn:
    """
    Remove all files in directory
    :param path: path to files which wants to remove
    """
    for file in os.listdir(path):
        os.remove(os.path.join(path, file))


class XML:
    """A class to using xml (create, generate, get attributes or list, etc)"""

    def __init__(self) -> None:
        self.xml_folder = "xml"
        self.xml_id = ""
        self.xml_level = ""

    @staticmethod
    def generate_xml() -> ET.Element:
        """
        xml generation according to the task
        :return: xml object
        :rtype: xml.etree.ElementTree.Element
        """
        root = ET.Element("root")
        ET.SubElement(root, "var", name="id", value=str(uuid.uuid1()))
        ET.SubElement(root, "var", name="level", value=str(randint(MIN_RANDOM_VALUE, MAX_RANDOM_VALUE)))
        objects = ET.SubElement(root, "objects")

        for _ in range(randint(1, 10)):
            rand_string = get_string()
            ET.SubElement(objects, "object", name=rand_string)

        return root

    def create_xml_file(self, name: str) -> NoReturn:
        root = XML.generate_xml()
        tree = ET.ElementTree(root)
        ET.indent(tree, space="\t", level=0)
        tree.write(os.path.join(self.xml_folder, name + '.xml'), encoding="utf-8", xml_declaration=True)

    @staticmethod
    def get_xml_parse(file: File) -> dict:
        """Get parsed dictionary from file"""
        xml_list = XML.get_xml_list(file)
        return XML.get_xml_attributes(xml_list)

    @staticmethod
    def get_xml_list(file: File) -> list:
        """
        Get elements (tags, attributes) from xml file
        :param file: object that wants to parse from filepath to elements of xml file
        :return: list of dictionaries with tags and attributes
        :rtype: list
        """
        tree = ET.parse(file.filepath)
        root = tree.getroot()
        return [{elem.tag: elem.attrib} for elem in root.iter()]

    @staticmethod
    def get_xml_attributes(xml_list: list) -> dict:
        """
        Get attributes from list of dictionaries which consists of tags and attributes of xml file
        :param xml_list: list of dictionaries
        :return: dictionary of lists. Lists consist of information about xml file
        :rtype: dict
        """
        file_id = ""
        level_values = []
        level_keys = []
        object_name_values = []
        object_name_keys = []
        for xml_dict in xml_list:
            for key, val in xml_dict.items():
                if key == 'var':
                    if val['name'] == 'id':
                        file_id = val['value']

                    if val['name'] == 'level':
                        level_values.append((file_id, val['value']))
                        level_keys = ['id', 'levels']

                if key == 'object':
                    object_name_values.append((file_id, val['name']))
                    object_name_keys = ['id', 'obj_name']

        return dict({
            'level_values': level_values,
            'level_columns': level_keys,
            'object_name_values': object_name_values,
            'object_name_columns': object_name_keys
        })


class CSV:
    """A class with different operations on CSV files (create, write, save)"""

    @staticmethod
    def create_csv(content: dict, columns: dict) -> pd.DataFrame:
        return pd.DataFrame(content, columns=columns)

    @staticmethod
    def save_to_csv(obj: pd.DataFrame, filename: str) -> NoReturn:
        obj.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)

    def write_to_csv(self, **kwargs: dict) -> NoReturn:
        content = kwargs['content']
        filenames = kwargs['filenames_path']

        levels = self.create_csv(content=content['level_values'], columns=content['level_columns'])
        self.save_to_csv(levels, filenames['levels'])

        obj_names = self.create_csv(content=content['object_name_values'], columns=content['object_name_columns'])
        self.save_to_csv(obj_names, filenames['obj_names'])


class File:
    """A class with information about the current file"""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath


class Parser:
    """A class to parse file in any formats"""

    def parse(self, file: File, file_format: str) -> dict:
        parser = get_parser(file_format)
        return parser(file)


def get_parser(file_format: str) -> function:
    """
    Creator for choice parse files
    :param file_format:
    :type file_format: str
    :return: particular parse function
    :rtype: function
    """
    if file_format == 'xml':
        return XML.get_xml_parse
    else:
        raise ValueError(file_format)


class Converter:
    """A class to convert files to/from format"""

    def __init__(
            self,
            output_filename: str = "",
            output_dir_path: str = "zip",
            archive_type: str = "zip",
            convert_dir_name: str = "xml"
    ) -> None:
        """
        Constructs all attributes for converting files
        :param output_dir_path: directory path for output converted archive
        :type output_filename:str
        :param archive_type: type of archive to convert
        :type archive_type:str
        :param convert_dir_name: directory path to source files which wants to be converted
        :type convert_dir_name:str
        """
        self.output_dir_path = output_dir_path
        self.archive_type = archive_type
        self.convert_dir_name = convert_dir_name
        self.path = os.path.join(output_dir_path, output_filename)

    def convert_to_zip(self) -> NoReturn:
        """Convert files to zip format"""
        shutil.make_archive(self.path, self.archive_type, self.convert_dir_name)
        remove_files_in_dir(self.convert_dir_name)

    def convert_from_zip(self) -> NoReturn:
        """Convert files from zip format"""
        for i in os.listdir(self.path):
            shutil.unpack_archive(os.path.join(self.output_dir_path, i), self.convert_dir_name)


def run_to_zip() -> NoReturn:
    if not os.path.isdir("xml"):
        os.mkdir("xml")

    if not os.path.isdir("zip"):
        os.mkdir("zip")

    number = 0
    for i in range(ZIP_COUNT):
        for _ in range(XML_COUNT):
            name = "test" + str(number)
            XML().create_xml_file(name)
            number += 1

        converter = Converter(output_filename="test" + str(i))
        converter.convert_to_zip()


def multiprocessing_function(filename: str) -> NoReturn:
    """A function for write attributes from xml file to csv file according to the task"""
    filepath = os.path.join("xml", filename)
    file = File(filepath)
    attributes_dict = Parser().parse(file, filepath.split('.')[-1])
    CSV().write_to_csv(
        content=attributes_dict,
        filenames_path={'levels': 'csv/levels.csv', 'obj_names': 'csv/obj_names.csv'}
    )


def run_from_zip() -> NoReturn:
    converter = Converter(output_dir_path='zip', convert_dir_name='xml')
    converter.convert_from_zip()

    if not os.path.isdir("csv"):
        os.mkdir("csv")

    pool = Pool()
    for filename in os.listdir("xml"):
        pool.apply_async(multiprocessing_function, (filename,))


if __name__ == '__main__':
    run_to_zip()
    run_from_zip()
