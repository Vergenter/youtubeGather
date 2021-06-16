"""Unit tests"""
# pylint: disable=R0201,C0116
import json
import unittest
from models.Video import fromJson
from process.language.LanguageProcessor import isVideoEnglish
from process.VideoProcess import process


class TestJsonParsing(unittest.TestCase):
    """Main Unit tests"""

    def test_parse_basic(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as file:
            basic_data = json.load(file)
        fromJson(basic_data)

    def test_parse_with_none_title(self):
        with open('src//tests//resources//noGameTitleItem.json', 'r', encoding='utf-8') as file:
            advanced_data = json.load(file)
        fromJson(advanced_data)

    def test_parse_language(self):
        with open('src//tests//resources//noGameTitleItem.json', 'r', encoding='utf-8') as file:
            advanced_data = json.load(file)
        self.assertFalse(isVideoEnglish(advanced_data))

    def test_parse_language_empty_description(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as file:
            basic_data = json.load(file)
        basic_data["snippet"]["description"] = ''
        self.assertFalse(isVideoEnglish(basic_data))

    def test_parse_language_whitespace_description(self):
        with open('src//tests//resources//whitespaceDescription.json', 'r', encoding='utf-8') as file:
            whitespaceDescription = json.load(file)
        self.assertFalse(isVideoEnglish(whitespaceDescription))

    def test_parse_language_dot_description(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as file:
            basic_data = json.load(file)
        basic_data["snippet"]["description"] = '.'
        self.assertFalse(isVideoEnglish(basic_data))

    def test_parse_language_for_english(self):
        with open('src//tests//resources//basicEnglish.json', 'r', encoding='utf-8') as file:
            basicEnglish = json.load(file)
        self.assertTrue(isVideoEnglish(basicEnglish))

    def test_processing(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as file:
            basic_data = json.load(file)
        process([basic_data], "Minecraft", set())

    def test_processing_without_snipped(self):
        with open('src//tests//resources//unavaiableItem.json', 'r', encoding='utf-8') as file:
            unavaiableItem = json.load(file)
        process([unavaiableItem], "Valorant", set())


if __name__ == '__main__':
    unittest.main()
