import sys
import json
import unittest
from src.models.Video import fromJson
from src.process.language.LanguageProcessor import isVideoEnglish
from src.process.VideoProcess import process
# with open('resources//basicItem.json', 'r', encoding='utf-8') as f:
#     basic_data = json.load(f, ensure_ascii=False, indent=4)
# advanced_data = json.loads("resources//noGameTitleItem.json")
# basic_data = json.loads("resources//basicItem.json")


class TestJsonParsing(unittest.TestCase):
    def test_parse_basic(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as f:
            basic_data = json.load(f)
        result = fromJson(basic_data)

    def test_parse_with_none_title(self):
        with open('src//tests//resources//noGameTitleItem.json', 'r', encoding='utf-8') as f:
            advanced_data = json.load(f)
        result = fromJson(advanced_data)

    def test_parse_language(self):
        with open('src//tests//resources//noGameTitleItem.json', 'r', encoding='utf-8') as f:
            advanced_data = json.load(f)
        self.assertFalse(isVideoEnglish(advanced_data))

    def test_parse_language_empty_description(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as f:
            basic_data = json.load(f)
        basic_data["snippet"]["description"] = ''
        isVideoEnglish(basic_data)

    def test_parse_language_whitespace_description(self):
        with open('src//tests//resources//whitespaceDescription.json', 'r', encoding='utf-8') as f:
            whitespaceDescription = json.load(f)
        isVideoEnglish(whitespaceDescription)

    def test_parse_language_dot_description(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as f:
            basic_data = json.load(f)
        basic_data["snippet"]["description"] = '.'
        isVideoEnglish(basic_data)

    def test_parse_language_for_english(self):
        with open('src//tests//resources//basicEnglish.json', 'r', encoding='utf-8') as f:
            basicEnglish = json.load(f)
        self.assertTrue(isVideoEnglish(basicEnglish))

    def test_processing(self):
        with open('src//tests//resources//basicItem.json', 'r', encoding='utf-8') as f:
            basic_data = json.load(f)
        process([basic_data], "Minecraft", set())

    def test_processing_without_snipped(self):
        with open('src//tests//resources//unavaiableItem.json', 'r', encoding='utf-8') as f:
            unavaiableItem = json.load(f)
        process([unavaiableItem], "Valorant", set())


if __name__ == '__main__':
    unittest.main()
