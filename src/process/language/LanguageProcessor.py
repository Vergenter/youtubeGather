from langdetect import detect

english = "en"


def isVideoEnglish(videoJSON):
    if len(videoJSON["snippet"]["description"].strip()) == 0:
        return detect(videoJSON["snippet"]["title"]) == english
    fromDescription = detect(videoJSON["snippet"]["description"])
    if len(videoJSON["snippet"]["description"]) > 100:
        return fromDescription == english
    fromTitle = detect(videoJSON["snippet"]["title"])
    return fromDescription == english and fromTitle == english
