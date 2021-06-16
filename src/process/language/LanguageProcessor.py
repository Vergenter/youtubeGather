from langdetect import detect, LangDetectException

ENGLISH = "en"
TITLE_TOO_SHORT = 15
DESCRIPTION_TO_SHORT = 30
DESCRIPTION_LONG = 100


def isVideoEnglish(videoJSON) -> bool:
    if len(videoJSON["snippet"]["description"]) > DESCRIPTION_TO_SHORT:
        try:
            fromDescription = detect(videoJSON["snippet"]["description"])
            if len(videoJSON["snippet"]["description"]) > DESCRIPTION_LONG or len(videoJSON["snippet"]["title"]) < TITLE_TOO_SHORT:
                return fromDescription == ENGLISH
            else:
                return fromDescription == ENGLISH and detect(videoJSON["snippet"]["title"]) == ENGLISH
        except LangDetectException:
            pass
    if len(videoJSON["snippet"]["title"]) > TITLE_TOO_SHORT:
        try:
            return detect(videoJSON["snippet"]["title"]) == ENGLISH
        except LangDetectException:
            pass

    return False
