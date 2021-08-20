from langdetect import detect, LangDetectException

TITLE_TOO_SHORT = 15
DESCRIPTION_TO_SHORT = 30
DESCRIPTION_LONG = 100


def isVideoInCorrectLanguage(languages, videoJSON) -> bool:
    if len(videoJSON["snippet"]["description"]) > DESCRIPTION_TO_SHORT:
        try:
            fromDescription = detect(videoJSON["snippet"]["description"])
            if len(videoJSON["snippet"]["description"]) > DESCRIPTION_LONG or len(videoJSON["snippet"]["title"]) < TITLE_TOO_SHORT:
                return fromDescription in languages
            else:
                return fromDescription in languages and detect(videoJSON["snippet"]["title"]) in languages
        except LangDetectException:
            pass
    if len(videoJSON["snippet"]["title"]) > TITLE_TOO_SHORT:
        try:
            return detect(videoJSON["snippet"]["title"]) in languages
        except LangDetectException:
            pass

    return False
