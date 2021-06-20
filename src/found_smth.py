
from textblob import TextBlob
from typing import cast
import datetime
import logging
import numpy as np
from operator import itemgetter
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_child_dc
from process.comment_process import process_command
from acquire.comment_api import query_by_comment_id
logging.basicConfig(filename='logs//comments_gather.log', level=logging.INFO)

dream_children_query = '''
match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})<--(v:Video)<--(c:Comment)
return c.commentId,c.textOriginal
'''

update_query = """
UNWIND $rows AS row
MATCH (c:Comment{commentId: row.commentId})
SET c.class = row.class
"""


def main():
    logging.info("Fetching comments started %s", datetime.datetime.now())
    conn = Neo4jConnection()
    children = conn.query(dream_children_query)
    result = [child for child in children]
    data = np.zeros(len(result), dtype=np.float64)
    for i in range(len(result)):
        test = TextBlob(result[i][1])
        data[i] = test.sentiment.polarity
        if i % 100:
            print(i//100, end="\r")
    rows = [{"commentId": result[i][0], "class":data[i]}
            for i in range(len(result))]
    conn.bulk_insert_data(
        update_query, rows)

    logging.info("Labeling comments succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()
