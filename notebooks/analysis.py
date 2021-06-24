# %%
import numpy as np
from datetime import date
import matplotlib.pyplot as plt
import os
import sys
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)
if True:  # prevent ordering
    from src.db.connection import Neo4jConnection
conn = Neo4jConnection()
# %%


def plot(ax, x, x_label, y, y_label, title):
    ax.plot(x, y)
    ax.set_title(title)
    ax.set(xlabel=x_label, ylabel=y_label)


def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'valid') / w


# %%
most_active_query = """
    match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(v:Video)--(c:Comment)--(commenter:Channel) 
    return commenter, count(c) as commentsCount, collect(c.textOriginal)
    ORDER BY commentsCount DESC
    LIMIT 10
"""
most_active_objective_query = """
    match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(v:Video)--(c:Comment)--(commenter:Channel) 
    return commenter, sum(c.polarity*(1-c.subjectivity)) as polarity, collect(c.textOriginal)
    ORDER BY polarity DESC
    LIMIT 10
"""
# %%
worst_polarity = """
    match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(v:Video)--(c:Comment)--(commenter:Channel) 
    return commenter, sum(c.polarity) as polarity, collect(c.textOriginal)
    ORDER BY polarity
    LIMIT 10
"""
# %%
best_polarity_query = """
    match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(v:Video)--(c:Comment)--(commenter:Channel) 
    return commenter, sum(c.polarity) as polarity, collect(c.textOriginal)
    ORDER BY polarity DESC
    LIMIT 10
"""
# %%
channels_polarity_query = """
    match (ch:Channel)--(:Video)--(c:Comment) 
    return ch, sum(c.polarity) as Allpolarity, avg(c.polarity) as Meanpolarity,count(c)
    ORDER BY Allpolarity DESC
    LIMIT 10
"""

# %%
dream_channel_analysis = """
    match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(v:Video)--(c:Comment)
    RETURN date(c.publishedAt) AS date,
        AVG(c.polarity) AS polarity,
        count(c) AS commentsCount
    ORDER BY date DESC
"""
results = list(conn.query(dream_channel_analysis))
x = [date(i[0].year, i[0].month, i[0].day) for i in results]
y_polarity = [i[1] for i in results]
y_counts = [i[2] for i in results]
fig, (ax1, ax2, ax3) = plt.subplots(3, figsize=(12, 10))
fig.suptitle('Channel statistics')
plot(ax1, x, "time", y_polarity, "polarity", "")
moving_average_step = 100
plot(ax2, x[moving_average_step-1:], "time",
     moving_average(y_polarity, moving_average_step), "polarity", "")
plot(ax3, x, "time", y_counts, "comments count", "")
fig.show()
# %%
dream_word_record_analysis = """
    match (:Video{videoId:"fj28UtF0-Fs"})--(c:Comment)
    RETURN date(c.publishedAt) AS date,
        AVG(c.polarity) AS polarity,
        count(c) AS commentsCount
    ORDER BY date
"""
results = list(conn.query(dream_word_record_analysis))
x = [date(i[0].year, i[0].month, i[0].day) for i in results]
y_polarity = [i[1] for i in results]
y_counts = [i[2] for i in results]
fig, (ax1, ax2) = plt.subplots(2, figsize=(12, 10))
fig.suptitle('Video statistics')
plot(ax1, x, "time", y_polarity, "polarity", "")
plot(ax2, x[3:], "time", y_counts[3:], "comments count", "")
fig.show()

# %%
conn.close()
