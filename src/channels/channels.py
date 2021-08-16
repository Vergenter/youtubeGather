from channels.channels_config import ChannelsModuleConfig


def main(data: ChannelsModuleConfig):
    # 1
    # fetch from main database channels
    # filter by own database
    # get data from youtube data api
    # insert channel static data
    # insert channel dynamic data
    # update own database

    # 2
    # get new channels from argument
    # get from own database to update
    # get data from youtube data api
    # insert new channels
    # insert new dynamic data
    # update own database

    # parrallel
    # read data from database to update
    # read channel_ids from kafka topic
    # if batch is big enough or times up
    # fetch videos by id
    # parse video to model
    # add to own db
    # send through kafka channel_ids
    # add to graph db

    print(data)
