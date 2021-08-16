from videos.videos_config import VideosModuleConfig


def main(data: VideosModuleConfig):
    # channel ids for channels videos baching -> channel videos ids
    ## check if input channels should be updated -> new? channel ids
    ## read channels videos data from database to update videos -> channel ids

    # video ids for update batching -> video state
    ## read videos data from database to update -> videos ids
    ## videos ids for need to be updated batching -> video ids
    ### input new? videos ids 
    ### channel new? videos ids 

    # video state -> add new videos update date to own database

    # new? videos ids to -> new videos ids
    ## input new? videos ids 
    ## channel new? videos ids 

    # new videos ids send through kafka

    # new videos ids + video state => add to graph database new static video data
    # !!! possible simplify connecting new and old

    # video state => add to graph database new dynamic video data

    # send through kafka new channel ids
    ## new videos ids query to db -> new channels ids

    print(data)
