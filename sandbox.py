import json
from datetime import datetime

from facepy import GraphAPI
import utils
from utils import download_group_posts, download_group_comments, download_group_reactions, Month


def main():
    group_name = list(groups.keys())[0]
    group_id = list(groups.values())[0]
    since = datetime(year=2017, month=11, day=5).timestamp()
    until = datetime(year=2017, month=11, day=22).timestamp()
    posts = download_posts(group_id, since, until, utils.graph, 1000, retries=10)


def download_posts(group_id, since, until, graph, limits, retries):
    '''
    :param group_id:
    :param month:
    :param facepy.GraphAPI graph:
    :param limits:
    :param retries:
    :return:
    '''
    fields = ['message', 'message_tags', 'created_time', 'updated_time', 'caption', 'description', 'story', 'from',
              'icon', 'properties', 'shares', 'link', 'name', 'object_id', 'parent_id', 'permalink_url', 'source',
              'status_type', 'target', 'type', 'to', 'with_tags', 'attachments'
              ]
    # data = graph.get(group_id + "/feed?fields=" + ','.join(fields), page=False, retry=retries, since=since, until=until,
    #                  limit=limits)
    data = graph.get(group_id + "/feed", page=False, fields=fields, retry=retries, since=since, until=until,
                     limit=limits)
    posts = data['data']
    return posts


if __name__ == '__main__':
    with open('credentials.json') as config_file:
        credentials = json.load(config_file)
        access_token = credentials['extended_access_token']

    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    utils.texts_root = 'texts'

    # utils.treshold = Month(year=2005, month=1)
    utils.treshold = Month(year=2017, month=11)
    utils.posts_limit = 1500
    utils.objects_limit = 600
    utils.retries = 10

    utils.graph = GraphAPI(access_token)
    main()