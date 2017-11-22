import json
from datetime import datetime

from facepy import GraphAPI
import utils
from utils import download_group_posts, download_group_comments, download_group_reactions, Month


def main():
    group_name = list(groups.keys())[0]
    group_id = list(groups.values())[0]
    since = Month(2017, 10).get_since()
    until = Month(2017, 10).get_until()
    # until = datetime(year=2017, month=10, day=23).timestamp()
    # since = datetime(year=2017, month=10, day=1).timestamp()

    # posts = download_posts(group_id, since, until, utils.graph, 1000, retries=10)
    # with open('posts_test.json', 'w+', encoding='utf-8') as file:
    #     json.dump(posts, file)
    # with open('posts_test.json', 'r', encoding='utf-8') as file:
    #     posts_all = json.load(file)
    # print(len(posts_all))
    # posts = download_posts(group_id, since, until, utils.graph, 100, retries=10)
    with open('texts_test/{}/posts/2017-11.json'.format(group_name), 'r', encoding='utf-8') as file:
        posts_all = json.load(file)
    print(len(posts_all))


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
    # data = graph.get(group_id + "/feed", page=False, fields=fields, retry=retries, since=since, until=until,
    #                  limit=limits)
    # posts = data['data']
    pages = graph.get(group_id + "/feed", page=True, fields=fields, retry=retries, since=since, until=until,
                     limit=limits)
    posts = []
    for page in pages:
        print("page len: {}".format(len(page['data'])))
        posts += page['data']
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