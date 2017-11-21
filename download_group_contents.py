import json
import pickle
import calendar
from datetime import datetime

import os
from dateutil.relativedelta import relativedelta
from facepy import GraphAPI
from enum import Enum

from progressbar import ProgressBar, Percentage, Bar


class Type(Enum):
    POST = 1
    COMMENT = 2
    REACTION = 3


class Month:
    def __init__(self, year=None, month=None):
        super().__init__()
        now = datetime.now()
        if year is None:
            year = now.year
        if month is None:
            month = now.month
        self.year = year
        self.month = month
        date = datetime(year=year, month=month, day=1, hour=0, minute=0, second=0)
        date.replace(year=self.year, month=self.month)
        self.date = date

    def get_next_month(self):
        next = self.date + relativedelta(months=+1)
        next_month = Month(next.year, next.month)
        return next_month

    def get_previous_month(self):
        previous = self.date + relativedelta(months=-1)
        previous_month = Month(previous.year, previous.month)
        return previous_month

    def get_since(self):
        return int(self.date.timestamp())

    def get_until(self):
        last = self.date + relativedelta(months=+1, seconds=-1)
        return int(last.timestamp())

    def timestamp(self):
        return int(self.date.timestamp())

    @staticmethod
    def from_str(string):
        year, month = string.split('-')
        return Month(int(year), int(month))

    def __str__(self):
        return "{}-{}".format(self.year, self.month)

    def __lt__(self, other):
        return self.timestamp() < other.timestamp()

    def __gt__(self, other):
        return self.timestamp() > other.timestamp()

    def __le__(self, other):
        return self.timestamp() <= other.timestamp()

    def __ge__(self, other):
        return self.timestamp() >= other.timestamp()

    def __eq__(self, other):
        return self.timestamp() == other.timestamp()


def download_posts_month(group_id, month):
    since = month.get_since()
    until = month.get_until()
    limit = 1200
    fields = ['message', 'message_tags', 'created_time', 'updated_time', 'caption', 'description', 'story', 'from',
              'icon', 'properties', 'shares', 'link', 'name', 'object_id', 'parent_id', 'permalink_url', 'source',
              'status_type', 'target', 'type', 'to', 'with_tags'
              ]
    data = graph.get(group_id + "/feed?fields=" + ','.join(fields), page=False, retry=5, since=since, until=until, limit=limit)
    posts = data['data']
    if len(posts) == limit:
        print({'{} has limit posts, need to paginate'.format(month)})
    # print(month, ': ', len(posts))
    return posts


def download_comments_month(group_name, month):
    with open(get_file(group_name, month, Type.POST), 'r', encoding='utf-8') as file:
        posts = json.load(file)
    post_ids = [i['id'] for i in posts]
    comments = []
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(post_ids)).start()
    for i, post_id in enumerate(post_ids):
        comments += download_comments_for_post(post_id)
        pbar.update(i+1)
    pbar.finish()
    # print(month, ': ', len(comments))
    return comments


def download_reactions_month(group_name, month):
    with open(get_file(group_name, month, Type.POST), 'r', encoding='utf-8') as file:
        posts = json.load(file)
    post_ids = [i['id'] for i in posts]
    with open(get_file(group_name, month, Type.COMMENT), 'r', encoding='utf-8') as file:
        comments = json.load(file)
    comment_ids = [i['id'] for i in comments]
    object_ids = post_ids + comment_ids
    reactions = []
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=len(object_ids)).start()
    for i, object_id in enumerate(object_ids):
        reactions += download_reactions_for_object(object_id)
        pbar.update(i+1)
    pbar.finish()
    return reactions


def download_comments_for_post(post_id):
    limit = 1200
    fields = ['message', 'created_time', 'from', 'id', 'attachment', 'object', 'parent', 'message_tags']
    data = graph.get(post_id + "/comments?filter=stream&summary=1&fields=" + ','.join(fields), page=False,
                     retry=5, limit=limit)
    comments = data['data']
    # print(post_id, ': ', len(comments))
    return comments


def download_reactions_for_object(object_id):
    limit = 1200
    # fields = ['id', 'name', 'type', 'profile_type']
    fields = ['id', 'name', 'type']
    data = graph.get(object_id + "/reactions?fields=" + ','.join(fields), page=False, retry=5, limit=limit)
    reactions = data['data']
    for reaction in reactions:
        reaction['object_id'] = object_id
    # print(object_id, ': ', len(reactions))
    return reactions


def save_data_month(data, group_name, month, type):
    directory = get_dir(group_name, type)
    file_name = get_file(group_name, month, type)

    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_name, 'w+', encoding='utf-8') as file:
        json.dump(data, file)


def download_group_posts(group_name, group_id):
    month = get_last_processed_month(group_name, Type.POST).get_previous_month()
    while month >= treshold:
        print("processing posts in month {}".format(month))
        posts = download_posts_month(group_id, month)
        save_data_month(posts, group_name, month, Type.POST)
        month = get_last_processed_month(group_name, Type.POST).get_previous_month()


def download_group_comments(group_name):
    month = get_last_processed_month(group_name, Type.COMMENT).get_previous_month()
    while month >= treshold:
        print("processing comments in month {}".format(month))
        comments = download_comments_month(group_name, month)
        save_data_month(comments, group_name, month, Type.COMMENT)
        month = get_last_processed_month(group_name, Type.COMMENT).get_previous_month()


def download_group_reactions(group_name):
    month = get_last_processed_month(group_name, Type.REACTION).get_previous_month()
    while month >= treshold:
        print("processing reactions in month {}".format(month))
        reactions = download_reactions_month(group_name, month)
        save_data_month(reactions, group_name, month, Type.REACTION)
        month = get_last_processed_month(group_name, Type.REACTION).get_previous_month()


def get_dir(group_name, type):
    if type == Type.POST:
        directory = 'texts/{}/posts'.format(group_name)
    elif type == Type.COMMENT:
        directory = 'texts/{}/comments'.format(group_name)
    elif type == Type.REACTION:
        directory = 'texts/{}/reactions'.format(group_name)
    else:
        raise Exception('unknown type')
    return directory


def get_file(group_name, month, type):
    return os.path.join(get_dir(group_name, type), '{}.json'.format(month))


def get_last_processed_month(group_name, type):
    directory = get_dir(group_name, type)
    earliest = Month().get_next_month()

    if not os.path.exists(directory):
        os.makedirs(directory)
    for string in os.listdir(directory):
        month = Month.from_str(string.replace('.json', ''))
        if month <= earliest:
            earliest = month
    return earliest


def main():
    for group_name, group_id in groups.items():
        download_group_posts(group_name, group_id)
        download_group_comments(group_name)
        download_group_reactions(group_name)


if __name__ == '__main__':
    with open('credentials.json') as config_file:
        credentials = json.load(config_file)
        access_token = credentials['extended_access_token']

    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    treshold = Month(year=2017, month=11)

    graph = GraphAPI(access_token)
    main()
