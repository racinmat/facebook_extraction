import json
import pickle
import calendar
from datetime import datetime

import os
from dateutil.relativedelta import relativedelta
from facepy import GraphAPI
from enum import Enum


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
    print(month, ': ', len(posts))
    return posts


def download_comments_month(group_name, month):
    with open(get_posts_file(group_name, month), 'r', encoding='utf-8') as file:
        posts = json.load(file)
    post_ids = [i['id'] for i in posts]
    comments = []
    for post_id in post_ids:
        comments += download_comments_for_post(post_id)
    print(month, ': ', len(comments))
    return comments


def download_comments_for_post(post_id):
    limit = 1200
    fields = ['message', 'created_time', 'from', 'id', 'attachment', 'object', 'parent', 'message_tags'
              ]
    data = graph.get(post_id + "/comments?filter=stream&summary=1&fields=" + ','.join(fields), page=False, retry=5, limit=limit)
    comments = data['data']
    print(post_id, ': ', len(comments))
    return comments


def save_data_month(data, group_name, month, type):
    if type == Type.POST:
        directory = get_posts_dir(group_name)
        file = get_posts_file(group_name, month)
    elif type == Type.COMMENT:
        directory = get_comments_dir(group_name)
        file = get_comments_file(group_name, month)
    else:
        raise Exception('unknown type')
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file, 'w+', encoding='utf-8') as file:
        json.dump(data, file)


def save_posts_month(posts, group_name, month):
    save_data_month(posts, group_name, month, Type.POST)


def save_comments_month(comments, group_name, month):
    save_data_month(comments, group_name, month, Type.COMMENT)


def download_group_posts(group_name, group_id):
    month = get_last_processed_month_posts(group_name).get_previous_month()
    while month >= treshold:
        posts = download_posts_month(group_id, month)
        save_posts_month(posts, group_name, month)
        month = get_last_processed_month_posts(group_name).get_previous_month()


def download_group_comments(group_name, group_id):
    month = get_last_processed_month_comments(group_name).get_previous_month()
    while month >= treshold:
        comments = download_comments_month(group_name, month)
        save_comments_month(comments, group_name, month)
        month = get_last_processed_month_comments(group_name).get_previous_month()


def get_posts_dir(group_name):
    return 'texts/{}/posts'.format(group_name)


def get_comments_dir(group_name):
    return 'texts/{}/comments'.format(group_name)


def get_posts_file(group_name, month):
    return os.path.join(get_posts_dir(group_name), '{}.json'.format(month))


def get_comments_file(group_name, month):
    return os.path.join(get_comments_dir(group_name), '{}.json'.format(month))


def get_last_processed_month_posts(group_name):
    directory = get_posts_dir(group_name)
    earliest = Month().get_next_month()
    if not os.path.exists(directory):
        os.makedirs(directory)
    for string in os.listdir(directory):
        month = Month.from_str(string.replace('.json', ''))
        if month <= earliest:
            earliest = month
    return earliest


def get_last_processed_month_comments(group_name):
    directory = get_comments_dir(group_name)
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
        download_group_comments(group_name, group_id)


if __name__ == '__main__':
    with open('credentials.json') as file:
        credentials = json.load(file)
        access_token = credentials['extended_access_token']

    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    treshold = Month(year=2017, month=11)

    graph = GraphAPI(access_token)
    main()