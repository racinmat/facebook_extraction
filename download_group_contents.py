import json
import pickle
import calendar
from datetime import datetime

import os
from dateutil.relativedelta import relativedelta
from facepy import GraphAPI


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


def download_posts_month(group_id, month):
    since = month.get_since()
    until = month.get_until()
    limit = 1000
    data = graph.get(group_id + "/feed", page=False, retry=3, since=since, until=until, limit=limit)
    posts = data['data']
    if len(posts) == limit:
        print({'{} has limit posts, need to paginate'.format(month)})
    print(month, ': ', len(posts))
    return posts


def save_posts_month(posts, group_name, month):
    directory = get_posts_dir(group_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(os.path.join(directory, '{}.json'.format(month)), 'w+') as file:
        json.dump(posts, file)
    # pickle.dumps(posts, open('texts/{}/{}.txt'.format(group_name, month), 'wb+'))


def download_group_posts(group_name, group_id):
    treshold = Month(year=2005, month=1)
    month = get_last_processed_month_posts(group_name).get_previous_month()
    while month > treshold:
        posts = download_posts_month(group_id, month)
        save_posts_month(posts, group_name, month)
        month = get_last_processed_month_posts(group_name).get_previous_month()


def get_posts_dir(group_name):
    return 'texts/{}/posts'.format(group_name)


def get_last_processed_month_posts(group_name):
    directory = get_posts_dir(group_name)
    earliest = Month()
    for string in os.listdir(directory):
        month = Month.from_str(string.replace('.json', ''))
        if month < earliest:
            earliest = month
    return earliest


if __name__ == '__main__':
    with open('credentials.json') as file:
        credentials = json.load(file)
        access_token = credentials['extended_access_token']

    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    graph = GraphAPI(access_token)

    for group_name, group_id in groups.items():
        download_group_posts(group_name, group_id)
