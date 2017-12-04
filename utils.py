import json
from datetime import datetime
import os
from itertools import chain
import pickle
from dateutil.relativedelta import relativedelta
from enum import Enum
from facepy import FacebookError
from joblib import Parallel, delayed
import time
from facepy import OAuthError
import logging
import sys


def is_limit_reached():
    try:
        graph.get('/me', retry=1)
        return False
    except OAuthError as e:
        return True


def wait_if_limit_reached(funct):
    def try_and_wait(*args, **kw):
        while True:
            try:
                result = funct(*args, **kw)
                return result
            except OAuthError as e:
                logger.info("Limit reached while calling {}".format(funct.__name__))
                logger.info(e)
                while is_limit_reached():
                    logger.info("Limit is reached, waiting")
                    time.sleep(600)  # one minute waiting
                logger.info("wait ended, trying if can query API")

    return try_and_wait


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

    @staticmethod
    def between(begin, end):
        months = {begin}
        if begin == end:
            return months

        iter = begin.get_next_month()
        while iter != end:
            months.add(iter)
            iter = iter.get_next_month()
        months.add(end)
        return months

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

    def __ne__(self, other):
        return self.timestamp() != other.timestamp()

    def __hash__(self):
        return hash(str(self))


@wait_if_limit_reached
def download_posts_month(group_id, month, graph, limits, retries):
    '''
    :param group_id:
    :param month:
    :param facepy.GraphAPI graph:
    :param limits:
    :param retries:
    :return:
    '''
    since = month.get_since()
    until = month.get_until()
    fields = ['message', 'message_tags', 'created_time', 'updated_time', 'caption', 'description', 'story', 'from',
              'icon', 'properties', 'shares', 'link', 'name', 'object_id', 'parent_id', 'permalink_url', 'source',
              'status_type', 'target', 'type', 'to', 'with_tags', 'attachments'
              ]
    # data = graph.get(group_id + "/feed?fields=" + ','.join(fields), page=False, retry=retries, since=since, until=until,
    #                  limit=limits)
    # data = graph.get(group_id + "/feed", page=False, fields=fields, retry=retries, since=since, until=until,
    #                  limit=limits)
    # posts = data['data']
    # if len(posts) == limits:
    #     logger.info({'{} has limit posts, need to paginate'.format(month)})
    pages = graph.get(group_id + "/feed", page=True, fields=fields, retry=retries, since=since, until=until,
                      limit=limits)
    posts = []
    for page in pages:
        # logger.info("page len: {}".format(len(page['data'])))
        posts += page['data']

    logger.info(month, ': ', len(posts))
    return posts


@wait_if_limit_reached
def download_comments_for_post(post_id, graph, limits, retries):
    fields = ['message', 'created_time', 'from', 'id', 'attachment', 'object', 'parent', 'message_tags']
    data = graph.get(post_id + "/comments", page=False,
                     retry=retries, limit=limits, summary=1, filter='stream', fields=fields)
    comments = data['data']
    # logger.info(post_id, ': ', len(comments))
    return comments


@wait_if_limit_reached
def download_reactions_for_object(object_id, graph, limits, retries):
    if graph is None:
        raise Exception('Forgot to initialize graph')

    # fields = ['id', 'name', 'type', 'profile_type']
    fields = ['id', 'name', 'type']
    try:
        data = graph.get(object_id + "/reactions", page=False, retry=retries, fields=fields, limit=limits)
        reactions = data['data']
        for reaction in reactions:
            reaction['object_id'] = object_id
        return reactions
    except FacebookError as e:
        if 'does not exist' in e.message:
            with open('errors.txt', 'a+') as f:
                f.write('Post {} does not exist.\n'.format(object_id))
        else:
            raise e
    # logger.info(object_id, ': ', len(reactions))
    return []


def download_comments_month(group_name, month):
    with open(get_file(texts_root, group_name, month, Type.POST), 'r', encoding='utf-8') as file:
        posts = json.load(file)
    post_ids = [i['id'] for i in posts]
    # return download_comments_usual(post_ids)
    return download_comments_parallel(post_ids)


def download_comments_usual(post_ids):
    combined = [download_comments_for_post(post_id, graph, objects_limit, retries) for post_id in post_ids]
    comments = list(chain.from_iterable(combined))
    # logger.info(month, ': ', len(comments))
    return comments


def download_comments_parallel(post_ids):
    combined = Parallel(n_jobs=workers)(delayed(download_comments_for_post)
                                        (post_id, graph, objects_limit, retries) for post_id in post_ids)
    comments = list(chain.from_iterable(combined))
    # logger.info(month, ': ', len(comments))
    return comments


def download_reactions_month(group_name, month):
    with open(get_file(texts_root, group_name, month, Type.POST), 'r', encoding='utf-8') as file:
        posts = json.load(file)
    post_ids = [i['id'] for i in posts]
    with open(get_file(texts_root, group_name, month, Type.COMMENT), 'r', encoding='utf-8') as file:
        comments = json.load(file)
    comment_ids = [i['id'] for i in comments]
    object_ids = post_ids + comment_ids
    return download_reactions_usual(object_ids)
    # since there is so much reactions, parallel mode is not needed
    # return download_reactions_parallel(object_ids)


def download_reactions_usual(object_ids):
    combined = [download_reactions_for_object(object_id, graph, objects_limit, retries) for object_id in object_ids]
    reactions = list(chain.from_iterable(combined))
    return reactions


def download_reactions_parallel(object_ids):
    combined = Parallel(n_jobs=workers)(delayed(download_reactions_for_object)
                                        (object_id, graph, objects_limit, retries) for object_id in object_ids)
    reactions = list(chain.from_iterable(combined))
    return reactions


def fb_to_datetime(string):
    return datetime.strptime(string, '%Y-%m-%dT%H:%M:%S%z')


def save_data_month(data, group_name, month, type):
    directory = get_dir(texts_root, group_name, type)
    file_name = get_file(texts_root, group_name, month, type)

    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_name, 'w+', encoding='utf-8') as file:
        json.dump(data, file)


def download_group_posts(group_name, group_id):
    for month in get_missing_months(group_name, Type.POST):
        logger.info("processing posts in month {}".format(month))
        posts = download_posts_month(group_id, month, graph, posts_limit, retries)
        save_data_month(posts, group_name, month, Type.POST)


def download_group_comments(group_name):
    if treshold is None:
        raise Exception('Forgot to initialize treshold')

    for month in get_missing_months(group_name, Type.COMMENT):
        logger.info("processing comments in month {}".format(month))
        comments = download_comments_month(group_name, month)
        save_data_month(comments, group_name, month, Type.COMMENT)


def download_group_reactions(group_name):
    if treshold is None:
        raise Exception('Forgot to initialize treshold')

    for month in get_missing_months(group_name, Type.REACTION):
        logger.info("processing reactions in month {}".format(month))
        reactions = download_reactions_month(group_name, month)
        save_data_month(reactions, group_name, month, Type.REACTION)


def get_dir(texts_root, group_name, type):
    if type == Type.POST:
        directory = os.path.join(texts_root, group_name, 'posts')
    elif type == Type.COMMENT:
        directory = os.path.join(texts_root, group_name, 'comments')
    elif type == Type.REACTION:
        directory = os.path.join(texts_root, group_name, 'reactions')
    else:
        raise Exception('unknown type')
    return directory


def get_file(texts_root, group_name, month, type):
    return os.path.join(get_dir(texts_root, group_name, type), '{}.json'.format(month))


def get_last_processed_month(group_name, type):
    directory = get_dir(texts_root, group_name, type)
    # earliest = Month().get_next_month() # not calling get_nexT_month skips current month, resulting in downloadin only complete months
    earliest = Month()

    if not os.path.exists(directory):
        os.makedirs(directory)

    for string in os.listdir(directory):
        month = Month.from_str(string.replace('.json', ''))
        if month <= earliest:
            earliest = month
    return earliest


def get_missing_months(group_name, type):
    directory = get_dir(texts_root, group_name, type)
    last = Month()
    # last = Month(2013, 12)

    if not os.path.exists(directory):
        os.makedirs(directory)

    existing_months = set([Month.from_str(s.replace('.json', '')) for s in os.listdir(directory)])
    all_months = Month.between(treshold, last)
    missing_months = list(all_months - existing_months)
    return list(reversed(sorted(missing_months)))


def load_data_month(texts_root, group_name, month, type):
    file_name = get_file(texts_root, group_name, month, type)

    if not os.path.exists(file_name):
        raise Exception('No data for group {}, month {} and type {}'.format(group_name, month, type))
    with open(file_name, 'r', encoding='utf-8') as file:
        return json.load(file)


def load_data(group_name, type):
    directory = get_dir(texts_root, group_name, type)
    if not os.path.exists(directory):
        raise Exception('No data for group {} and type {}'.format(group_name, type))

    # on goedel, this is 2x faster than parallel, what the fuck?
    data = []
    for string in os.listdir(directory):
        month = Month.from_str(string.replace('.json', ''))
        data += load_data_month(texts_root, group_name, month, type)

    # combined = Parallel(n_jobs=workers)(delayed(load_data_month)
    #                                     (texts_root, group_name, Month.from_str(string.replace('.json', '')), type)
    #                                     for string in os.listdir(directory))
    # data = list(chain.from_iterable(combined))
    return data


def strip_group_id(id):
    # from composite ids e.g. 135384786514720_1006346316085225, strip group id
    if '_' in id:
        return id.split('_')[1]
    return id


def unify_data_group(group_name):
    logger.info("loading data for group {}".format(group_name))
    start = time.time()

    posts = load_data(group_name, Type.POST)
    comments = load_data(group_name, Type.COMMENT)
    reactions = load_data(group_name, Type.REACTION)

    end = time.time()
    logger.info("done loading data, going to process them, loaded {}, {}, {} posts, comments and reactions"
          .format(len(posts), len(comments), len(reactions)))

    logger.info('time in usual for loop: ', end - start)

    posts = {strip_group_id(post['id']): post for post in posts}
    comments = {comment['id']: comment for comment in comments}
    for id, post in posts.items():
        post['id'] = strip_group_id(post['id'])
        post['comments'] = []
        post['reactions'] = []
    for id, comment in comments.items():
        comment['object']['id'] = strip_group_id(comment['object']['id'])
        comment['reactions'] = []

    for reaction in reactions:
        reaction['object_id'] = strip_group_id(reaction['object_id'])
        object_id = reaction['object_id']
        if object_id in posts:
            posts[object_id]['reactions'].append(reaction)
        elif object_id in comments:
            comments[object_id]['reactions'].append(reaction)

    for id, comment in comments.items():
        parent_id = comment['object']['id']
        if parent_id not in posts:
            logger.info("shit, post with id {} is missing".format(parent_id))
            continue
        posts[parent_id]['comments'].append(comment)

    logger.info("processed, going to pickle them. Pickle RICK!!!!!!")
    return posts


def get_binary_dir(group_name):
    return os.path.join(texts_root, group_name)


def get_binary_file(group_name):
    return os.path.join(get_binary_dir(group_name), 'data.bin')


def save_binary_data(group_name, data):
    directory = get_binary_dir(group_name)
    file_name = get_binary_file(group_name)

    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_name, 'wb+') as file:
        pickle.dump(data, file)


def load_binary_data(group_name):
    directory = get_binary_dir(group_name)
    file_name = get_binary_file(group_name)

    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_name, 'rb') as file:
        return pickle.load(file)


texts_root = None
treshold = None
graph = None
objects_limit = None
retries = None
posts_limit = None
workers = 10
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
