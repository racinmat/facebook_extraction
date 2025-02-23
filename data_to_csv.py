import json
import os
import utils
from utils import load_binary_data, get_binary_dir, fb_to_datetime
import os.path as osp
import pandas as pd


def json_posts_to_pandas(json_file):
    with open(json_file) as fp:
        data = json.load(fp)

    flat_posts = []

    for post in data:
        # some data preliminary cleaning
        # I don't care about commerce products
        if 'attachments' in post:
            post['attachments']['data'] = [i for i in post['attachments']['data'] if
                                           i['type'] != 'commerce_product_mini_list']
        try:
            # should be only 1 attachment
            attachment = post['attachments']['data'][0] if 'attachments' in post else None
            if 'from' in post:
                # for old posts, which have from
                flat_post = {
                    'id': post['id'],
                    'from_id': post['from']['id'],
                    'from_name': post['from']['name'],
                    'object_id': post.get('object_id', None),
                    'status_type': post.get('status_type', None),  # getter with defaulting if key is not present
                    'type': post['type'],
                    'created_time': post['created_time'],
                    'updated_time': post['updated_time'],
                    'message': post.get('message', None),
                    'attachment_type': attachment.get('type', None) if attachment is not None else None,
                    'attachment_title': attachment.get('title', None) if attachment is not None else None,
                    'attachment_url': attachment.get('url', None) if attachment is not None else None,
                    'shares_count': post['shares']['count'] if 'shares' in post else 0,
                }
            else:
                # for new posts, which don't have from
                flat_post = {
                    'id': post['id'],
                    'from_id': None,
                    'from_name': None,
                    'object_id': post.get('object_id', None),
                    'status_type': post.get('status_type', None),  # getter with defaulting if key is not present
                    'type': post['type'],
                    'created_time': post['created_time'],
                    'updated_time': post['updated_time'],
                    'message': post.get('message', None),
                    'attachment_type': attachment.get('type', None) if attachment is not None else None,
                    'attachment_title': attachment.get('title', None) if attachment is not None else None,
                    'attachment_url': attachment.get('url', None) if attachment is not None else None,
                    'shares_count': post['shares']['count'] if 'shares' in post else 0,
                }
            if 'attachments' in post and len(post['attachments']['data']) > 1:
                # print('much attachments')
                raise Exception('much attachments')
            flat_posts.append(flat_post)
        except KeyError as e:
            print('hi')

    df = pd.DataFrame(flat_posts)
    return df


def json_comments_to_pandas(json_file):
    with open(json_file) as fp:
        data = json.load(fp)

    flat_comments = []

    for comment in data:
        # some data preliminary cleaning
        # I don't care about commerce products
        try:
            if 'from' in comment:
                # for old posts, which have from
                flat_comment = {
                    'id': comment['id'],
                    'message': comment['message'],
                    'from_id': comment['from']['id'],
                    'from_name': comment['from']['name'],
                    'created_time': comment['created_time'],
                    'parent_id': comment['parent']['id'] if 'parent' in comment else None,
                    'object_id': comment['object']['id'],
                }
            else:
                # for new posts, which don't have from
                flat_comment = {
                    'id': comment['id'],
                    'message': comment['message'],
                    'from_id': None,
                    'from_name': None,
                    'created_time': comment['created_time'],
                    'parent_id': comment['parent']['id'] if 'parent' in comment else None,
                    'object_id': comment['object']['id'],
                }
            flat_comments.append(flat_comment)
        except KeyError as e:
            print('hi')
            print(e)

    df = pd.DataFrame(flat_comments)
    return df


def json_reactions_to_pandas(json_file):
    with open(json_file) as fp:
        data = json.load(fp)

    flat_reactions = []

    fields = set()
    for reaction in data:
        fields = fields.union(reaction.keys())
        # some data preliminary cleaning
        # I don't care about commerce products
        try:
            flat_reaction = {
                'id': reaction['id'],
                'name': reaction['name'],
                'object_id': reaction['object_id'],
                'type': reaction['type'],
            }
            flat_reactions.append(flat_reaction)
        except KeyError as e:
            print('hi')
            print(e)

    df = pd.DataFrame(flat_reactions)
    return df


def data_to_pandas(group_name):
    directory = utils.get_dir(utils.texts_root, group_name, utils.Type.POST)
    existing_months = sorted(set([utils.Month.from_str(s.replace('.json', '')) for s in os.listdir(directory)]))
    for month in existing_months:
        df = json_posts_to_pandas(osp.join(directory, f'{month}.json'))
        # df.to_csv(json_file + '.csv')

    directory = utils.get_dir(utils.texts_root, group_name, utils.Type.COMMENT)
    existing_months = sorted(set([utils.Month.from_str(s.replace('.json', '')) for s in os.listdir(directory)]))
    for month in existing_months:
        df = json_comments_to_pandas(osp.join(directory, f'{month}.json'))

    # # todo: implement the rest
    # directory = utils.get_dir(utils.texts_root, group_name, utils.Type.REACTION)
    # existing_months = sorted(set([utils.Month.from_str(s.replace('.json', '')) for s in os.listdir(directory)]))
    # for month in existing_months:
    #     df = json_reactions_to_pandas(osp.join(directory, f'{month}.json'))


def main():
    for group_name, group_id in groups.items():
        data_to_pandas(group_name)


if __name__ == '__main__':
    # utils.texts_root = 'texts_test'
    utils.texts_root = 'texts'
    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720',
        # 'scitani_ceskych_a_slovenskych_otaku_old': '135384786514720',
    }

    main()

    print("Data transformed to script format")
