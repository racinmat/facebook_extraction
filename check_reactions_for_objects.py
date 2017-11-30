import utils
from utils import *


def main():
    group_name = list(groups.keys())[0]
    print("loading data for group {}".format(group_name))

    month = Month(2017, 5)
    posts = load_data_month(utils.texts_root, group_name, month, Type.POST)
    comments = load_data_month(utils.texts_root, group_name, month, Type.COMMENT)
    reactions = load_data_month(utils.texts_root, group_name, month, Type.REACTION)

    post_ids = [strip_group_id(post['id']) for post in posts]
    comment_ids = [comment['id'] for comment in comments]
    object_ids = post_ids + comment_ids

    reactions_without_objects = []
    for reaction in reactions:
        if 'object_id' not in reaction:
            print(reaction)
            raise Exception('some shit happened')
        object_id = reaction['object_id']
        if object_id in object_ids:
            object_ids.remove(object_id)
        else:
            object_id = strip_group_id(reaction['object_id'])
            if object_id in object_ids:
                object_ids.remove(object_id)
            else:
                reactions_without_objects.append(object_ids)

    print('found objects for all reactions, remaining: {} objects, missing {} objects'.format(len(object_ids), len(reactions_without_objects)))


if __name__ == '__main__':
    # utils.texts_root = 'texts_test'
    utils.texts_root = 'texts'
    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    main()
