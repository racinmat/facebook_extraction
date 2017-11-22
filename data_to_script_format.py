import os
import utils
from utils import load_binary_data, get_binary_dir, fb_to_datetime


def transform_to_script(group_name, data, file_name):
    lines = []

    for id, post in data.items():
        message = None
        if 'message' in post:
            message = post['message']
        elif 'description' in post and 'link' in post:
            message = '{}, link: {}'.format(post['description'], post['link'])
        elif 'status_type' in post and post['status_type'] == 'added_photos':
            message = 'added photos'
        else:
            print(post)
            raise Exception('Weird post')

        created_time = fb_to_datetime(post['created_time']).strftime('%d.%m.%Y %H:%M:%S')
        lines.append('{}: {}: {}'.format(post['from']['name'], created_time, message))
        for comment in post['comments']:
            created_time = fb_to_datetime(comment['created_time']).strftime('%d.%m.%Y %H:%M:%S')
            lines.append('{}: {}: {}'.format(comment['from']['name'], created_time, comment['message']))
        lines.append('-' * 15)
    with open(os.path.join(get_binary_dir(group_name), file_name), 'w+') as file:
        file.writelines(lines=lines)


def main():
    for group_name, group_id in groups.items():
        data = load_binary_data(group_name)
        transform_to_script(group_name, data, 'scripts.txt')


if __name__ == '__main__':
    utils.texts_root = 'texts_test'
    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    main()

    print("Everything denormalized")
