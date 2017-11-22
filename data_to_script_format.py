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
        elif 'story' in post and post['type'] in ('photo', 'status', 'video'):
            message = post['story']
        elif 'attachments' in post and len(post['attachments']['data']) > 0 and \
                        post['attachments']['data'][0]['type'] == 'unavailable':
            continue
        elif 'attachments' in post and len(post['attachments']['data']) > 0 and \
                        post['attachments']['data'][0]['type'] == 'animated_image_share':
            message = 'some gif is shared'
        else:
            print(post)
            # raise Exception('Weird post')

        created_time = fb_to_datetime(post['created_time']).strftime('%d.%m.%Y %H:%M:%S')
        lines.append('{}: {}: {}\n'.format(post['from']['name'], created_time, message))
        for comment in sorted(post['comments'], key=lambda p: fb_to_datetime(p['created_time']).timestamp()):
            created_time = fb_to_datetime(comment['created_time']).strftime('%d.%m.%Y %H:%M:%S')
            lines.append('{}: {}: {}\n'.format(comment['from']['name'], created_time, comment['message']))
        lines.append(('-' * 15) + '\n')
    with open(os.path.join(get_binary_dir(group_name), file_name), 'w+', encoding='utf-8') as file:
        file.writelines(lines)


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

    print("Data transformed to script format")
