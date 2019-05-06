import json

import time
from facepy import GraphAPI, OAuthError
import utils
from utils import download_group_posts, download_group_comments, download_group_reactions, Month, is_limit_reached


def download():
    for group_name, group_id in groups.items():
        # download_group_posts(group_name, group_id)    # todo: uncomment
        download_group_comments(group_name)
        download_group_reactions(group_name)


def main():
    while True:
        try:
            download()
        except OAuthError as e:
            print("Limit reached")
            print(e)
            while is_limit_reached():
                print("Limit is reached, waiting")
                time.sleep(60 * 10)  # 10 minutes waiting
            print("wait ended")


#
# def performance_tests():
#     start = time.time()
#     usual = download_comments_month('scitani_ceskych_a_slovenskych_otaku', Month(2017, 11))
#     end = time.time()
#     print('time in usual for loop: ', end - start)
#     start = time.time()
#     parallel = download_comments_month_parallel('scitani_ceskych_a_slovenskych_otaku', Month(2017, 11))
#     end = time.time()
#     print('time in parallel for loop: ', end - start)
#     pass


if __name__ == '__main__':
    with open('credentials.json') as config_file:
        credentials = json.load(config_file)
        access_token = credentials['extended_access_token']

    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720',
        # 'test_matrika': '465119610230277',
    }

    utils.texts_root = 'texts'

    utils.treshold = Month(year=2008, month=1)
    # utils.treshold = Month(year=2013, month=1)
    # utils.treshold = Month(year=2017, month=11)
    utils.posts_limit = 100  # paginated, so 100 is limit per one page
    utils.objects_limit = 600
    utils.retries = 10

    utils.graph = GraphAPI(access_token)
    main()
    # performance_tests()
    print("Everything downloaded to month {}".format(utils.treshold))
