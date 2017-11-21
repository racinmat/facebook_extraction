from datetime import datetime
import facebook
import pickle
import os

import re
import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dateutil.rrule import rrule, MONTHLY


def fb_to_datetime(string):
    return datetime.strptime(string, '%Y-%m-%dT%H:%M:%S%z')


def load_posts():
    user = 'me'

    graph = facebook.GraphAPI(access_token)
    # profile = graph.get_object(user)
    # posts = graph.get_connections(profile['id'], 'posts')
    # posts = graph.get_connections('135384786514720', 'feed')    # sčítání
    posts = graph.get_connections('295897323905870', 'feed')    # otaku seznamka

    all_posts = []
    limit = 10000
    while True:
        if len(all_posts) > limit:
            break
        try:
            # Perform some action on each post in the collection we receive from
            # Facebook.
            all_posts += [{'message': post['message'], 'updated': fb_to_datetime(post['updated_time'])} for post in posts['data'] if 'message' in post]
            # Attempt to make a request to the next page of data, if it exists.
            posts = requests.get(posts['paging']['next']).json()
            print("{} posts loaded, last post from {}".format(len(all_posts), all_posts[-1]['updated']))
        except KeyError:
            all_posts += [post['message'] for post in posts['data']]
            # When there are no more pages (['paging']['next']), break from the
            # loop and end the script.
            break

    return all_posts


def main():
    name = 'seznamka_texts.cache'
    if not os.path.isfile(name):
        all_posts = load_posts()
        with open(name, 'wb+') as file:
            pickle.dump(all_posts, file)
    else:
        with open(name, 'rb') as file:
            all_posts = pickle.load(file)
    # now I have all posts.

    # matches = ['koč', 'mač']
    re.match('jsem bi', '')
    matches = ['[^a-z]bi[^a-z]', '[^a-z]bisex', 'jsem b']
    print("total posts: {}".format(len(all_posts)))
    # [print(post['message']) for post in all_posts]
    post_messages = [post['message'] for post in all_posts]
    # cat_posts_bool = [any([cat in post.lower() for cat in cats]) for post in post_messages]
    matched_posts = [post for post in all_posts for match in matches if re.match(match, post['message'], re.I)]
    # print("cat posts: {}".format(sum(cat_posts_bool)))
    print("cat posts: {}".format(len(matched_posts)))
    [print(post['message']) for post in matched_posts]
    show_posts_histogram(all_posts, matched_posts)


def show_posts_histogram(posts, cat_posts):
    # generate some random data (approximately over 5 years)
    # convert the epoch format to matplotlib date format
    mpl_data1 = [mdates.date2num(post['updated']) for post in posts]
    mpl_data2 = [mdates.date2num(post['updated']) for post in cat_posts]

    # plot it
    fig, ax = plt.subplots(2, 1)

    # ax.grid(color='black', linestyle='-.', linewidth=1)
    from_day = datetime(2015, 12, 1)
    to_day = datetime(2017, 11, 1)
    months = list(rrule(MONTHLY, dtstart=from_day, until=to_day))

    ax1 = ax[0]
    ax2 = ax[1]
    ax1.grid()
    ax1.set_title('počet příspěvků')
    ax2.grid()
    ax2.set_title('počet příspěvků s kočkami')
    ax1.hist(mpl_data1, bins=[int(i) for i in mdates.date2num(months)], edgecolor='black')
    # ax.hist(mpl_data, bins=24, edgecolor='black')
    # ax.hist(mpl_data, bins=50, color='lightblue')
    ax1.xaxis.set_major_locator(mdates.MonthLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m.%y'))
    ax2.hist(mpl_data2, bins=[int(i) for i in mdates.date2num(months)], edgecolor='black')
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m.%y'))
    plt.show()


if __name__ == '__main__':
    # You'll need an access token here to do anything.  You can get a temporary one
    # here: https://developers.facebook.com/tools/explorer/
    access_token = open('access_token.txt').readline()
    main()