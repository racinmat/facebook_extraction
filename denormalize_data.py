import utils
from utils import unify_data_group, save_binary_data


def main():
    for group_name, group_id in groups.items():
        data = unify_data_group(group_name)
        save_binary_data(group_name, data)


if __name__ == '__main__':
    # utils.texts_root = 'texts_test'
    utils.texts_root = 'texts'
    groups = {
        'scitani_ceskych_a_slovenskych_otaku': '135384786514720'
    }

    main()

    print("Everything denormalized")
