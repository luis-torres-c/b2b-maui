import os
import shutil
import argparse
parser = argparse.ArgumentParser(description='Copy Directory')
parser.add_argument('original_path', type=str, help='File Path to copy directory')
parser.add_argument('destination_path', type=str, help='File Path to copy directory')
args = parser.parse_args()


def copy_directory(original_path, destination_path):
    if os.path.exists(original_path):
        print('Copying tree..')
        tree = shutil.copytree(original_path, destination_path)
        print('Tree copied to', tree)
    for dir_name, subdirList, fileList in os.walk(destination_path):
        if not os.listdir(dir_name):
            print('Removing an empty directory : {}'.format(dir_name))
            shutil.rmtree(dir_name)


if __name__ == '__main__':
    copy_directory(args.original_path, args.destination_path)
