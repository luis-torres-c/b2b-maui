import argparse
import os
parser = argparse.ArgumentParser(description='Delete files csv')
parser.add_argument('path', type=str, help='File Path to delete')
args = parser.parse_args()


def delete(path):
    rootDir = path
    for dirName, subdirList, fileList in os.walk(rootDir):
        for fname in fileList:
            print('Archivo: {}'.format(fname))
            file_path = os.path.join(dirName, fname)
            if os.path.isfile(file_path) and fname.endswith(".csv"):
                print('Removing {}'.format(file_path))
                os.unlink(file_path)

# def delete(path):
#     folder_path_objects = path+r'objects'
#     folder_path_metrics = path+r'metrics'
#     folders_objects = os.listdir(folder_path_objects)
#     folders_metrics = os.listdir(folder_path_metrics)
#     for x in range(len(folders_objects)):
#         file_path3 = os.path.join(folder_path_objects, folders_objects[x])
#         print(file_path3)
#         delete_folder(file_path3)
#     for x in range(len(folders_metrics)):
#         file_path2 = os.path.join(folder_path_metrics, folders_metrics[x])
#         print(file_path2)
#         delete_folder(file_path2)


# def delete_folder(path):
#     file_list = os.listdir(path)
#     for file_name in file_list:
#         file_path = os.path.join(path, file_name)
#         if os.path.isfile(file_path):
#             print('Removing {}'.format(file_path))
#             os.remove(file_path)


if __name__ == '__main__':
    delete(args.path)
