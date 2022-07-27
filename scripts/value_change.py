import os
import glob
import argparse

parser = argparse.ArgumentParser(description='Change username or empresa from files')
parser.add_argument('--username', dest='username', type=str)
parser.add_argument('--empresa', dest='empresa', type=str)
parser.add_argument('--new_username', dest='new_username', type=str)
parser.add_argument('--new_empresa', dest='new_empresa', type=str)
parser.add_argument('--path', dest='path', type=str)
args = parser.parse_args()


username = args.username
empresa = args.empresa
new_username = args.new_username
new_empresa = args.new_empresa

SOURCE_INT_PATH = args.path

file_name = '*_{b2b_username}_{b2b_empresa}_*.csv'
file_name = file_name.format(b2b_username=username, b2b_empresa=empresa)
file_path = os.path.join(SOURCE_INT_PATH, '**', file_name)

files = glob.glob(file_path, recursive=True)

for filename in files:
    fn = os.path.basename(filename)
    new_fn = fn.split('_')
    if new_username:
        new_fn[1] = new_username
    if new_empresa:
        new_fn[2] = new_empresa
    fn_new = '_'.join(new_fn)

    print("Change {} to {}".format(fn, fn_new))
    os.rename(filename, filename.replace(fn, fn_new))
