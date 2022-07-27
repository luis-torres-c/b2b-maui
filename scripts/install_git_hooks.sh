#!/bin/bash
pre_commit_python="#!/usr/bin/env python

import os
import re
import shutil
import subprocess
import sys
import tempfile


def system(*args, **kwargs):
    kwargs.setdefault('stdout', subprocess.PIPE)
    proc = subprocess.Popen(args, **kwargs)
    out, err = proc.communicate()
    return out


def main():
    modified = re.compile('^[AM]+\s+(?P<name>.*\.py(?:\.\w+)?)', re.MULTILINE)
    files = system('git', 'status', '--porcelain')
    files = modified.findall(files.decode('utf-8'))

    tempdir = tempfile.mkdtemp()
    for name in files:
        print('checking file {}'.format(name))
        filename = os.path.join(tempdir, name if name.endswith('.py') else '{}.py'.format(name))
        filepath = os.path.dirname(filename)
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        with open(filename, 'w') as f:
            system('git', 'show', ':{}'.format(name), stdout=f)
    output = system('flake8', '--ignore=E501', '.', cwd=tempdir)
    shutil.rmtree(tempdir)
    if output:
        print(output.decode('utf-8'))
        sys.exit(1)

if __name__ == '__main__':
    main()
"

pre_commit='#!/usr/bin/env bash

PYTHON_RES=$(.git/hooks/pre-commit-python)
rc=$?
echo "$PYTHON_RES"

if [[ $rc != 0 ]]; then
    exit $rc;
fi
'

project_path=$PWD
echo "base path is $project_path"

for f in 'pre-commit' 'pre-commit-python'; do echo "installing $f"
    `touch $project_path/.git/hooks/$f`;
    `chmod a+x $project_path/.git/hooks/$f`;
    tpl_name=$(echo $f | sed 's/-/_/g')
    tpl="${!tpl_name}"
    `echo "$tpl" > $project_path/.git/hooks/$f`
done

echo "done."
