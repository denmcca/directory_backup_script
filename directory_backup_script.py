#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import configparser
from datetime import datetime
from time import sleep
import logging
import shutil
import contextlib
import argparse

DATETIME_LOG_FMT = '%Y%m%dT%H%M%S.log'

# get configs (destination and path to directories file)
def get_configs():
    parser = configparser.ConfigParser()
    parser.read('configs')
    return {k: configs[k] for _, configs in parser.items() for k in configs}

# get directories {name:path}
def get_directory_from_list(dir_file):
    # returns backup name and path to files to backup.
    with open(dir_file, 'rt') as dirs:
        _name, _path = next(dirs).split(':')
        yield _name, os.path.expanduser(_path) 


def get_last_time_backup(backup_dir_path):
    logs_path = os.path.join(backup_dir_path, 'logs')
    if not os.path.isdir(logs_path):
        os.makedirs(logs_path)
    logs = os.listdir(logs_path)
    if not logs:
        return datetime.fromtimestamp(0)
    logs.sort()
    last_log = logs[-1]
    timestamp = datetime.strptime(last_log, DATETIME_LOG_FMT)
    return timestamp

@ contextlib.contextmanager
def open_log_file(backup_dir_path, datetime_in):
    try:
        os.makedirs(backup_dir_path + os.sep + 'logs', exist_ok=True)
    except OSError:
        pass
    with open(os.path.join(
            backup_dir_path,
            'logs', 
            datetime_in.strftime(DATETIME_LOG_FMT)), 'wt') as log_file:
        log_file.write('No files were updated.')
        log_file.seek(0)
        print('writing to: %s' % log_file.name)
        yield log_file


def get_backup_dir(dst_dir, src_path):
    dst_dir_split_without_name = dst_dir.split(os.sep)[:-2]
    src_file_split = src_path.split(os.sep)
    backup_dir = os.sep.join(dst_dir_split_without_name + 
        src_file_split[len(dst_dir_split_without_name):])
    try:
        os.makedirs(backup_dir, exist_ok=True)
    except OSError:
        pass
    return backup_dir


def define_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true')


def set_logger(level=logging.WARNING):
    logging.getLogger().setLevel(level=level)


def file_more_recent(file_in, last_log_time):
    file_in_dt = datetime.fromtimestamp(os.path.getmtime(file_in))
    last_log_dt = last_log_time
    print('file_in_dt: %s' % file_in_dt)
    print('lst_log_dt: %s' % last_log_dt)
    return datetime.fromtimestamp(os.path.getmtime(file_in)) > last_log_time 


def copy_file_to_backup(target_file, backup_dir):
    backup_file = os.path.join(backup_dir, os.path.basename(target_file))
    return shutil.copy2(target_file, backup_file)


def log_file_copy(log_file, target_file, backup_file):
    log_file.write('%s %s -> %s\n' % (datetime.now(), file_path, backup_file))

# Get script arguments.
args = define_args()
# Set logging level.
set_logger(logging.DEBUG if args.debug else logging.WARNING)

# for each name copy file from path to backup location if files were changed/updated.
# maybe add a delay (1ms) for each file to reduce impact.
configs = get_configs()
delay = configs.get('delay_in_ms', 0) / 1000
dirs_to_backup = get_directory_from_list(configs.get('directories'))
backup_root = os.path.expanduser(configs.get('backup_root'))
logging.debug('BackupTo: %s' % backup_root)
for backup_name, curr_dir in dirs_to_backup:
    logging.debug('Scanning: %s' % curr_dir)
    # destination for backup of curr_dir.
    backup_dir = os.path.join(backup_root, backup_name)
    # last time logs taken for curr_dir.
    last_log_time = get_last_time_backup(backup_dir)
    logging.debug('Last Log Time: %s' % last_log_time)
    new_log_time = datetime.now()
    # real directory name of backedup directory within backup_dir.
    backup_path = os.path.join(backup_dir, os.path.basename(curr_dir))
    with open_log_file(backup_dir, new_log_time) as log_file:
        for root, _, files in os.walk(curr_dir):
            backup_dir = get_backup_dir(backup_path, root)
            logging.debug('BackupDir: %s' % backup_dir)
            for f in files:
                file_path = os.path.join(root, f)
                logging.debug('Checking: %s' % file_path)
                if file_more_recent(file_path, last_log_time):
                    copied_file = copy_file_to_backup(file_path, backup_dir)
                    log_file_copy(log_file, file_path, copied_file)
                    # Slow down to lower impact at runtime.
                    sleep(delay)
