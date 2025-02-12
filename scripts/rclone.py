import os
import logging
import configparser
from typing import Callable, List, Union

from rclone_python import rclone
from rclone_python.remote_types import RemoteTypes

SECRET_NAME = "RCLONE_CONFIG"

def LoadRemoteConfig(name):
    config = configparser.ConfigParser()
    config.read(f'/run/secrets/{SECRET_NAME}')
    if len(config) == 0:
        raise Exception(f"Failed to load rclone config")
    if name is None or name == "":
        raise Exception("RCLONE_NAME not found in environment")
    if not name in config:
        raise Exception(f"Config {name} not found from RCLONE_CONFIG")
    if len(config[name]) == 0:
        raise Exception(f"Config {name} is empty")
    return config

def hasRemote(remote):
    return remote+":" in rclone.get_remotes()

def createRemote(remote, config:configparser.ConfigParser):
    if hasRemote(remote+":"):
        return
    with open(f"{os.getenv('HOME')}/.config/rclone/rclone.conf", "w") as f:
        config.write(f)
    if not hasRemote(remote+":"):
        raise Exception("Failed to create remote")

class Recorder:
    # Records all updates provided to the update function.
    def __init__(self):
        self.history = []

    def update(self, update: dict):
        self.history.append(update)

    def get_summary_stats(self, stat_name: str) -> List[any]:
        # returns the stats related to the overall transfer task.
        return [update[stat_name] for update in self.history]

    def get_subtask_stats(self, stat_name: str, task_name: str) -> List[any]:
        # returns stats related to a specific subtask.
        return [
            task_update[stat_name]
            for update in self.history
            for task_update in update["tasks"]
            if task_update["name"] == task_name
        ]

def copy(source, destination):
    recorder = Recorder()
    os.makedirs(destination, mode=775, exist_ok=True)
    rclone.copy(source, destination, listener=recorder.update, args=["--drive-shared-with-me"])
    return recorder

def sync(source, destination):
    recorder = Recorder()
    os.makedirs(destination, mode=775, exist_ok=True)
    rclone.sync(source, destination, listener=recorder.update, args=["--drive-shared-with-me"])
    return recorder

def check(source, destination):
    returncode, result = rclone.check(source, destination, args=["--drive-shared-with-me"])
    return_result = []
    for obj in result:
        if obj[0] != '=':
            return_result.append(obj)
    return return_result
def init():
    if not rclone.is_installed():
        raise Exception("Rclone is not installed")
    # print("Setting up rclone")
    if not hasRemote(remote_name):
        remote_name = os.getenv("RCLONE_NAME")
        remote_config = LoadRemoteConfig(remote_name)
        createRemote(remote_name, remote_config)
    if not hasRemote(remote_name):
        raise Exception("Failed to create remote")


init()