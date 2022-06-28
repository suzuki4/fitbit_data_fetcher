#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  6 16:04:17 2020

@author: user
"""

import sys
from slacker import Slacker
import traceback

import logging
logger = logging.getLogger(__name__)

from parameter import Conf, Args
conf = Conf().conf
args = Args().args
slack = Slacker(conf.get("slack", "token"))


class Message:
    
    def __init__(self, msg=None, sep="\n"):
        self.msg = ""
        if msg is not None:
            self.add(msg, sep)
        
    def add(self, msg, sep="\n"):
        self.msg += msg
        self.msg += sep
    
    def concat(self, msg_obj):
        self.msg += msg_obj.msg

    def add_new_line(self, sep="\n"):
        self.msg += sep

    def add_deco(self, msg, sep="\n", deco="[]"):
        msg = f"{deco[0]}{msg}{deco[1]}"
        self.add(msg, sep)

    def send_slack(self, channel=None, as_user=True):
        slack_msg(self.msg)


def notify_slack(start=True, channel=None, as_user=True):
    
    def _notify_slack(func):         
        def wrapper(*arg, **kwargs):
            if start:
                slack_msg(f'Start {args}.', channel=channel, as_user=as_user)
            ret = func(*arg, **kwargs)
            slack_msg(f'End {args}.', channel=channel, as_user=as_user)
            return ret
        return wrapper
    return _notify_slack
    

def slack_msg(msg, channel=None, as_user=True):
    
    if channel is None:
        channel = conf.get("slack", "channel")
        
    slack.chat.post_message(channel, msg, as_user=as_user)


def slack_error(msg, channel=None, as_user=True):
    
    msg = f"<!channel> ERROR: {msg}"
    slack_msg(msg, channel, as_user)


def slack_warning(msg, channel=None, as_user=True):
    
    msg = f"WARNING: {msg}"
    slack_msg(msg, channel, as_user)
    	

def log_info(func):    

    def wrapper(*args, **kwargs):
        logger.info(f"Start {func.__name__}.")
        ret = func(*args, **kwargs)
        logger.info(f"End {func.__name__}.")
        return ret

    return wrapper


def log_debug(func):    

    def wrapper(*args, **kwargs):
        logger.debug(f"Start {func.__name__}.")
        ret = func(*args, **kwargs)
        logger.debug(f"End {func.__name__}.")
        return ret

    return wrapper


def handle_error(_continue=True):    
    
    def _handle_error(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                #msg = f"{str(type(e))} {str(e)}"
                #logger.error(msg)
                logger.exception(e)
                #slack_error(msg)
                slack_error(traceback.format_exc())
                if _continue:
                    logger.info("Continue process.")
                else:
                    sys.exit(1)
        return wrapper
    return _handle_error
