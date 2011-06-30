# -*- coding: utf-8 -*-

class Message(Exception):
    def __init__(self, text):
        self.text = text
    def __repr__(self):
        return self.text

class Error(Message):
    pass
