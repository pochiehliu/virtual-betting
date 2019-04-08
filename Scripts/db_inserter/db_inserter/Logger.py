import os
import datetime as dt


class Logger:
    def __init__(self, name, loc='', overwrite=False):
        if overwrite:
            self.name = loc + name
            self._load(kind='w+')
        else:
            self.name = loc + name
            self._load(kind='a+')
            self._close()

    def log(self, info):
        self._load(kind='a+')
        self.file.write(self._get_date() + ': ' + info + '\n')
        self._close()

    def _load(self, kind):
        self.file = open('./' + self.name + '.txt', kind)

    def _close(self):
        self.file.close()

    @staticmethod
    def _get_date():
        return str(dt.datetime.now())
