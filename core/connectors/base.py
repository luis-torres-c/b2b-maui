import os.path


class Connector:
    @classmethod
    def get_instance(cls, **kwargs):
        raise NotImplementedError


class FileSystemConnector(Connector):

    def get_object_file(self, file_path, **kwargs):
        raise NotImplementedError

    def get_list_files(self, path):
        raise NotImplementedError


class ObjectFile:
    def __init__(self, full_file_path, not_found=False, data=None):
        p = os.path.split(full_file_path)
        self.path = p[0]
        self.filename = p[1]
        self._data = data
        self._filepath = ''
        self._not_found = not_found

    def absolute_path(self):
        if self.filepath:
            return self.filepath
        import os
        return os.path.join(self.path, self.filename)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def filepath(self):
        return self._filepath

    @filepath.setter
    def filepath(self, filepath):
        self._filepath = filepath

    @property
    def not_found(self):
        return self._not_found

    @not_found.setter
    def not_found(self, is_found):
        self._not_found = is_found
