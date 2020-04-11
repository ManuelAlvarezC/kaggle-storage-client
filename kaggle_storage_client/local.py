import os
import shutil

import pandas as pd


class LocalStorage:

    def __init__(self, root):
        if not os.path.exists(root):
            raise FileNotFoundError(root)
        self.__root = root

    @property
    def root(self):
        return self.__root

    @property
    def datasets(self):
        return [d for d in os.listdir(self.root)]

    @property
    def files(self):
        folder_files = [(d, f) for d, _, f in os.walk(self.root)]
        return [os.path.join(folder, file) for folder, files in folder_files for file in files]

    def save(self, username, dataset, filename, content, content_call=None):
        """Stores the content on the local storage in path username/dataset/filename.

        The value of content changes the behavior of this method:
        - If content is ``str`` it will be considered a full path to a file, and this function
        will copy to the path generated with root/username/dataset/filename.

        - If content is ``pandas.DataFrame``, then parameter ``content_call`` is required, and will
        be used the following way:
        1. The first element of the tuple, will be considered the method name.
        2. The second the positional arguments, the filename must NOT be present
        3. The third the keyword arguments.

        See the example below:

        >>> username = 'user'
        >>> dataset = 'example'
        >>> filename = 'somefile.csv'
        >>> content = pandas.DataFrame([{'A': 0, 'B': 1}])
        >>> content_call = ('to_csv', [], {'index': False, 'header': True})
        >>> LocalStorage.save(username, dataset, filename, content, content_call)

        Arguments:
            username(str): Kaggle username.
            dataset(str): Dataset name.
            filename(str): Local name of your file.
            content(Union[str, pandas.DataFrame]): Contents of the file.
            content_call(tuple[str, list, dict]): Call to execute on content if it's a DataFrame

        Returns:
            str: Complete file path for the file created.

        """
        if isinstance(content, pd.DataFrame) and content_call is None:
            raise ValueError('When content is a pandas.DataFrame, content_call can\'t be null')

        dataset_dir = os.path.join(self.root, username, dataset)
        file_path = os.path.join(dataset_dir, filename)

        if not os.path.exists(dataset_dir):
            os.makedirs(dataset_dir)

        if isinstance(content, pd.DataFrame):
            method, args, kwargs = content_call
            getattr(content, method)(file_path, *args, **kwargs)
        else:
            shutil.copy(content, file_path)

        return file_path
