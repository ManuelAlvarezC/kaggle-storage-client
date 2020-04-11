import os
import json

from kaggle import KaggleApi

from kaggle_storage_client.helpers import leafname, print_fstree
from kaggle_storage_client.local import LocalStorage

DATASET_METADATA_FILE = 'dataset-metadata.json'
DEFAULT_DATA_DIR = 'data'
DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.kaggle/kaggle.json')


class KaggleStorageClient:
    """Syncs a local folder with you datasets in kaggle.

    The constructor for KaggleStorage accepts three ways to autenticate into kaggle:
    1. The default one (using kaggle config file in the default path), can be used with
        (both lines are equivalent):

    >>> KaggleStorageClient()
    >>> KaggleStorageClient(login_file=True, configfile=None)

    2. Using a kaggle config file on another path(both lines are equivalent):
    >>> KaggleStorageClient(configfile='path/to/file')
    >>> KaggleStorageClient(login_file=True, configfile='path/to/file')

    3. Using environment variables:
    >>> KaggleStorageClient(login_file=False)

    Arguments:
        datadir(str): Optional. Path to the local folder to use as storage root.
        login_file(bool): Wheter to use the kaggle.json auth file or the environ variables.
        configfile(Union[str, None]): Path to the config file.

    """
    def __init__(self, datadir=None, login_file=True, configfile=None):
        if login_file and configfile is None:
            configfile = DEFAULT_CONFIG_FILE

        if datadir is None:
            datadir = DEFAULT_DATA_DIR

        self.__local_storage = self.__get_local_storage(datadir)
        self.__api = self.__get_authenticated_kaggle_api(configfile)

    @property
    def api(self):
        return self.__api

    @property
    def local_storage(self):
        return self.__local_storage

    @property
    def username(self):
        return os.environ['KAGGLE_USERNAME']

    def __get_local_storage(self, root):
        # ensure root folder
        if not os.path.exists(root):
            os.mkdir(root)

        return LocalStorage(root=root)

    def __get_authenticated_kaggle_api(self, configfile):
        if configfile is not None:
            self.login_with_configfile(configfile)

        api = KaggleApi()
        api.authenticate()
        return api

    def __patch_metadata_file(self, dataset_folder):
        metadata_filepath = os.path.join(dataset_folder, DATASET_METADATA_FILE)
        with open(metadata_filepath) as mdfile:
            md = json.load(mdfile)

        slug = '-'.join(leafname(dataset_folder).split(' ')).lower()
        title = ' '.join(map(lambda token: f'{token[0].upper()}{token[1:]}', slug.split('-')))
        md['id'] = md['id'].replace('INSERT_SLUG_HERE', slug)
        md['title'] = md['title'].replace('INSERT_TITLE_HERE', title)

        with open(metadata_filepath, 'w') as mdfile:
            mdfile.write(json.dumps(md))
        print(json.dumps(md))

    def login_with_configfile(self, configfile):
        with open(configfile) as f:
            credentials = json.load(f)

        os.environ['KAGGLE_USERNAME'] = credentials['username']
        os.environ['KAGGLE_KEY'] = credentials['key']

    def logout(self):
        """Remove kaggle login configuration"""
        del os.environ['KAGGLE_USERNAME']
        del os.environ['KAGGLE_KEY']

    def create(self, dataset):
        """Create dataset"""
        raise NotImplementedError()

    def download(self, username, dataset, filename):
        """Download the file from the kaggle dataset"""
        dataset_file_content = self.api.datasets_download_file(username, dataset, filename)
        filepath = self.local_storage.save(username, dataset, filename, dataset_file_content)
        folder = os.path.dirname(filepath)
        metadata_filepath = os.path.join(folder, DATASET_METADATA_FILE)
        if not os.path.exists(metadata_filepath):
            self.api.dataset_initialize(folder)
            self.__patch_metadata_file(folder)
            self.api.dataset_create_version(folder, version_notes="Patch metadata.")
        return filepath

    def add_local(self, dataset, name, content, content_call=None):
        """Add data in the local storage.

        The value of content changes the behavior of this method:
        - If content is ``str`` it will be considered a full path to a file, and this function
        will copy to the path generated with root/username/dataset/filename.

        - If content is ``pandas.DataFrame``, then parameter ``content_call`` is required, and will
        be used the following way:
        1. The first element of the tuple, will be considered the method name.
        2. The second the positional arguments, the filename must NOT be present
        3. The third the keyword arguments.

        Arguments:
            dataset(str): Name of the dataset (will be used as subfolder in local storage).
            name(str): Name of the file
            content(str or pandas.DataFrame): Either a dataframe or path to a file.
            content_call(tuple[str, list, dict]): Call to execute on content if it's a DataFrame

        """
        return self.local_storage.save(self.username, dataset, name, content, content_call)

    def upload(self, dataset, filepath):
        """Uploads the given ``filepath`` as part of ``dataset`` in kaggle. """

        if not os.path.exists(filepath):
            raise FileNotFoundError(filepath)

        folder = os.path.dirname(filepath)
        print(f'upload -dataset {dataset} -filepath {filepath} -folder {folder}')
        remote_status = self.api.datasets_status(self.username, dataset)
        print(f'remote-status of {self.username}/{dataset} is {remote_status}')

        if 'dataset-metadata.json' not in os.listdir(folder):
            self.api.dataset_initialize(folder)
        self.__patch_metadata_file(folder)

        if remote_status != 'ready':
            print(f'create {folder}')
            print_fstree(folder)
            return self.api.dataset_create_new_cli(folder)
        else:
            print(f'sync {folder}')
            print_fstree(folder)
            return self.api.dataset_create_version_cli(
                folder,
                version_notes=f"Upload {leafname(filepath)} to {dataset}."
            )

    def add_upload(self, dataset, name, content, content_call=None):
        """Add a dataset to the local storage and then upload to kaggle.

        Arguments:
            dataset(str): Name of the dataset (will be used as subfolder in local storage).
            name(str): Name of the file
            content(str or pandas.DataFrame): Either a dataframe or path to a file.
            content_call(tuple[str, list, dict]): Call to execute on content if it's a DataFrame
        """
        filepath = self.add_local(dataset, name, content, content_call)
        self.upload(dataset, filepath)
