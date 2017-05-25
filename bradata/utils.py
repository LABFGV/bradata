import zipfile
import os
import bradata
import datetime

# python has a built-in type checking tool in python 3.5
def _treat_inputs(value):
    """
    Make sure that inputs are in the right type

    Ints and floats are converted to strings

    Args:
        value: str, int, float

    Returns: Ints and floats are converted to strings
    """
    if value is None:
        return value

    if not isinstance(value, (int, float, str)):
        raise AttributeError('This is a {}.\n'
                             'Make sure to insert an int, float or str'
                             .format(type(value)))

    if isinstance(value, (int, float)):
        value = str(value)

    return value


def _must_contain(this=None, keys=None):
    """
    Check whether the specified values exists on a dict

    This function presumes that all keys are mapped on this dict
    Args:
        this: dict :: variable names and their values
        keys: list :: variable names that must not be None

    Returns:
        True if the dict contains the values
        Raise error if there are missing values
    """

    result = {k: v is None for k, v in this.items() if k in keys}

    missing_keys = [k for k, v in result.items() if v is True]

    if len(missing_keys) != 0:
        raise AttributeError('{} must have a value'.format(','.join(str(p) for p in missing_keys)))

    else:
        return True

def _unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)


def _set_download_directory(user_path=None):
    """
    sets up a directory where files downloaded by bradata will be stored. it is 
    usually located in the user's home directory at bradata/, but a personalized 
    path can be set. there's currently no support for persisting this personal 
    choice.
    """
    if user_path is None:
        user_path = os.path.expanduser('~')
    download_path = os.path.join(user_path, "bradata_download")
    try:
        os.makedirs(download_path)
    except FileExistsError:
        pass
    except PermissionError:
        user_path = input("bradata doesn't seem to have the permission to write to the default download directory. please specify your desired download path:\n ")
        download_path = _set_download_directory(user_path)  # to check if provided path is writable
    return download_path

def _create_download_subdirectory(submodule_name):
    submodule_download_path = os.path.join(bradata.__download_dir__, submodule_name)
    if not os.path.exists(submodule_download_path):
        os.mkdir(submodule_download_path)
    return None


def _parse_time(date, freq='d'):
    #add docs and make tests (what happens if begin_date>end_date?)
    freq_dict = {'d': '%Y-%m-%d', 'm': '%Y-%m', 'y': '%Y', 'a': '%Y'}
    freq_str = freq_dict[freq.lower()]
    if date is None:
        date = datetime.date.today()
    elif isinstance(date, str):
        date = datetime.datetime.strptime(date, freq_str).date()
    if not isinstance(date, datetime.date):
        raise Exception("begin_date or end_date not valid input. input must be string in {} format or a valid datetime object.".format(freq_str))
    return date
