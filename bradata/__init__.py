# -*- coding: utf-8 -*-
import bradata.agencias
import bradata.cgu
import bradata.tse
import bradata.utils
from pkg_resources import get_distribution as _get_distribution

try:
    __version__ = _get_distribution(__name__).version
except:
    __version__ = 'unknown'


__download_dir__ = bradata.utils._set_download_directory()
