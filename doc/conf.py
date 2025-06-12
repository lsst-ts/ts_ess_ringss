"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
For more information, see:
https://developer.lsst.io/stack/building-single-package-docs.html
"""

import lsst.ts.ess.ringss  # noqa
from documenteer.conf.pipelinespkg import *  # type: ignore # noqa

project = "ts_ess_ringss"
html_theme_options["logotext"] = project  # type: ignore # noqa
html_title = project
html_short_title = project
