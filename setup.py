# Copyright 2013 David Irvine
#
# This file is part of gridengine-accounting
#
# gridengine-accounting is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# gridengine-accounting is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with gridengine-accounting.  If not, see <http://www.gnu.org/licenses/>.

from distutils.core import setup
setup(
	name="gridengine-accounting",
	version="0.1",
	description="Read grid engine accounting files",
	author="David Irvine",
	author_email="irvined@gmail.com",
	url="https://github.com/irvined1982/gridengine-accounting",
	packages=["gridengine_accounting"],
	classifiers=[
		"License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
		"Natural Language :: English",
		"Operating System :: POSIX",
		"Topic :: Scientific/Engineering",
		"Topic :: Utilities",
		],
)
