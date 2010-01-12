#!/usr/bin/env python
"""
 Copyright (c) 2005-2008 Ingres Corporation. All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 as
 published by the Free Software Foundation.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""

from distutils.core import setup, Extension
import os
import sys
import re
from glob import glob

try:
    from docutils.core import publish_cmdline
except ImportError:
    publish_cmdline = None


"""
 History

    13-Sep-2004 - Initial creation (crogr01@ca.com)
        Targeted at Linux.

    13-Sep-2004 - (loera01@ca.com/clach04@ca.com)
        preliminary support for win32 

    20-Sep-2004 - (crogr01@ca.com)
        fixed libraries to link against the correct ODBC/CLI library.
        
    24-Sep-2004 - (clach04@ca.com)
        Added hack for win32 compiles to deal with lack of 
        bit varying (SQL_BIT_VARYING) in MS ODBC headers.
        Put in correct library name for ODBC (under win32).
    20-Oct-2003 - (crogr01@ca.com)
        Incremented version to 0.9.1.3 
    21-Oct-2003 - (crogr01@ca.com)
        removed reference to ingres.lib since it is not used.
    25-Oct-2003 - (crogr01@ca.com)
        Added DBIVERSION macro definition
    21-Dec-2004 - (Ralph.Loen@ca.com)
        Incremented version to 1.0.4.
    10-Jan-2005 - (Ralph.Loen@ca.com)
        Incremented version to 1.0.5.
    31-Jan-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.0.6.
    16-Feb-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.0.7.
    23-Feb-2004 (Ralph.Loen@ca.com)
        Incremented version to 1.0.8.
    19-May-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.0.9.
    19-May-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.5.0.
    24-June-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.9.0.
    16-Nov-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.9.1.
    22-Nov-2005 (Ralph.Loen@ca.com)
        Incremented version to 1.9.3.
    23-Jan-2005 (Chris.Clark@ingres.com)
        Simple helper changes:
            Allow script to be called directly.
            Check II_SYSTEM is set.
            Changed few CA references
        Still need to change license and url for project.
    27-Feb-2005 (Chris.Clark@ingres.com)
        Incremented version to 1.9.4.
        Removed duplicate version string constants.
        Added simple help output if no params given.
        Replaced readme.html with a text file and added html generation.
    25-May-2006 (Ralph.Loen@ingres.com)
        Removed markdown dependencies and went back to a straight README.html
        file.  Incremented version to 1.9.5.
    06-June-2006 (Ralph.Loen@ingres.com)
        Incremented version to 1.9.6.
    09-Aug-2006 (Ralph.Loen@ingres.com)
        Incremented version to 2.0.0.
    05-May-2008 (grant.croker@ingres.com)
        Incremented version to 2.0.1.
    06-May-2008 (grant.croker@ingres.com)
        Added Trove tags, the trove classification for distutils is documented in PEP 301
        http://www.python.org/dev/peps/pep-0301/
    04-Aug-2009 (Chris.Clark@ingres.com)
        Updated Trove tags with new maintainer.
        Updated so that sdist now includes the headers so that sdist
        produced tarballs can be built.
    18-Nov-2009 (Chris.Clark@ingres.com)
        README.html is now created by docutils.
    12-Jan-2010 (Chris.Clark@ingres.com)
        Retract fix for Trac bug 224 - rt library is not needed on 
        any platform. rt lib is missing from Mac OSX which prevents
        it from building.

 Known Issues

    * there is no support for installing to an alternate directory.
          o perhaps there is in distutils, not investigated at this time.
    * rpm not available as yet to do this but can be built if required.
    * problem with "misc" dir/file under win32 - commented it out!
    * need to included header files.
    * to force a full refresh, delete the manifest file first.
    * win32 only, python 2.3 only) - if distutils fails with:
    
        error: Python was built with version 6 of Visual Studio, 
        and extensions need to be built with the same version of 
        the compiler, but it isn't installed.
        
      and you do have version 6 of Visual Studio installed run 
      "Microsoft Visual C++ 6.0" msdev.exe (the GUI) then quit out and 
      retry the build. See Python mailing list:
      
       http://mail.python.org/pipermail/python-dev/2003-November/040478.html

      ...for further information.

    * win32 only, python 2.4 need .NET SDK (or msys/mingw32) to build
      Consider be using:
          setup.py bdist_wininst
          setup.py bdist_msi
      To create installers for end users.

"""
    
if len(sys.argv) <= 1:
    print """
Suggested setup.py parameters:

    * build
    * install
    * sdist
    * bdist_wininst

"""

# Normal setup.py functions for platform/version packaging

# Take a copy of platform incase sys.platform is insufficient and we need more granularity
platform=sys.platform

"""
setup some variables to for Ingres stuff cannot use plain env vars in python variables.
"""
if os.path.expandvars("$II_SYSTEM") == "$II_SYSTEM":
    raise SystemExit, "Operating system variable II_SYSTEM must be set."
assert(os.path.expandvars("$II_SYSTEM") != '$II_SYSTEM')
ii_system=os.path.expandvars("$II_SYSTEM")
ii_system_files=ii_system+"/ingres/files"
ii_system_lib=ii_system+"/ingres/lib"


# version of the driver
# change only this value when incrementing the driver level
dbiversion='2.0.1'
dbiversion_str='"""%s"""' % (dbiversion)

filename_txt = 'README.txt'
dest_html = os.path.splitext(filename_txt)[0] + '.html'

if os.path.exists(dest_html) and os.path.getmtime(dest_html) < os.path.getmtime(filename_txt):
    os.unlink(dest_html)

if not os.path.exists(dest_html) and publish_cmdline:
    print 'creating %s' % dest_html
    publish_cmdline(writer_name='html',
                        argv=[
                                '--initial-header-level=2',
                                '--stylesheet-path=ingres_style.css',
                                filename_txt,
                                dest_html,
                            ]
                    )


# Default build flags, libraries, etc.
defmacros=[("DBIVERSION", dbiversion_str)]
libraries=["iiodbc.1","m", "c", "rt"]
libraries=["iiodbc.1","m", "c"]

# wintel build flags, hopefully the same for amd64 winxp
if platform=="win32":
    # Note! May have more defines than we really need.
    defmacros=[("IMPORT_DLL_DATA",None),
               ("_DLL", None),
               ("_MT", None),
               ("DESKTOP", None),
               ("INGRESII", None),
               ("_X86_", None),
               ("i386", 1),
               ("INCLUDE_ALL_CL_PROTOS", None),
               ("DEVL", None),
               ("WIN32", None),
               ("NT_GENERIC", None),
               ("SQL_BIT_VARYING", 15),
               ("DBIVERSION",dbiversion_str)
               ]
    libraries=["odbc32","msvcrt","kernel32"]
"""

    The "sources" section below could be replaced with a static MANIFEST.IN
    file, but at the moment we are leaving all the build logic in one file,
    and the MANIFEST file gets created dynamically, by distutils. 
    The MANIFEST.in is generated semi-dynamically from this script. 
    Note that if one changes the contents of setup.py, the MANIFEST file
    needs to be deleted.  Note
    also that MANIFEST is a Jam file as well, so Jam needs to be taught to
    ignore the pydbi directory entirely.

"""
if os.path.exists('MANIFEST'):
    os.unlink('MANIFEST')
MANIFEST_in=open('MANIFEST.in', 'w')
MANIFEST_in.write('recursive-include hdr *.h\n')
MANIFEST_in.write('include CHANGELOG\n')
MANIFEST_in.write('include LICENSE\n')
MANIFEST_in.write('include ingres_style.css\n')
MANIFEST_in.close()
# rest of the source is handled by distutils.code.Extension
# Extension does not handle include files.

ingresdbi=Extension("ingresdbi",
    sources=glob("dbi/*.c"),
    include_dirs=[ii_system_files,"hdr/"],
    define_macros=defmacros,
    library_dirs=[ii_system_lib],
    libraries=libraries)

setup(
    name="ingresdbi",
    version=dbiversion,
    url="http://ingres.com",
    author="Ralph Loen",
    author_email="Ralph.Loen@ingres.com",
    maintainer="Chris Clark",
    maintainer_email="Chris.Clark@ingres.com",
    license="GPL",
    description="Ingres DBI 2.0 Python Driver",
    long_description="""
ingresdbi is a C based interface to the Ingres_ DBMS server, 
Ingres Star, Enterprise Access Gateways and EDBC servers. The
interface is compliant with Python database API version 2.0 
[PEP-0249].
 
ingresdbi is licensed under the `GPL v2`_ 

.. _Ingres: http://www.ingres.com/
.. _`GPL v2`: http://www.gnu.org/licenses/gpl-2.0.html
.. [PEP-0249] http://www.python.org/peps/pep-0249.html""",
    ext_modules = ([ingresdbi]),
    classifiers=[
        'Classifier: Development Status :: 5 - Production/Stable',
        'Classifier: Environment :: Other Environment',
        'Classifier: License :: OSI Approved :: GNU General Public License (GPL)',
        'Classifier: Operating System :: Microsoft :: Windows :: Windows NT/2000',
        'Classifier: Operating System :: OS Independent',
        'Classifier: Operating System :: POSIX',
        'Classifier: Operating System :: POSIX :: Linux',
        'Classifier: Operating System :: Unix',
        'Classifier: Programming Language :: C',
        'Classifier: Programming Language :: Python',
        'Classifier: Topic :: Database',
        'Classifier: Topic :: Database :: Database Engines/Servers',
        ],
    download_url="http://ingres.com/",
    ),
