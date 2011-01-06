
Ingres Python DBI Driver Version 2.0.1
========================================

.. include:: <isonum.txt>
.. contents::
.. TODO add .. sectnum:: - requires Ingres css cleanup due to dupicate (and out of step) section numbers
     code blocks - take default css for docutils and apply Ingres changes to it piecemeal
     fix up Mac docs, e.g. x86 builds for 32 bit - see forumn
     python 2.4 support is mandatory due to datetime
     copyright symbol work?

--------

Welcome
-------

This README contains all the documentation on the Ingres Python DBI driver.

Please review this README before building or installing this software. We
encourage users to test the software and provide feedback.

--------


New in This Release
~~~~~~~~~~~~~~~~~~~~~

The following changes have been made to the driver since its last release:

-   Raise IOError if the trace file cannot be opened
-   Extend search for odbcinst.ini

See the *CHANGELOG* for a complete listing of all changes.

--------


Operating System Support
------------------------

This Ingres Python DBI driver supports all of the platforms supported by
Ingres, including:

-   Solaris
-   HP-UX
-   AIX
-   Linux
-   Windows
-   SCO UnixWare
-   SCO OpenServer

--------


Installation Considerations
---------------------------

Building the driver
~~~~~~~~~~~~~~~~~~~

To build and install the Ingres Python DBI interface, the following
components are needed:

-   Ingres r3 or above, including Ingres 2006. For a list of binary
    downloads, see http://www.ingres.com. For a source listing if you wish
    to build Ingres from source code, see http://www.ingres.com.
-   Ingres ODBC driver and related header files
-   C compiler (for example, GNU/C or Microsoft Visual Studio)
-   Python interpreter version 2.4 or above
-   The Ingres Python DBI source code

Using the driver
~~~~~~~~~~~~~~~~

-   Ingres r3 or above, including Ingres 2006. For a list of binary
    downloads, see http://www.ingres.com.
-   Ingres ODBC driver
-   Python interpreter version 2.4 or above

--------


General Considerations
------------------------


Features Not Included
~~~~~~~~~~~~~~~~~~~~~~~

The following features are currently not included in the Ingres Open Source
Python DBI driver:

-   Connection pooling (non-Windows only)
-   The following extended Cursor attributes and methods:

    -   messages
    -   lastrowid
    -   scroll

-   Due to the limitations of the Ingres ODBC driver, the following items
    are not supported:

    -   Executing functions asynchronously
    -   Cursor direction other than forward-only
    -   Support for Ingres SQL command "COPY TABLE"
    -   Support for Ingres SQL command "SAVEPOINT"

-   Due to syntax limitations of the Cursor.callproc() method, BYREF and
    output parameters are not supported in stored procedures. Row-returning
    procedures, however, are supported.

--------


Syntax for the ingresdbi.connect() Method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Connection objects are constructed using the ingresdbi.connect() method. The
following keywords are valid:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Keyword
     - Description
   * - dsn
     - the ODBC Data Source Name
   * - database
     - the target database
   * - connectstr
     - alternate connection string
   * - vnode
     - vnode definition as defined in the Ingres netutil utility. For more information, see the `Ingres Connectivity Guide <http://docs.ingres.com/connectivity/>`_
   * - uid
     - user login ID
   * - pwd
     - user password
   * - servertype
     - the type of the target database
   * - trace
     - enables optional tracing of the DBI driver
   * - rolename
     - role name
   * - rolepwd
     - role password
   * - group
     - group
   * - dbms_pwd
     - DBMS password
   * - selectloops
     - fetches using select loops instead of the default cursor loops
   * - autocommit
     - whether autocommit is enabled. Autocommit is off by default
   * - catschemanull
     - whether to disable underscores in wildcard searches
   * - catconnect
     - whether to use separate sessions for catalog operations
   * - numeric_overflow
     - whether to ignore or fail numeric overflows

If the "dsn" keyword is specified, the other keywords are optional. If the
database is specified, the other keywords are optional. If the vnode keyword
is specified, and the connection is local, the value "(LOCAL)" can be used as
the vnode definition, or the vnode attribute can be omitted.

If the "connectstr" keyword is specified, the other keywords are optional.
The "connectstr" keyword specifies an ODBC connection string. For examples of
valid Ingres ODBC connection strings, see the `Ingres Connectivity Guide <http://docs.ingres.com/connectivity/>`_.

All of the above keywords reference string values except for the "trace"
keyword. The "trace" keyword references a tuple with two members. The first
member is the tracing level, which can be a value of 0 through 7. The second
member is a string that describes the trace file. If the second member has a
value of "None", the tracing is written to the standard output.

The following values are valid for the "autocommit", "selectloops",
"catconnect", "catschemanull", and "numeric_overflow" keyword attributes:

-   "on"
-   "off"
-   "y"
-   "n"
-   "Yes"
-   "No"

The following values are valid for the "servertype" keyword:

-   "INGRES"
-   "DCOM"
-   "IDMS"
-   "DB2"
-   "IMS"
-   "ODBC"
-   "VSAM"
-   "RDB"
-   "STAR"
-   "RMS"
-   "ORACLE"
-   "INFORMIX"
-   "SYBASE"
-   "MSSQL"
-   "DB2UDB"

If "INGRES" is not specified, the "servertype" values require access to an
Ingres (that is, Enterprise Access) or EDBC gateway server. Otherwise, no
gateway is required. The default is "INGRES".

Select loops usually have the best performance. However, only one select loop
can be active at a time. Cursor loops support unlimited multiple active
result sets, but can be slower in performance.

Following is an example of a valid instantiation of the ingresdbi connection
object, using all keywords:

::

        conn = ingresdbi.connect(dsn ="myDSN",
                database = "myDB",
                vnode = "(LOCAL)",
                uid = "myUID",
                pwd = "myPWD",
                dbms_pwd = "myDbmsPWD",
                group = "myGroup",
                rolename = "myRoleName",
                rolepwd = "myRolePwd",
                selectloops = "Y",
                autocommit = "Y",
                servertype = "INGRES",
                driver = "Ingres",
                catschemanull = "off",
                catconnect = "Off",
                numeric_overflow = "yes",
                connectStr = "DSN=myDSN",
                trace = (7, "dbi.log")
                )



Connection objects can be constructed without keywords. If keywords are not
used, arguments must follow the order: dsn, database, vnode, uid, pwd,
selectloops, autocommit, servertype, and trace. An example without keywords
is shown here:

::

        conn = ingresdbi.connect(
                "myDSN",
                "myDB",
                "(LOCAL)",
                "MnyUID",
                "myPWD",
                "Y",
                "Yes",
                "INGRES",
                "Ingres 3.0",
                "myRoleName",
                "myrolePWD",
                "myGroup",
                "n",
                "NO",
                "YES",
                "yes",
                "N",
                "myDbmsPwd",
                "DSN=myDSN",
                (7,"dbi.log")
                )



--------


Syntax for the Ingres Extension Cursor.prepared Attribute
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although PEP 249 requires all queries to be prepared, the Ingres DBI driver
does not prepare queries by default. Instead, the Cursor attribute "prepared"
can be deployed.

If Cursor.prepared is set to "y", "yes", or "on", subsequent queries
specified in the Cursor.execute() or Cursor.executemany() methods are
executed as prepared. However, only one query string is allowed for each
cursor instantiation. If the query string is changed, a warning is issued,
and the Ingres DBI driver resorts to direct query execution.

The Cursor.callproc() method raises an exception if the Cursor.prepared
attribute is set to "y", "yes", or "on".

The following values (entered in either uppercase or lowercase) are valid for
the Cursor.prepared attribute:

-   "on"
-   "off"
-   "y"
-   "n"
-   "Yes"
-   "No"

--------


Building and Installing the Ingres Python DBI Driver
------------------------------------------------------


Building the Driver
~~~~~~~~~~~~~~~~~~~~~

The build process has been simplified by the use of the Python `DistUtils package <http://docs.python.org/lib/module-distutils.html>`_. Start the build process by extracting the necessary files from the
Ingres DBI compressed archive:


Mac OS X, Linux and Unix
::::::::::::::::::::::::

1.  Extract the files from ingresdbi-2.0.1.tar.gz: ::

        gzip -cd ingresdbi-2.0.1.tar.gz | tar zvf -

    or if you have GNU tar: ::

        tar zxvf ingresdbi-2.0.1.tar.gz

2.  Enter the newly created source directory, ``ingresdbi-2.0.1``: ::

        cd ingresdbi-2.0.1

3.  Initiate the build process.
    NOTE this requires the python devel packages to be installed ::

        env LANG=c python setup.py build --force

4.  Optional, run the test suite: ::

        env LANG=c python tests/test_ingresdbi_dbapi20.py
        env LANG=c python setup.py test


Windows
:::::::

-   Use WinZip (or similar product) to extract the directories and files
    from ingresdbi-2.0.1.zip.

    -   Cygwin users can use either of the commands for *Mac OS X, Linux and Unix*

-   Enter the newly created source directory, ``ingresdbi-2.0.1``: ::

            cd ingresdbi-2.0.1

-   Initiate the build process. ::

            python setup.py build --force


**Note:** You can skip this and jump straight to the install process as this
will automatically build.

--------


Installing the Driver
~~~~~~~~~~~~~~~~~~~~~~~

As with the build process, the installation process makes use of DistUtils.
By default, the Ingres Python DBI driver is installed into Python's site-
packages directory. The ability to provide alternate installation locations
has not been investigated at this time. The only requirement for installing
is to be able to write to the site-packages directory.

To install, execute the following command:

::

        python setup.py install


To create a deliverable source package, execute the following command:

::

        python setup.py sdist


--------


Example Code
------------

The following code provides a simple Python database example using the Ingres
Python DBI driver:

::

    import ingresdbi
    import pprint
    """
    import os
    username=os.getenv('test_username')
    password=os.getenv('test_password')
    vnode=os.getenv('test_vnode')
    database=os.getenv('test_database')
    trace=None
    """
    enable_trace = 0
    if enable_trace == 1:
        trace=(7, None)
    else:
        trace=(0, None)
    database='iidbdb'
    vnode='(local)'
    prog_str = 'DEMO SIMPLE SELECT'
    print prog_str, "connecting to database: " + database
    dc=ingresdbi.connect(database=database, vnode=vnode,
    trace=trace)
    print prog_str, "Creating new cursor()"
    c=dc.cursor()
    print prog_str, "About to call cursor.execute()"
    c.execute("select * from iidbconstants")
    print "cursor.description = "
    description = c.description
    pprint.pprint (description )
    print prog_str, "cursor.fetchall()"
    rows = c.fetchall()
    print "rows = ", rows
    row_count = 0
    for row in rows:
        row_count = row_count + 1
    print 'Row #', row_count
    count = 0
    for column in row:
        print description[count][0] , ': ', column
        count = count + 1
    print "-----------------------------"
    print prog_str, "connection.commit()"
    dc.commit()
    print prog_str, "connection.close()"
    dc.close()


--------


Known Issues
------------

Generic Issues
~~~~~~~~~~~~~~~~

-   There is no support for installing to an alternate directory other
    than the default site-packages.

Windows Issues
~~~~~~~~~~~~~~~~

-   **Python 2.3 only** - ``python setup.py build`` may fail with: ::

            error: Python was built with version 6 of Visual Studio,
            and extensions need to be built with the same version of the
            compiler, but it isn't installed.


    If you have Visual Studio version 6 installed, run Microsoft Visual
    C++ 6.0 ``MSDEV.EXE`` (the GUI), quit out, and then retry the build. For
    further information, see Python mailing list
    http://mail.python.org/pipermail/python-dev/2003-November/040478.html

--------


Support
-------

Support for the Python Ingres DBI interface is available from a number of
different sources. Users with a support contract can either raise an issue
via https://servicedesk.ingres.com/ or through their local technical
support department. See http://ingres.com/support for a complete list of
locations, primary service hours, and telephone numbers. Users without a
support contract can post questions or problems to the Ingres Community
Forums (http://community.ingres.com/forum/ or to comp.databases.ingres
(http://groups.google.com/group/comp.databases.ingres)

--------

Copyright |copy| 2008 Ingres Corporation. All rights reserved.
