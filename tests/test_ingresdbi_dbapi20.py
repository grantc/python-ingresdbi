#!/usr/bin/env python
# -*- coding: ascii -*-
# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
"""
 Test script for the Ingres Python DBI based on test_psycopg_dbapi20.py.
 Uses dbapi20.py version 1.10 obtained from
 http://stuartbishop.net/Software/DBAPI20TestSuite/
 TODO check out http://code.google.com/p/acute-dbapi/

 History:
    02-May-2008 (grant.croker@ingres.com)
        Created
    06-May-2008
        Added the detection/creation of the procedure 'lower' to setUp
    05-Aug-2009 (Chris.Clark@ingres.com)
        Tests all failed on new machine with no arguments.
        Create database had bad argument order (failed with Ingres 9.1.1, 9.2).
        The name of the database to be created is now picked up from arguments.
        New database is created with Unicode support, NFC normalization.
        Many tests created warnings about access to attributes:, e.g.:
            Warning: DB-API extension connection.Warning used
            Warning: DB-API extension connection.Error used
    06-Aug-2009 (Chris.Clark@ingres.com)
        Added template for easy copy/paste of new tests.
        Added bug script for Ingres Bugs bug# 121611 Trac ticket:403
         - Fatal Python error: deallocating None
        Added bug script for Trac ticket:255 - embedded nulls are lost
        Added (not yet running/enabled) bug script for Trac ticket:260
         - microsecs on selects of (ansi) timestamps are incorrect
    18-Aug-2009 (Chris.Clark@ingres.com)
        Replaced some assert calls that made equality checks with real
        calls to AssertEqual.
        Added code to set the current working directory to the test directory
        this allows the test suite to be run from any path as some tests use
        data files that are included with the test scripts.
        Added warnings about tests that are not ran.
        Added new tests:
            test_decimalLongValues
            test_decimalBindLongValues
            test_decimalCrash
            test_SimpleUnicodeSelect
            NOtest_SimpleUnicodeBind - disabled
    19-Aug-2009 (Chris.Clark@ingres.com)
        Fixed all of the bare excepts.
        Cleaned up createdb failure code.
        Cleaned up procedure creation/check code.
        Added Unicode check.
    19-Aug-2009 (Chris.Clark@ingres.com)
        Tracing information was not being passed to the driver correctly.
    20-Aug-2009 (Chris.Clark@ingres.com)
        Added JDBC/Jython support. Requires jython2.5.0 (older releases
        of Jython have not been tested).
        Added logic to drop table to handle JDBC.
        Cleaned up sqlstate check for IngresDBI in drop table.
        Cleaned up spurious trace setting line in pooled check.
        Added setUpOnce() function to make clear what is single
        setup for the whole suite.
        Removed semi-colons ";" from SQL, DBMS/JDBC-driver does not like it.
    01-Sep-2009 (Chris.Clark@ingres.com)
        test_bug_Embedded_Nulls split into two seperate tests, one for 
        VARCHAR type the other for NVARCHAR (Unicode), new tests:
            test_bug_Embedded_NullsVarchar
            test_bug_Embedded_NullsUnicode
        New tests based on test_bug_Embedded_Nulls that is for bind parameters:
            test_bug_Embedded_BindNullsVarcharNonPrepared -- fails
            test_bug_Embedded_BindNullsVarchar
            test_bug_Embedded_BindNullsUnicode
            test_bug_Embedded_BindNullsVarcharInsert
            test_bug_Embedded_BindNullsUnicodeInsert
        test_bug_Embedded_BindNullsVarcharNonPrepared is based on 
        test_bug_Embedded_BindNullsVarchar and fails presently, not sure if
        this is an IngresDBI bug or an ODBC bug.
        Fixed "bad" test NOtest_SimpleUnicodeBind, it has now been renamed to 
        test_SimpleUnicodeBind and is now part of all test runs.
        Added new tests that do not run that show problems/hangs with/in 
        test_cursorIter when certain kinds of errors occur in previous tests.
            NOtest_anotherPreCursIter
            NOtest_iterhang_presetup
        Warnings that list tests that do not run by default are now in sorted
        order.
        Updated test_stringUnicodeCharLongValues with possible DBMS problems information.
        Check for dbproc called lower was not checking the schema of the 
        current user which leed to dbproc failures if different users used 
        the same database for testing.
    15-Sep-2009 (clach04)
        New test test_BindNullNone for Trac ticket 417. Simply None/NULL test.
        New test test_BindNullNoneTypes - more advanced test that checks
        multiple types at destination.
        Added support for drop temporary table.
        Drop table function update for JDBC so ignore logic can be 
        shared for IngresDBI and (Ingres) JDBC.
    12-Oct-2009 (clach04)
        New test test_FetchAfterClose for Trac ticket 445
        New test test_ExecuteWithEmptyParams for Trac ticket 446
        Don't change current directory if path of test is the current directory. 
    24-Nov-2009 (clach04)
        New test test_bugSelectLongNvarchar for Trac ticket 502
        New test NOtest_test_bugSelectLongNvarcharHang for iter hangs
    10-Dec-2010 (grant)
        Replaced the test_bugSelectLob with test_bugNonSelectFetch for 
        Trac ticket 161
    10-Dec-2010 (grant)
        Trac ticket 702 - Fix up tearDown() so it actually cleans up the 
        cursor/connection after each test, preventing hangs.
"""
import dbapi20
import unittest
import popen2
import re
import os
import sys
import warnings
import datetime
from decimal import Decimal
import gc # CPython specific.....

#poor but simple test
jython_runtime_detected=hasattr(sys, 'JYTHON_JAR') or str(copyright).find('Jython') > 0

if jython_runtime_detected:
    from com.ziclix.python.sql import zxJDBC as driver
    """
    JDBC needs a few things setup, minimum:
        Windows
            set CLASSPATH=%II_SYSTEM%\ingres\lib\iijdbc.jar
            set test_password=mypassword(operating system password for Ingres user)
            set test_port=R27
        Unix
            setenv CLASSPATH ${II_SYSTEM}/ingres/lib/iijdbc.jar
            export CLASSPATH=${II_SYSTEM}/ingres/lib/iijdbc.jar
            .....
    """
else:
    import ingresdbi as driver

"""

 By default the test expects a locally accessible database called
 dbapi20_test. This, along with other settings, can be overridden
 using the following OS environment variables:

    test_dsn - ODBC data source name
    test_username - user to connect as
    test_password - password for the above user
    test_vnode - Ingres/NET virtual node alias (use netutil to 
        define)
    test_database - database name, defaults to dbapi20_test if not 
        specified
    test_pooled - *Windows only*, use ODBC pooling, Boolean 
        (true, on, false, off)
    test_trace - Level of trace output 0-7, default 0
    test_trace_file - where to put trace output, if not set output 
        goes to STDERR

 Windows users use:
    set VARNAME=value
 for example
    set test_database=pytestdb
 Linux/UNIX/Mac OS X use:
    VARNAME=value; export VARNAME # sh,ksh,bash   Bourne shell variants
    setenv VARNAME=value          # csh, tcsh     C-shell variants

 for example
    test_database=pytestdb; export test_database  # sh,ksh,bash
    setenv test_database pytestdb                 # csh, tcsh 

"""
dsn=os.getenv('test_dsn')
username=os.getenv('test_username')
password=os.getenv('test_password')
vnode=os.getenv('test_vnode')
database=os.getenv('test_database')
if database is None:
    database='dbapi20_test'
pooled=os.getenv('test_pooled')  # Windows only
traceLevel_str=os.getenv('test_trace')
if traceLevel_str != None:
    traceLevel = int(traceLevel_str)
else:
    traceLevel = 0
traceFile=os.getenv('test_trace_file')
if jython_runtime_detected:
    hostname=os.getenv('test_hostname')
    if not hostname:
        hostname='localhost'
    port=os.getenv('test_port')
    if not port:
        port='II7'
    if not username:
        # JDBC needs a user name, either default to current user
        # or default to named user
        username='ingres'

# Not sure about this!!!!
warnings.warn('Some tests are not being ran at the moment due to hangs (after they complete)')
# list of tests to check obtained from:
#   grep NOtest tests/test_ingresdbi_dbapi20.py  |grep def |sed  's/(self)://g'|sed  's/def//g' |sort -u
warnings.warn('Hang after: NOtest_anotherPreCursIter')
warnings.warn('Hang after: NOtest_bugTimestampMicrosecs')
warnings.warn('Hang after: NOtest_iterhang_presetup')
warnings.warn('Hang after: NOtest_test_bugSelectLongNvarcharHang')
warnings.warn('FIXME Filtering of warnings enabled! This should be checked.')
warnings.simplefilter('ignore', Warning)
## We have an odd behavior, e.g. accessing connection.Warning, see dbapi20.py:210. pysqlite2 does not have this
## oddly, setting warnings to ERRORS, silences Ingres but not sqlite!

def dropTable(c, tablename):
    """Simple drop table wrapper that ignores "table does not exist" errors. Warning: Not sql injection safe.
        c = cursor
        tablename = string containing name only

    Does not handle quote names (unless they are already quoted).
    
DataError: (3, 2753, '42500', "[Ingres][NGRES ODBC Driver][NGRES]DROP: 'myblob' does not exist or is not owned by you.", 'drop table myBlob')
    """
    try:
        sql_string = 'drop table %s' % tablename
        c.execute(sql_string)
    except driver.Error, info:
        # now follows some slightly hairy sqlstate check code
        # this should be fairly straight forward but the information
        # is not in an easy to use format
        if jython_runtime_detected:
            sqlstate_str=info.message[info.message.rfind('['):]
            if not sqlstate_str.startswith('[SQLState: '):
                raise
            sqlstate=sqlstate_str[len('[SQLState: '):]
            # Lose ending ']'
            sqlstate=sqlstate[:-1]
        else:
            # ingresdbi
            sqlstate=info.args[2]
        # 42500 - regular table
        # 42503 - temp table
        if sqlstate not in ['42500', '42503']:
            raise

globalsetUpOnceFlag=False
class test_Ingresdbi(dbapi20.DatabaseAPI20Test):
    driver = driver
    connect_args = ()
    connect_kw_args = {}
    
    if jython_runtime_detected:
        ingres_driver_class='com.ingres.jdbc.IngresDriver'
        ingres_driver_uri='ingres'
        d, u, p, v = "jdbc:%s://%s:%s/%s/;select_loop=on"%(ingres_driver_uri, hostname, port, database), username, password, ingres_driver_class
        connect_args += (d, u, p, v)
    else:
        if dsn != None:
            connect_kw_args.setdefault('dsn',dsn)
        if username != None:
            connect_kw_args.setdefault('uid',username)
        if password != None:
            connect_kw_args.setdefault('pwd',password)
        if vnode != None:
            connect_kw_args.setdefault('vnode',vnode)
        connect_kw_args.setdefault('database',database)
        if pooled != None:
            connect_kw_args.setdefault('pooled', pooled)
        trace=(traceLevel, traceFile)
        connect_kw_args.setdefault('trace',trace)
    
    table_prefix = 'dbapi20test_' # If you need to specify a prefix for tables

    lower_func = 'lower' # For stored procedure test

    def setUpOnce(self):
        """Custom, one shot custom setup issued only once for the entire batch of tests
        NOTE actually runs for every test if it fails......
        """
        global globalsetUpOnceFlag
        if globalsetUpOnceFlag:
            return
        
        if jython_runtime_detected:
            dbname = database # not sure about this
        else:
            dbname = self.connect_kw_args['database']
        try:
            con = self._connect()
        except self.driver.DataError:
            cmd = "createdb -i %s -f nofeclients" % dbname
            cout,cin = popen2.popen2(cmd)
            cin.close()
            createdb_output=cout.read()
            if 'E_' in createdb_output:
                self.fail('createdb failed\n'+createdb_output)
            # retry connect
            con = self._connect()

        # Do we have a procedure called lower? 
        cur=con.cursor()
        cur.execute("select procedure_name from iiprocedures where procedure_name = '%s' and procedure_owner = USER"%self.lower_func)
        if len(cur.fetchall()) == 0:
            cur.execute("""create procedure %s( a varchar(20) not null)
                            result row (varchar(20))
                            as 
                                declare x=varchar(20) not null; 
                            begin 
                                select lower(a) into x; 
                                return row(x); 
                            end"""%self.lower_func)
            con.commit()

        # check we have the expected type of Unicode support.
        sql_query = "select DBMSINFO('UNICODE_NORMALIZATION') from iidbconstants"
        cur.execute(sql_query)
        rs = cur.fetchone()
        self.assertEqual(rs[0], 'NFC', 'Test database "%s" needs to use NFC UNICODE_NORMALIZATION (i.e. "createdb -i ...")' % dbname) # this probably should be made more obvious in the error output!
        con.close()
        # set complete flag AFTER everything has been done successfully
        globalsetUpOnceFlag=True
    
    def setUp(self):
        # NOTE using Python unittest, setUp() is called before EACH and every
        # test. There is no single setup routine hook (other than hacking init,
        # module main, etc.). Setup is done here in case an external test
        # suite runner is used (e.g. nose, py.test, etc.).
        
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self) 
        
        # ensure intial setup complete
        self.setUpOnce()
        
        # end of setUp() there is no per test setup required.

    def tearDown(self):
        if hasattr(self, "cur"):
            try:
                self.cur.close()
            except (self.driver.Error):
                pass
            del(self.cur)
        if hasattr(self, "con"):
            try:
                self.con.close()
            except (self.driver.Error):
                pass
            del(self.con)

    def test_nextset(self): pass
    def test_setoutputsize(self): pass

    """ 
    Additional Ingres DBI tests 

    For the time being these are kept out of dbapi20.py so as not to pollute
    the test suite.
    """

    ddl3 = 'create table %sblob1 (name long varchar)' % table_prefix
    ddl4 = 'create table %sblob2 (name long byte)' % table_prefix
    ddl5 = 'create table %stdata (colint1 integer, colbigint2 bigint, colfloat3 float, colchar4 char (20), colvarchar5 varchar(20))' % table_prefix

    xddl3 = 'drop table %sblob1' % table_prefix
    xddl4 = 'drop table %sblob2' % table_prefix
    xddl5 = 'drop table %stdata' % table_prefix

    def executeDDL3(self,cursor):
        cursor.execute(self.ddl3)

    def executeDDL4(self,cursor):
        cursor.execute(self.ddl4)

    def executeDDL5(self,cursor):
        cursor.execute(self.ddl5)

    def test_smallblob(self):
        blob = "123456"
        con = self._connect()
        try:
             cur = con.cursor()
             self.executeDDL3(cur)
             cur.execute("insert into %sblob1 values (?)" % 
                 self.table_prefix, (blob,) )
             cur.execute("select * from %sblob1  " % 
                 self.table_prefix, (blob,) )
             rs = cur.fetchall()
             for x in range (0, len(blob)):
                 self.assertEqual(blob[x],rs[0][0][x])
                  
        finally:
            cur.close()
            con.close()

    def test_4Kblob(self):
        blob = "1234567890" * 400
        con = self._connect()
        try:
             cur = con.cursor()
             self.executeDDL3(cur)
             cur.execute("insert into %sblob1 values (?)" % 
                 self.table_prefix, (blob,) )
             cur.execute("select * from %sblob1  " % 
                 self.table_prefix, (blob,) )
             rs = cur.fetchall()
             for x in range (0, len(blob)):
                 self.assertEqual(blob[x],rs[0][0][x])

        finally:
            cur.close()
            con.close()
            del(blob)


    def test_40Kblob(self):
        blob = self.driver.Binary("1234567890" * 4000)
        con = self._connect()
        try:
             cur = con.cursor()
             self.executeDDL4(cur)
             cur.execute("insert into %sblob2 values (?)" % 
                 self.table_prefix, (blob,) )
             cur.execute("select * from %sblob2  " % 
                 self.table_prefix, (blob,) )
             rs = cur.fetchall()
             for x in range (0, len(blob)):
                 self.assertEqual(blob[x],rs[0][0][x])

        finally:
            cur.close()
            con.close()
            del(blob)


    def test_40K2R(self):
        blob = self.driver.Binary("1234567890" * 4000)
        con = self._connect()
        try:
             cur = con.cursor()
             self.executeDDL4(cur)
             cur.execute("insert into %sblob2 values (?)" % 
                 self.table_prefix, (blob,) )
             cur.execute("insert into %sblob2 values (?)" % 
                 self.table_prefix, (blob,) )
             cur.execute("select * from %sblob2  " % 
                 self.table_prefix, (blob,) )
             rs = cur.fetchall()
             for x in range (0, len(blob)):
                 self.assertEqual(blob[x],rs[0][0][x])
             for x in range (0, len(blob)):
                 self.assertEqual(blob[x],rs[1][0][x])

        finally:
            cur.close()
            con.close()
            del(blob)

    def test_data(self):
        fobj = file("data.txt","r")
        list = fobj.readlines()
        fobj.close()

        eog = False
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL5(cur)
            for x in range (0, len(list)):
                tdata = list[x].rstrip('\n')
                if (tdata.startswith("INT ")):
                    intdata = int(tdata.lstrip("INT "))
                elif (tdata.startswith("LONG ")):
                    longdata = long(tdata.lstrip("LONG "))
                elif (tdata.startswith("FLOAT ")):
                    floatdata = float(tdata.lstrip("FLOAT "))
                elif (tdata.startswith("CHAR ")):
                    chardata = (tdata.lstrip("CHAR "))
                elif (tdata.startswith("VARCHAR ")):
                    varchardata = (tdata.lstrip("VARCHAR "))
                    eog = True
                if (eog == True):
                    eog = False
                    cur.execute("insert into %stdata values (?, ?, ?, ?, ?)" % 
                        self.table_prefix, (intdata, longdata, floatdata,
                        chardata, varchardata))
            cur.execute("select * from %stdata" % self.table_prefix) 
            rs = cur.fetchall()
            for y in range(0, len(rs)):
                for x in range (0, 5):
                    tdata = list[x+(5*y)].rstrip('\n')
                    if (tdata.startswith("INT ")):
                        intdata = int(tdata.lstrip("INT "))
                        self.assertEqual(intdata, rs[y][0])
                    elif (tdata.startswith("LONG ")):
                        longdata = long(tdata.lstrip("LONG "))
                        self.assertEqual(longdata, rs[y][1])
                    elif (tdata.startswith("FLOAT ")):
                        floatdata = float(tdata.lstrip("FLOAT "))
                        self.assertEqual(floatdata, rs[y][2])
                    elif (tdata.startswith("CHAR ")):
                        chardata = (tdata.lstrip("CHAR "))
                        m = re.search(chardata, rs[y][3])
                        substr = m.group()
                        self.assertEqual(chardata, substr)
                    elif (tdata.startswith("VARCHAR ")):
                        varchardata = (tdata.lstrip("VARCHAR "))
                        self.assertEqual(varchardata, rs[y][4])
        finally:
            cur.close()
            con.close()

    def test_connectionAttributes(self):
        kw_args = self.connect_kw_args.copy()

        kw_args.setdefault('vnode', '(LOCAL)')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('servertype', 'Ingres')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('driver', 'Ingres')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('selectloops', 'on')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('rolename', 'nullrole')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('rolepwd', 'nullpwd')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('group', 'nullgroup')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('numeric_overflow', 'N')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()

        kw_args.setdefault('dbms_pwd', 'nulldbmspwd')
        self.con = self.driver.connect( *self.connect_args, **kw_args)
        self.con.close()


    def test_cursorPreparedAttribute(self):
        self.con = self._connect()
        try:
            self.curs = self.con.cursor()
            self.curs.prepared = "Yes"
            self.curs.execute("select * from iitables")
            l = self.curs.fetchall()
            self.failIfEqual(len(l), 0) 
        finally:
            self.curs.close()
            self.con.close()

    def test_cursorMessages(self):
        self.con = self._connect()
        try:
            self.curs = self.con.cursor()
            self.curs.prepared = "Yes"
            self.curs.execute("select * from iitables")
            self.curs.execute("select * from iidbcapabilities")
            self.failIfEqual(len(self.curs.messages), 0)
        finally:
            self.curs.close()
            self.con.close()

    def test_connectionMessages(self):
        self.con = self._connect()
        self.con.close()
        try:
            self.con.close()
        except self.driver.Error:
            pass
        self.failIfEqual(len(self.con.messages), 0)

    def test_connectionErrorHandler(self):
        def connHandler(connection, cursor, exception, msg):
            self.failUnless(connection is self.con)
            self.failUnless(cursor == None)
            self.failIf(exception == None)
            self.failIf(len(msg) == 0)
        self.con = self._connect()
        self.con.errorhandler = connHandler
        self.con.close()
        self.con.close()

    def test_cursorConnectionAttribute(self):
        self.con = self._connect()
        try:
            self.curs = self.con.cursor()
            self.failUnless(hasattr(self.curs,'connection'))
            self.failUnlessEqual(self.curs.connection, self.con)
        finally:
            self.curs.close()
            self.con.close() 

    def test_cursorErrorHandler(self):
        def handler(connection, cursor, exception, msg):
            self.failUnless(connection is self.con)
            self.failUnless(cursor == self.curs)
            self.failIf(exception == None)
            self.failIf(len(msg) == 0)
        self.con = self._connect()
        try:
            self.curs = self.con.cursor()
            self.curs.errorhandler = handler
            self.curs.execute("select * from xiitables")
        finally:
            self.curs.close()
            self.con.close()
    
    def test_cursorIter(self):
        self.con = self._connect()
        try:
            self.curs = self.con.cursor()
            self.curs.execute("select * from iitables")
            for i in self.curs:
                self.failIfEqual(i, None)
                self.failIfEqual(len(i[0]), 0)
        finally:    
            self.curs.close()
            self.con.close()

    def test_cursorScroll(self):
        # Not supported until scrollable cursors are implemented
        self.con = self._connect()
        self.curs = self.con.cursor()
        self.curs.execute("select * from iitables")
        try:
            self.curs.scroll(1)
        except self.driver.NotSupportedError:
            pass
        self.curs.close()
        self.con.close()

    def test_cursorLastRowId(self):
        # Not supported if or until row ID's are implemented
        self.con = self._connect()
        self.curs = self.con.cursor()
        self.curs.execute("select * from iitables")
        try:
            rowid = self.curs.lastrowid
        except self.driver.NotSupportedError:
            pass
        self.curs.close()
        self.con.close()

        """ End of Ingres custom tests """
    
    ###############################################################
    # Start of Ingres bug/regression tests

    def test_bugPyNoneTooManyFrees(self):
        """Trac ticket:403 Bug 121611 - PyNone multiple (too many) frees/decrefs, opposite of a memory-leak.
        Error from Python interpreter:
            Fatal Python error: deallocating None
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        # type is not really that important (all types appear to free too many times), using ints to have a specific type to test
        sql_query = "select int4(NULL), int4(NULL), int4(NULL), int4(NULL), int4(NULL), int4(NULL), char(NULL), varchar(NULL), date(NULL), float4(NULL),  float8(NULL), decimal(NULL, 30, 5) from iidbconstants"
        # 'note ref count dips below original/start'
        number_of_selects=1 ## this won't crash but is enough to see None refcount go down
        number_of_selects=200 ## this should cause the crash
        pynone_count_fuzzy=2 # not yet sure what a reasonably fuzz factor is, ideally zero
        gc.collect() # force garbarge collection, may not be needed. Be Safe.
        count_pre_loop=sys.getrefcount(None)
        for x in range(number_of_selects):
            count_pre_execute=sys.getrefcount(None)
            self.curs.execute(sql_query)
            count_pre_fetchall=sys.getrefcount(None)
            rs = self.curs.fetchall()
            count_post_fetchall=sys.getrefcount(None)
            del rs
            gc.collect()
            count_post_delfetchall=sys.getrefcount(None)
            self.assert_(pynone_count_fuzzy >= count_pre_execute-count_post_delfetchall, 'appear to have a PyNone refcount problem %r >= %r - %r' % (pynone_count_fuzzy, count_pre_execute, count_post_delfetchall))
        gc.collect()
        count_post_loop=sys.getrefcount(None)
        self.assert_(pynone_count_fuzzy >= count_pre_loop-count_post_loop, 'appear to have a PyNone refcount problem %r >= %r - %r' % (pynone_count_fuzzy, count_pre_execute, count_post_delfetchall))
        
        self.curs.close()
        self.con.close()

    def test_bugPyNoneTooManyFreesDataBaseProcedure(self):
        """dbproc version of Trac ticket:403 Bug 121611 - PyNone multiple (too many) frees/decrefs, opposite of a memory-leak.
        Error from Python interpreter:
            Fatal Python error: deallocating None
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        # type is not really that important (all types appear to free too many times), using ints to have a specific type to test
        # NOTE this is a non-row returning dbproc
        sql_query = '''create procedure dbp_ingresdbi_test_null (a integer, b integer, c integer, d integer, e integer, f integer, g char(10), h varchar(10), i date /* ingresdate */, j float4, k float8, l decimal(30, 5)) as
            begin
                a = NULL;
                b = NULL;
                c = NULL;
                d = NULL;
                e = NULL;
                f = NULL;
                g = NULL;
                h = NULL;
                i = NULL;
                j = NULL;
                k = NULL;
                l = NULL;
            end;
        '''
        self.curs.execute(sql_query)
        bind_params = (None, None, None, None, None, None, None, None, None, None, None, None);
        # 'note ref count dips below original/start'
        number_of_executes=1 ## this won't crash but is enough to see None refcount go down
        number_of_executes=200 ## this should cause the crash
        pynone_count_fuzzy=2 # not yet sure what a reasonably fuzz factor is, ideally zero
        gc.collect() # force garbarge collection, may not be needed. Be Safe.
        count_pre_loop=sys.getrefcount(None)
        for x in range(number_of_executes):
            count_pre_execute=sys.getrefcount(None)
            dbproc_result = self.curs.callproc('dbp_ingresdbi_test_null', bind_params)
            count_pre_fetchall=sys.getrefcount(None)
            #rs = self.curs.fetchall()  # this isn't needed (and causes InterfaceError) (as dbproc is not row/table returning) but may be a useful test addition/excercise
            count_post_fetchall=sys.getrefcount(None)
            #del rs
            del dbproc_result
            gc.collect()
            count_post_delfetchall=sys.getrefcount(None)
            self.assert_(pynone_count_fuzzy >= count_pre_execute-count_post_delfetchall, 'appear to have a PyNone refcount problem %r >= %r - %r' % (pynone_count_fuzzy, count_pre_execute, count_post_delfetchall))
        gc.collect()
        count_post_loop=sys.getrefcount(None)
        self.assert_(pynone_count_fuzzy >= count_pre_loop-count_post_loop, 'appear to have a PyNone refcount problem %r >= %r - %r' % (pynone_count_fuzzy, count_pre_execute, count_post_delfetchall))
        
        self.curs.close()
        self.con.close()

    def test_bug_Embedded_NullsVarchar(self):
        """Trac ticket:255 - embedded nulls in varchar type are lost
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        sql_query="""select
    length(varchar(U&'x\\0000y')) as length_singlebyte,
    varchar(U&'x\\0000y') as hex_singlebyte
    from iidbconstants
        """ # NOTE need 9.2 or later (problems with older/pre-patched versions, e.g. 9.1.2 does NOT work)
        #expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[1], expected_value)
        self.assertEqual(rs[0], len(rs[1]))

        self.curs.close()
        self.con.close()
    
    def test_bug_Embedded_NullsUnicode(self):
        """Trac ticket:255 - embedded nulls In Unicode are lost
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        sql_query="""select
    length(U&'x\0000y') as length_unicode,
    U&'x\0000y' as hex_unicode
    from iidbconstants
        """ # NOTE need 9.2 or later (problems with older/pre-patched versions)
        sql_query="""select 
                    length(nvarchar(U&'x\\0000y')) as length_unicode,
                    nvarchar(U&'x\\0000y') as hex_unicode
                    from iidbconstants;""" # NOTE need 9.2 or later (problems with older/pre-patched versions)
        #expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[1], expected_value)
        self.assertEqual(rs[0], len(rs[1]))

        self.curs.close()
        self.con.close()


    def test_bug_Embedded_BindNullsVarcharNonPrepared(self):
        """Trac ticket:441 (DBMS ServiceDesk issue 146002)- Identical to test_bug_Embedded_BindNullsVarchar but does not use prepared cursors.
        This is a DBMS bug (_may_ be an ODBC bug), if self.curs.prepared='y' is set, no problem reported. See Service Desk issue 146002.
        As of 2009-09-01 fails with error:
            Syntax error on 'using'.  The correct syntax is:
             OPEN CURSORID CURSOR FOR fullselect
               [FOR [DIRECT | DEFERRED] UPDATE OF column {, ... }]
               [FOR READONLY]
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        self.curs.prepared = "No" # explictly turn it off

        sql_query="""select
    length(varchar(?)) as length_singlebyte,
    varchar(?) as hex_singlebyte
    from iidbconstants
        """
        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        self.curs.execute(sql_query, (expected_value, expected_value))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[1], expected_value)
        self.assertEqual(rs[0], len(rs[1]))

        self.curs.close()
        self.con.close()

    def test_bug_Embedded_BindNullsVarchar(self):
        """embedded bind params nulls in varchar type are lost
        This _may_ be an ODBC bug, if self.curs.prepared='y' is set, no problem reported
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        self.curs.prepared = "Yes" # FIXME remove this! this is a hack workaround. Not sure if this is an ODBC or IngresDBI bug

        sql_query="""select
    length(varchar(?)) as length_singlebyte,
    varchar(?) as hex_singlebyte
    from iidbconstants
        """
        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        self.curs.execute(sql_query, (expected_value, expected_value))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[1], expected_value)
        self.assertEqual(rs[0], len(rs[1]))

        self.curs.close()
        self.con.close()

    def test_bug_Embedded_BindNullsUnicode(self):
        """bind params, select Unicode strings with embedded nulls from nvarchar
embedded bind params nulls In Unicode are lost
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        self.curs.prepared = "Yes" # FIXME remove this! this is a hack workaround. Not sure if this is an ODBC or IngresDBI bug

        sql_query="""select
                    length(nvarchar(?)) as length_unicode,
                    nvarchar(?) as hex_unicode
                    from iidbconstants;"""
        expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        self.curs.execute(sql_query, (expected_value, expected_value))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[1], expected_value)
        self.assertEqual(rs[0], len(rs[1]))

        self.curs.close()
        self.con.close()

    def test_bug_Embedded_BindNullsVarcharInsert(self):
        """bind params, insert embedded nulls in varchar
        Basically the same as test_bug_Embedded_BindNullsVarchar but uses a table
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        table_name='nullVarchar'
        
        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 varchar(9))" % table_name)
        self.curs.execute("insert into %s (col1, col2) values (1, ?)" % table_name, (expected_value,))
        self.con.commit()
        
        self.curs.execute("select col2 from %s order by col1" % table_name)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        # clean up
        dropTable(self.curs, table_name)
        self.con.commit()

        self.curs.close()
        self.con.close()
    
    def test_bug_Embedded_BindNullsUnicodeInsert(self):
        """bind params, insert Unicode string with embedded nulls in nvarchar
        Basically the same as test_bug_Embedded_BindNullsUnicode but uses a table
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        table_name='nullNVarchar'
        
        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 varchar(9))" % table_name)
        self.curs.execute("insert into %s (col1, col2) values (1, ?)" % table_name, (expected_value,))
        self.con.commit()
        
        self.curs.execute("select col2 from %s order by col1" % table_name)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        # clean up
        dropTable(self.curs, table_name)
        self.con.commit()

        self.curs.close()
        self.con.close()

    def NOtest_iterhang_presetup(self):
        """Basically a bad test version of test_bug_Embedded_BindNullsUnicodeInsert
        (i.e. has logic errors, inserted Unicode (count be ascii with varchar)
        into numeric column and then selecting numeric column and comparing with
        Unicode. Causes test_cursorIter to hang if this test runs first!

            -v test_Ingresdbi.NOtest_iterhang_presetup test_Ingresdbi.test_cursorIter
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        table_name='nullNVarchar'

        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 varchar(9))" % table_name)
        self.curs.execute("insert into %s (col1) values (?)" % table_name, (expected_value,))
        self.con.commit()

        self.curs.execute("select col1 from %s order by col1" % table_name)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        # clean up
        dropTable(self.curs, table_name)
        self.con.commit()

        self.curs.close()
        self.con.close()

    
    ## FIXME needs to be enabled (and debugged) along with test_cursorIter (which hangs when this test is enabled with r1834)
    def NOtest_bugTimestampMicrosecs(self):
        """Trac ticket:260 - microsecs on selects of (ansi) timestamps are incorrect
        WARNING having this test present appears to hang the test suite
        at test test_cursorIter :-( e.g.:

            ./test_ingresdbi_dbapi20.py -v test_Ingresdbi.NOtest_bugTimestampMicrosecs test_Ingresdbi.test_cursorIter

        Will run this test first and then test_cursorIter which hangs and never completes.

        but runs OK standalone:

            ./test_ingresdbi_dbapi20.py test_Ingresdbi.NOtest_bugTimestampMicrosecs

        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        expected_value=datetime.datetime(2008, 9, 18, 12, 42, 45, 899000)
        sql_query = "select expire_date, varchar(expire_date) from pytimestampbug"
        dropTable(self.curs, 'pytimestampbug')
        self.curs.execute("create table pytimestampbug(expire_date timestamp)")

        # insert bind param test case
        self.curs.execute("delete from pytimestampbug")
        self.curs.execute("insert into pytimestampbug(expire_date) values (?)", (expected_value,))
        self.curs.execute(sql_query)
        row=self.curs.fetchone()
        dateval = row[0]
        #dateval = expected_value # dumb test for debugging, if this is uncommented test_cursorIter() completes
        self.assertEqual(expected_value, dateval)
        
        # Original bug test case
        self.curs.execute("delete from pytimestampbug")
        self.curs.execute("insert into pytimestampbug(expire_date) values ('2008-09-18 12:42:45.899000')")
        self.curs.execute(sql_query)
        row=self.curs.fetchone()
        dateval = row[0]
        #dateval = expected_value # dumb test for debugging, if this is uncommented test_cursorIter() completes
        self.assertEqual(expected_value, dateval)
        
        self.curs.close()
        self.con.close()
    
    def test_decimalLongValues(self):
        """Decimal type, sanity check for "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        expected_value_str="-0.12345"
        sql_query="select decimal(%s, 5, 5) from iidbconstants" % expected_value_str
        
        expected_value=Decimal(expected_value_str)
        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)
        
        self.curs.close()
        self.con.close()

    def test_decimalBindLongValues(self):
        """Decimal type, sanity check for bind "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        expected_value_str="-0.12345"
        expected_value_str="-0.1234567890123456789012345678901"
        expected_value=Decimal(expected_value_str)
        sql_query="select decimal(?, 31, 31) from iidbconstants" # Max precision for Ingres 9.1.x and earlier
        
        self.curs.execute(sql_query, (expected_value,))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)
        
        self.curs.close()
        self.con.close()

    def test_decimalCrash(self):
        """Select/fetch of Decimal values caused crash
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        dropTable(self.curs, 'pydec_bug')

        sql_query="""create table pydec_bug(
        col001  char(4) not null,
        col002  char(3) not null,
        col003  decimal(6,0) not null,
        col004  char(4) not null,
        col005  char(8) not null default ' ',
        col006  decimal(11,0) not null,
        col007  decimal(20,0) not null,
        col008  decimal(11,0) not null,
        col009  char(2) not null default ' ',
        col010  char(2) not null default ' ',
        col011  decimal(20,0) not null,
        col012  decimal(20,0) not null,
        col013  decimal(20,0) not null,
        col014  decimal(11,0) not null default 0,
        col015  decimal(11,0) not null,
        col016  char(1) not null default ' ',
        col017  char(5) not null default ' ',
        col018  char(1) not null default ' ',
        col019  char(5) not null default ' ',
        col020  char(1) not null default ' ',
        col021  char(1) not null default ' ',
        col022  char(4) not null,
        col023  char(3) not null,
        col024  char(1) not null,
        col025  char(1) not null,
        col026  char(1) not null,
        col027  char(1) not null,
        col028  char(1) not null,
        col029  decimal(11,0) not null,
        col030  char(1) not null default ' ',
        col031  char(1) not null,
        col032  decimal(11,0) not null,
        col033  decimal(11,0) not null,
        col034  decimal(6,0) not null,
        col035  decimal(6,0) not null,
        col036  char(10) not null,
        col037  char(1) not null,
        col038  char(10) not null,
        col039  char(1) not null,
        col040  char(3) not null,
        col041  char(2) not null,
        col042  char(2) not null,
        col043  char(2) not null,
        col044  char(2) not null,
        col045  char(2) not null,
        col046  char(2) not null,
        col047  char(2) not null,
        col048  char(2) not null,
        col049  decimal(11,0) not null default 0
        )"""
        self.curs.execute(sql_query)

        sql_query="""insert into pydec_bug 
        values ('ATP ', 
        'AA ', 245, '8055', 'VANVOEGF', 1000000, 
        212076948176824000, 1, '  ', '  ', 212076878400000000, 
        464268974400000000, 464269060799999000, 12216486, 0, ' 
        ', '     ', ' ', '     ', '2', ' ', '    ', 'PCF', ' ',
         ' ', 'S', 'L', ' ', 1, 'N', ' ', 0, 0, 0, 0, 
        '          ', ' ', '          ', ' ', 'ADT', 'V ', '  '
        , '  ', '  ', '  ', '  ', '  ', '  ', 0)
        """
        self.curs.execute(sql_query)


        # Select of literals of same values does not crash, difference may be nullabilty
        sql_query = "select * from pydec_bug"
        self.curs.execute(sql_query)
        rs= self.curs.fetchall()

        self.curs.close()
        self.con.close()

    def test_SimpleUnicodeSelect(self):
        """Simple Unicode test of select
        String length is the same/close to max length defined for column (descriptor)
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        table_name='utable'
        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 nvarchar(9))" % table_name)
        self.curs.execute("insert into %s (col1, col2) values (1, U&'1\91523456789')" % table_name) # NOTE uses escaped Ingres Unicode literal, Ingres 9.1 and later feature
        self.con.commit()

        self.curs.execute("select * from %s order by col1" % table_name)
        all_rows = self.curs.fetchall()
        x=all_rows[0][1]
        self.assertEqual(all_rows, [(1, u'1\u91523456789',)])

        self.curs.close()
        self.con.close()

    def test_SimpleUnicodeBind(self):
        """Simple Unicode test of bind parameter
        String length is the same/close to max length defined for column (descriptor)

            ./test_ingresdbi_dbapi20.py -v test_Ingresdbi.NOtest_SimpleUnicodeBind test_Ingresdbi.test_cursorIter
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        mystr = u'123456789'
        table_name='utable'
        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 nvarchar(9))" % table_name)
        self.curs.execute("insert into %s (col1, col2) values (2, ?)"%table_name, (mystr,))
        self.con.commit()

        self.curs.execute("select * from %s order by col1" % table_name)
        all_rows = self.curs.fetchall()
        self.assertEqual(all_rows, [(2, mystr,)])

        self.curs.execute("select ? from iidbconstants", (mystr,))
        all_rows = self.curs.fetchall()

        self.assertEqual(all_rows, [(mystr,)])
        self.curs.close()
        self.con.close()

    ## FIXME needs to be enabled (and debugged) along with test_cursorIter (which hangs when this test is enabled with r1843)
    def NOtest_anotherPreCursIter(self):
        """based on test_SimpleUnicodeBind but with bad table name

            ./test_ingresdbi_dbapi20.py -v test_Ingresdbi.NOtest_anotherPreCursIter test_Ingresdbi.test_cursorIter
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        mystr = u'123456789'
        table_name='utable'
        dropTable(self.curs, table_name)
        self.curs.execute("create table %s (col1 integer, col2 nvarchar(9))" % table_name)
        self.curs.execute("insert into %s (col1, col2) values (2, ?)", (mystr,)) # NOTE bad table name
        self.con.commit()

        self.curs.close()
        self.con.close()

    def test_stringLongValues(self):
        """string (varchar) type, sanity check for "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value="X"*50
        expected_value_len=len(expected_value)
        sql_query="select varchar('%s', %d) from iidbconstants" % (expected_value, expected_value_len)

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()
        self.con.close()

    def test_stringBindLongValues(self):
        """string (varchar) type, sanity check for bind "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value="-0.1234567890123456789012345678901"
        expected_value_len=len(expected_value)
        sql_query="select varchar(?, %d) from iidbconstants" % expected_value_len

        self.curs.execute(sql_query, (expected_value,))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()

    def test_stringCharLongValues(self):
        """string (char) type, sanity check padding for "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value="X"*50
        padlength=20
        expected_value_len=len(expected_value)+padlength
        sql_query="select char('%s', %d) from iidbconstants" % (expected_value, expected_value_len)

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        expected_value=expected_value + ' '*padlength
        self.assertEqual(rs[0], expected_value)

        self.curs.close()
        self.con.close()

    def test_stringUnicodeCharLongValues(self):
        """string Unicode (char) type, sanity check padding for "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value=u"X"*50 # Not python 3.x compat
        padlength=20
        expected_value_len=len(expected_value)+padlength
        sql_query="select nchar('%s', %d) from iidbconstants" % (expected_value, expected_value_len) # Requires Ingres 9.1.1/9.2 with fix for bug 121900

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        expected_value=expected_value + ' '*padlength
        dbvalue = rs[0]
        self.assertEqual(dbvalue, expected_value, 'len dbvalue %d len canon %d. %r != %r'%(len(dbvalue), len(expected_value), dbvalue, expected_value))

        self.curs.close()
        self.con.close()

    def test_stringUnicodeLongValues(self):
        """Unicode string (nvarchar) type, sanity check for "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        Relies on Ingres 9.1 coercion of Unicode strings support
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value="X"*50
        expected_value_len=len(expected_value)
        sql_query="select nvarchar('%s', %d) from iidbconstants" % (expected_value, expected_value_len)

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()
        self.con.close()

    def test_stringUnicodeBindLongValues(self):
        """Unicode string (nvarchar) type, sanity check for bind "long" values.
        Long values as those where the value uses all the space and
        will display at maximum width.
        Relies on Ingres 9.1 coercion of Unicode strings support
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value="-0.1234567890123456789012345678901"
        expected_value_len=len(expected_value)
        sql_query="select nvarchar(?, %d) from iidbconstants" % expected_value_len

        self.curs.execute(sql_query, (expected_value,))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()

    def test_BindNullNone(self):
        """Test bind parameters of None (NULL) Trac ticket:417
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value=None
        sql_query="select ? from iidbconstants"

        self.curs.execute(sql_query, (expected_value,))
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()
        self.con.close()
    
    def test_BindNullNoneTypes(self):
        """Test bind parameters of None (NULL) with different destination types Trac ticket:417
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        expected_value=None
        table_name='session.Temp'
        sql_insert="INSERT INTO %s VALUES(?)" % table_name
        sql_query="select col1 from %s" % table_name
        column_type_list=['integer', 'float', 'char', 'varchar', 'nchar', 'nvarchar', 'decimal(10, 3)', 'date']

        for temp_type in column_type_list:
            dropTable(self.curs, table_name)
            self.curs.execute("DECLARE GLOBAL TEMPORARY TABLE %s (col1 %s) ON COMMIT PRESERVE ROWS WITH NORECOVERY" % (table_name, temp_type))
            self.curs.execute(sql_insert, (expected_value,))
            self.curs.execute(sql_query)
            rs = self.curs.fetchall()
            rs = rs[0]
            self.assertEqual(rs[0], expected_value)
        # clean up
        dropTable(self.curs, table_name)

        self.curs.close()
        self.con.close()
    
    def test_FetchAfterClose(self):
        """Trac ticket 445 Python DBI Driver Segfaults when fetching rows from cursor on closed connection.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        # tests go here
        self.curs.execute('SELECT * FROM iidbconstants')
        self.con.close()

        # should raise exception
        expected_exception=driver.OperationalError  ## old Linux ODBC driver only behavior
        expected_exception=driver.InterfaceError
        self.assertRaises(expected_exception, self.curs.fetchone)

    def test_ExecuteWithEmptyParams(self):
        """Trac ticket 446 execute with empty parm iterable
        """
        self.con = self._connect()
        self.curs = self.con.cursor()

        # tests go here
        expected_value=1
        sql_query="select 1 from iidbconstants"

        bind_parms=[] # List
        self.curs.execute(sql_query, bind_parms)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        bind_parms=() # Tuple
        self.curs.execute(sql_query, bind_parms)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)

        self.curs.close()
        self.con.close()

    def test_bugSelectLongNvarcharValues(self):
        """Trac ticket:502 select on LONG NVARCHAR returns empty string
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        # tests go here
        expected_value='bake it'
        sql_query="SELECT long_nvarchar('%s') FROM iidbconstants" % expected_value

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)
        
        self.curs.close()
        self.con.close()

    def NOtest_test_bugSelectLongNvarcharHang(self):
        """Trac ticket:502 select on LONG NVARCHAR returns empty string
        Related to test_bugSelectLongNvarcharValue test but issues multiple
        SQL statements which then cause test_cursorIter to hang if bug 502 is present.
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        # tests go here
        expected_value='bake it'
        ##  NOTE causes test_cursorIter to hang if bug 502 is present and this test is used
        sql_query="SELECT recipe_string FROM recipe"
        self.curs.execute('create table recipe (recipe_string long nvarchar)')
        self.curs.execute("insert into recipe (recipe_string) values (%s)"%expected_value) # No bind params in this test, test is the select not the insert

        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assertEqual(rs[0], expected_value)
        
        self.curs.close()
        self.con.close()

    def test_bugNonSelectFetch(self):
        """Trac ticket:161 - fetch after insert (i.e. no select)
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        text = '1255130'
        table = 'bug161'
        dropTable(self.curs, '%s' % table)
        self.curs.execute("create table %s (mytext varchar(10) not null)" % (table))
        self.curs.execute("insert into %s values(?)" % (table), (text))
        self.failUnlessRaises(driver.DataError, self.curs.fetchall)
        self.curs.close()
        self.con.close()

    '''
    def test_TemplateCopyPasteMe(self):
        """Template test description fill me in
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        # tests go here
        
        self.curs.close()
        self.con.close()
    '''

if __name__ == '__main__':
    # Some tests use data files (without a full pathname)
    # set current working directory to test directory if
    # test is not being run from the same directory
    testpath=os.path.dirname(__file__)
    if testpath:
        os.chdir(testpath) 
    unittest.main()
