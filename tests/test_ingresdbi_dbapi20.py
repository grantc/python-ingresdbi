#!/usr/bin/env python
# -*- coding: ascii -*-
# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
"""
 Test script for the Ingres Python DBI based on test_psycopg_dbapi20.py.
 Uses dbapi20.py version 1.10 obtained from
 http://stuartbishop.net/Software/DBAPI20TestSuite/

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
"""
import dbapi20
import unittest
import ingresdbi
import popen2
import re
import os
import sys
import warnings
import datetime
import gc # CPython specific.....

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
pooled=os.getenv('test_pooled')  # Windows only
traceLevel_str=os.getenv('test_trace')
if traceLevel_str != None:
    traceLevel = int(traceLevel_str)
else:
    traceLevel = 0
traceFile=os.getenv('test_trace_file')

# Not sure about this!!!!
warnings.warn('FIXME Hang with: ./test_ingresdbi_dbapi20.py -v test_Ingresdbi.NOtest_bugTimestampMicrosecs test_Ingresdbi.test_cursorIter')
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
    except ingresdbi.DataError, info:
        sqlstate=repr(info.args[2])
        if sqlstate != "'42500'": # this looks buggy.. string with quotes as part of data?
            raise

class test_Ingresdbi(dbapi20.DatabaseAPI20Test):
    driver = ingresdbi
    connect_args = ()
    connect_kw_args = {}

    if dsn != None:
        connect_kw_args.setdefault('dsn',dsn)
    if username != None:
        connect_kw_args.setdefault('uid',username)
    if password != None:
        connect_kw_args.setdefault('pwd',password)
    if vnode != None:
        connect_kw_args.setdefault('vnode',vnode)
    if database != None:
        connect_kw_args.setdefault('database',database)
    else:
        connect_kw_args.setdefault('database', 'dbapi20_test')
    if pooled != None:
        connect_kw_args.setdefault('pooled', pooled)
        trace=(traceLevel, traceFile)
    else:
        trace=(0, None)
    connect_kw_args.setdefault('trace',trace)
    table_prefix = 'dbapi20test_' # If you need to specify a prefix for tables

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self) 

        dbname = self.connect_kw_args['database']

        try:
            con = self._connect()
            try:
                # Do we have a procedure called lower? 
                cur=con.cursor()
                cur.execute("select procedure_name from iiprocedures where procedure_name = 'lower'")
                if len(cur.fetchall()) == 0:
                    cur.execute("create procedure lower( a varchar(20) not null) result row (varchar(20)) as declare x=varchar(20) not null; begin select lower(a) into x; return row(x); end")
                    con.commit()
                con.close()
            except:
                print 'FIXME bare except hit with PASS'
                pass
        except:
            print 'FIXME bare except hit'
            cmd = "createdb -i %s -f nofeclients" % dbname
            cout,cin = popen2.popen2(cmd)
            cin.close()
            createdb_output=cout.read()
            if 'E_' in createdb_output:
                print createdb_output
                raise SystemExit()

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

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

    def test_bug_Embedded_Nulls(self):
        """Trac ticket:255 - embedded nulls are lost
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        sql_query=r"""select
    length(U&'x\0000y') as length_unicode,
    hex(U&'x\0000y') as hex_unicode, 
    length(varchar(U&'x\0000y')) as length_singlebyte, 
    hex(varchar(U&'x\0000y')) as hex_singlebyte
from iidbconstants
        """ # sample SQL to run in TM
        sql_query=r"""select
    length(U&'x\0000y') as length_unicode,
    U&'x\0000y' as hex_unicode, 
    length(varchar(U&'x\0000y')) as length_singlebyte, 
    varchar(U&'x\0000y') as hex_singlebyte
from iidbconstants
        """ # NOTE need 9.1.2 or later (problems with older/pre-patched versions)
        #expected_value=u'x\u0000y' # NOTE not Python 3.x compatible....
        expected_value='x\x00y' # should be Python 2.x and 3.x compatible
        self.curs.execute(sql_query)
        rs = self.curs.fetchall()
        rs = rs[0]
        self.assert_(rs[1] == expected_value)
        self.assert_(rs[0] == len(rs[1]))
        self.assert_(rs[3] == expected_value)
        self.assert_(rs[2] == len(rs[3]))

        self.curs.close()
        self.con.close()
    
    ## FIXME needs to be enabled (and debugged)
    def NOtest_bugTimestampMicrosecs(self):
        """Trac ticket:260 - microsecs on selects of (ansi) timestamps are incorrect
        WARNING having this test present appears to hang the test suite
        at test test_cursorIter :-( e.g.:

            ./test_ingresdbi_dbapi20.py -v test_Ingresdbi.NOtest_bugTimestampMicrosecs test_Ingresdbi.test_cursorIter

        Will run this test firs and then test_cursorIter which hangs and never completes.

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
        self.assert_(expected_value == dateval)
        
        # Original bug test case
        self.curs.execute("delete from pytimestampbug")
        self.curs.execute("insert into pytimestampbug(expire_date) values ('2008-09-18 12:42:45.899000')")
        self.curs.execute(sql_query)
        row=self.curs.fetchone()
        dateval = row[0]
        #dateval = expected_value # dumb test for debugging, if this is uncommented test_cursorIter() completes
        self.assert_(expected_value == dateval)
        
        self.curs.close()
        self.con.close()
    
    '''
    not actually abug by the looks of it
    def test_bugSelectLob(self):
        """Trac ticket:161 - fetch after insert (i.e. no select) - what is the expected behavior?
        """
        self.con = self._connect()
        self.curs = self.con.cursor()
        
        blob = 'x' * 1255130
        #self.curs.execute("drop table myBlob")
        dropTable(self.curs, 'myBlob')
        self.curs.execute("create table myBlob(blobCol long byte)")
        self.curs.execute("insert into myBlob values(?)", (ingresdbi.Binary(blob),))
        #self.curs.execute("select * from myBlob")
        rs = self.curs.fetchall() # issue fetch after an insert
        
        self.curs.close()
        self.con.close()
    '''

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
    unittest.main()
