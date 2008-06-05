#!/usr/bin/env python
"""
 Test script for the Ingres Python DBI based on test_psycopg_dbapi20.py.
 Uses dbapi20.py version 1.10 obtained from
 http://stuartbishop.net/Software/DBAPI20TestSuite/

 History:
    02-May-2008 (grant.croker@ingres.com)
        Created
    06-May-2008
        Added the detection/creation of the procedure 'lower' to setUp
"""
import dbapi20
import unittest
import ingresdbi
import popen2
import re
import os

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
                pass
        except:
            cmd = "createdb -fnofeclients dbapi20_test"
            cout,cin = popen2.popen2(cmd)
            cin.close()
            cout.read()

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

if __name__ == '__main__':
    unittest.main()
