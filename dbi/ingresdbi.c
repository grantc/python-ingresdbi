/*
** Copyright (c) 2008 Ingres Corporation
**
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License version 2 as
** published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License along
** with this program; if not, write to the Free Software Foundation, Inc.,
** 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
**
*/
#include <iidbi.h>
#include <iidbiconn.h>
#include <iidbicurs.h>
#include <iidbiutil.h>
#include <structmember.h>
#include <time.h>
#include <datetime.h>
#include <sys/types.h>
#include <sys/stat.h>

/**
** Name: iingresdbi.c - Ingres Python DB API base module classes and
**       methods
**
** Description:
**     This file defines:
**(E
    IIDBI_binary
    IIDBI_timeFromTicks
    IIDBI_time
    IIDBI_timeStampFromTicks
    IIDBI_timeStamp
    IIDBI_dateFromTicks
    IIDBI_date
    IIDBI_connect
    IIDBI_CONNECTION *IIDBI_connConstructor
    IIDBI_connCursor
    IIDBI_CURSOR *IIDBI_cursorConstructor
    IIDBI_cursorGetConnection
    IIDBI_cursorSetConnection
    IIDBI_cursorGetRownumber
    IIDBI_cursorSetRownumber
    IIDBI_cursorGetMessages
    IIDBI_cursorSetMessages
    IIDBI_cursorGetLastRowId
    IIDBI_cursorSetLastRowId
    IIDBI_cursorDestructor
    IIDBI_cursorPrint
    initingresdbi
    IIDBI_connGetMessages
    IIDBI_connSetMessages
    IIDBI_connGetError
    IIDBI_connSetError
    IIDBI_connGetInterfaceError
    IIDBI_connSetInterfaceError
    IIDBI_connGetDatabaseError
    IIDBI_connSetDatabaseError
    IIDBI_connGetInternalError
    IIDBI_connSetInternalError
    IIDBI_connGetOperationalError
    IIDBI_connSetOperationalError
    IIDBI_connGetProgrammingError
    IIDBI_connSetProgrammingError
    IIDBI_connGetIntegrityError
    IIDBI_connSetIntegrityError
    IIDBI_connGetDataError
    IIDBI_connSetDataError
    IIDBI_connGetNotSupportedError
    IIDBI_connSetNotSupportedError
    IIDBI_connGetWarning
    IIDBI_connSetWarning
    IIDBI_connGetErrorHandler   
    IIDBI_connSetErrorHandler
    IIDBI_cursorGetErrorHandler
    IIDBI_cursorSetErrorHandler
    IIDBI_cursorGetPrepared
    IIDBI_cursorSetPrepared
    IIDBI_connDestructor
    IIDBI_connPrint
    IIDBI_cursorExecuteMany
    IIDBI_cursorExecute
    IIDBI_cursorCallProc
    IIDBI_cursorNextSet
    IIDBI_cursorScroll
    IIDBI_cursorSetInputSizes
    IIDBI_cursorSetOutputSize
    IIDBI_cursorFetchMany
    IIDBI_cursorFetchAll
    IIDBI_connCommit
    IIDBI_connRollback
    IIDBI_mapError2exception
    IIDBI_connClose
    IIDBI_cursorClose
    IIDBI_cursorCleanup
    IIDBI_mapType
    IIDBI_cursorIterator
    IIDBI_cursorIterNext
    IIDBI_handleError
    IIDBI_handleWarning
    IIDBI_clearMessages
    IIDBI_connCleanup
    IIDBI_sendParameter
**)E
**
** History:
**   13-sep-04 (Ralph.Loen@ca.com)
**       Created.
**   17-sep-04 (Ralph.Loen@ca.com)
**       ProgrammingError and NotSupportedError were mistakenly initialized as
**       "Error".  Changed this to "DatabaseError".
**   17-sep-04 (Ralph.Loen@ca.com)
**       In IIDBI_cursorFetch(), added support for BIGINT and TINYINT.  Cast
**       bigints, longs, shorts, and chars more rigorously to prevent
**       possible data corruption or alignment problems.
**   18-oct-04 (Ralph.Loen@ca.com)
**       Return a "None" description attribute if the query has no result set.
**       Return all null data as "None".
**   21-oct-2004 (Ralph.Loen@ca.com)
**       New fixes for BIGINT.  If the Python interpreter passes a parmeter
**       as a long, cast and bind as long long (SQLBIGINT).  If fetching
**       a BIGINT column, cast as a PyLong data type.
**   22-Oct-2004 (Ralph.Loen@ca.com)
**       Fixed up support for FLOAT4 and FLOAT8.
**   25-Oct-2004 (grant.croker@ca.com)
**        Added version attribute (ingresdbi.version)
**   13-Dec-2004 (Ralph.Loen@ca.com)
**        Discard type objects ROWID, STRING, BINARY, NUMBER and DATETIME,
**        and replace them with module attributes.  Set the description
**        attribute accordingly.
**   17-Dec-2004 (Ralph.Loen@ca.com)
**        Fix accvio in fetching timestamp columns.
**   21-Dec-2004 (Ralph.Loen@ca.com)
**        Fixed cast of cursor ID's.  Added connection attribute "autocommit".
**   10-Jan-2005 (Ralph.Loen@ca.com)
**        Use new dbi_freeData() routine to resolve memory leaks.
**   14-Jan-2005 (Ralph.Loen@ca.com)
**        Initial support for Unicode data types.
**   19-Jan-2005 (Ralph.Loen@ca.com)
**        In IIDBI_connectionClose(), free dsn string, if present.
**   01-Feb-2005 (Ralph.Loen@ca.com)
**        Added cursor rowcount attribute.
**   08-Feb-2005 (Ralph.Loen@ca.com)
**        Tighten up memory management.  Removed INCREF of Py_None when
**        inserting into a tuple.  Removed INCREF of returned row when
**        fetching cursors.  
**   22-Feb-2005 (Ralph.Loen@ca.com)
**        Added cursor.fetchmany() and cursor.callproc() methods.  This
**        initial version of callproc() supports only input parameters.
**   19-may-2005 (Ralph.Loen@ca.com)
**        Added support for row-returning procedures in Cursor.callproc().
**        Return input parameters.  Output and BYREF parameters are still
**        not supported because the DBI specs aren't flexible enough to
**        work with Ingres limitations.
**   29-may-2005 (Ralph.Loen@ca.com)
**        Added cursor.executemany().
**   09-jun-2005 (loera01)
**        Added ingresdbi.Date(), ingresdbi.DateFromTicks(), ingresdbi.Time(),
**        ingresdbi.TimeFromTicks().
**   15-jun-2005 (loera01)
**        Added selectloops connection attribute.
**   26-jul-2005 (loera01)
**        Added servertype connection attribute.
**   16-nov-2005 (loera01)
**        Populated cursor.description sequence with precision, scale, 
**        internal size, and display size values.
**   22-nov-2005 (loera01)
**        Added __iter__(), next() cursor methods.  Added rownumber cursor
**        attribute.
**   26-jan-2006 (loera01)
**        In IIDBI_cursorIterNext(), increment the reference count before
**        returning the row.
**   27-Feb-2006 (Ralph.Loen@ingres.com)
**        Amended several memory leaks as advised by Tilman:
**        1.  Removed weak reference counting of connection and 
**            cursor objects. The weak references were for some reason not 
**            cleaned up properly, causing the list of referenceable objects 
**            in the garbage collector to grow indefinitely.
**        2.  Added code to the cursor.close() function which removes the 
**            reference to the cursor from the connection's cursor list.
**        3.  Calling cursor.close() caused an exception in case no SQL 
**            statement had been performed prior to closing the cursor.
**        4.  Repeatetly performing a query with cursor.execute() did not 
**            clean up the internal descriptor object. 
**   18-may-2006 (Ralph.Loen@ingres.com)
**        Reinstated weak reference dictionaries.  Strong references resulted
**        in negative reference count exceptions when running in debug mode.
**        Return warnings for extended attributes and methods.
**   20-may-2006 (Ralph.Loen@ingres.com)
**        Added cursor.setinputsizes() and cursor.setoutputsize().  
**   31-may-2006 (Ralph.Loen@ingres.com)
**        Added documentation for methods and functions.
**  04-Aug-2009 (Chris.Clark@ingres.com)
**      Removed compile warnings:
**        dbi/ingresdbi.c:217: warning: function declaration isn't a prototype
**        dbi/ingresdbi.c:223: warning: function declaration isn't a prototype
**        dbi/ingresdbi.c:245: warning: function declaration isn't a prototype
**        dbi/ingresdbi.c:1361: warning: function declaration isn't a prototype
**        dbi/ingresdbi.c:1533: warning: function declaration isn't a prototype
**      Reported by GCC 4.2.4 with Python 2.4. Defaults to -Wstrict-prototypes
**      Added explict void to functions prototypes that take input parameters.
**      Alternative is to simply use -Wno-strict-prototypes
**  07-Aug-2009 (Chris.Clark@ingres.com)
**      Ingres Bugs bug# 121611 Trac ticket:403
**      Error (and Cpython exits):
**              Fatal Python error: deallocating None
**      After fetch'ing None/NULL values.
**      Added correct counter increment to Py_None
**      simplified NULL logic into one place by
**      removing duplicate code.
**/

static PyObject *IIDBI_Warning;
static PyObject *IIDBI_Error;
static PyObject *IIDBI_DatabaseError;
static PyObject *IIDBI_InterfaceError;
static PyObject *IIDBI_DataError;
static PyObject *IIDBI_OperationalError;
static PyObject *IIDBI_IntegrityError;
static PyObject *IIDBI_InternalError;
static PyObject *IIDBI_ProgrammingError;
static PyObject *IIDBI_NotSupportedError;
static PyObject *IIDBI_IOError;

static PyObject * IIDBI_connect(PyObject *self, PyObject *args,
    PyObject *keywords);
static PyObject * IIDBI_binary(PyObject *self, PyObject *args);
static PyObject *IIDBI_connCursor(IIDBI_CONNECTION *);
static IIDBI_CONNECTION *IIDBI_connConstructor(void);
static void IIDBI_connDestructor(IIDBI_CONNECTION *self);
static int IIDBI_connPrint(IIDBI_CONNECTION *self, FILE *fp, int flags);
static PyObject *IIDBI_connCommit(IIDBI_CONNECTION *self);
static PyObject *IIDBI_connRollback(IIDBI_CONNECTION *self);
static PyObject *IIDBI_connClose(IIDBI_CONNECTION *self);
static IIDBI_CURSOR *IIDBI_cursorConstructor(void);
static PyObject *IIDBI_cursorExecute(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorExecuteMany(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorFetch(IIDBI_CURSOR *self);
static PyObject *IIDBI_cursorFetchAll(IIDBI_CURSOR *self);
static PyObject *IIDBI_cursorFetchMany(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorCallProc(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorClose(IIDBI_CURSOR *self);
static PyObject *IIDBI_cursorSetInputSizes(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorSetOutputSize(IIDBI_CURSOR *self, PyObject *args);
static PyObject *IIDBI_cursorNextSet(IIDBI_CURSOR *self);
static PyObject *IIDBI_cursorScroll(IIDBI_CURSOR *self, PyObject *args);
static PyObject * IIDBI_date(PyObject *self, PyObject *args);
static PyObject * IIDBI_dateFromTicks(PyObject *self, PyObject *args);
static PyObject * IIDBI_time(PyObject *self, PyObject *args);
static PyObject * IIDBI_timeFromTicks(PyObject *self, PyObject *args);
static PyObject * IIDBI_timeStamp(PyObject *self, PyObject *args);
static PyObject * IIDBI_timeStampFromTicks(PyObject *self, PyObject *args);

static PyObject * IIDBI_cursorIterator(IIDBI_CURSOR *self);
static PyObject * IIDBI_cursorIterNext(IIDBI_CURSOR *self);

PyMODINIT_FUNC SQL_API initingresdbi(void);
int IIDBI_cursorCleanup(IIDBI_CURSOR *self);
void IIDBI_connCleanup(IIDBI_CONNECTION * self);
static void IIDBI_cursorDestructor(IIDBI_CURSOR *self);
static int IIDBI_cursorPrint(IIDBI_CURSOR *self, FILE *fp, int flags);
int IIDBI_mapError2exception(PyObject *self, IIDBI_ERROR *err, RETCODE rc, 
    char *queryText);
int IIDBI_handleError(PyObject *self, PyObject *exception, char *errMsg);
void IIDBI_handleWarning(char *errMsg, PyObject *messages);
void IIDBI_clearMessages(PyObject *messages);
int IIDBI_sendParameters(IIDBI_CURSOR *self, PyObject *params);

/*
** Get/Setters
*/
static PyObject *IIDBI_cursorGetConnection(IIDBI_CURSOR *self, void *closure);
static int IIDBI_cursorSetConnection(IIDBI_CURSOR *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_cursorGetRownumber(IIDBI_CURSOR *self, void *closure);
static int IIDBI_cursorSetRownumber(IIDBI_CURSOR *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_cursorGetMessages(IIDBI_CURSOR *self, void* closure);
static int IIDBI_cursorSetMessages(IIDBI_CURSOR *self, PyObject *value, 
    void * closure);
static PyObject *IIDBI_cursorGetLastRowId(IIDBI_CURSOR *self, void *closure); 
static int IIDBI_cursorSetLastRowId(IIDBI_CURSOR *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_cursorGetErrorHandler(IIDBI_CURSOR *self, 
    void *closure); 
static int IIDBI_cursorSetErrorHandler(IIDBI_CURSOR *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetMessages(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetMessages(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);

static PyObject *IIDBI_connGetError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetInterfaceError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetInterfaceError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetDatabaseError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetDatabaseError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetInternalError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetInternalError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetOperationalError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetOperationalError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetProgrammingError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetProgrammingError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetIntegrityError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetIntegrityError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetDataError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetDataError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetNotSupportedError(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetNotSupportedError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetWarning(IIDBI_CONNECTION *self, void* closure);
static int IIDBI_connSetWarning(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_connGetErrorHandler(IIDBI_CONNECTION *self, 
    void *closure); 
static int IIDBI_connSetErrorHandler(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure);
static PyObject *IIDBI_cursorGetPrepared(IIDBI_CURSOR *self, 
    void *closure); 
static int IIDBI_cursorSetPrepared(IIDBI_CURSOR *self, PyObject *value, 
    void *closure);

int checkBooleanArg(char *arg);
int IIDBI_mapType(int type);

/*
** Globally shared environment
*/
IIDBI_ENV IIDBIenv;

static PyObject *IIDBI_module = NULL;
static PyObject *decimalType;

static char ingresdbi_doc[] = 
"The ingresdbi module is a DBI driver intended for Ingres databases, \n" \
"Ingres gateways, and EDBC servers. It uses the Ingres ODBC Driver for \n" \
"database access.  The ingresdbi driver supports all of the DBI 2.0 core \n" \
"specification and all of the extended 2.0 specification as documented in \n" \
"PEP 249.  The only exceptions are the cursor.scroll() method and the \n" \
"cursor.nextset() method, due to the current limitations of Ingres \n" \
"database servers.";

static PyMethodDef IIDBI_methods[] = 
{
    {
        "Binary", (PyCFunction)IIDBI_binary, METH_VARARGS, "ingresdbi.Binary"
    },
    {
        "connect", (PyCFunction)IIDBI_connect, METH_VARARGS | METH_KEYWORDS, "Connect(dsn | database [,username, password, vnode, autocommit, selectloops, servertype, driver, rolename, rolepwd, group, catconnect, numeric_overflow, catschemanull, dbms_pwd, [trace=traceLevel]]"
    },
    {
        "Date", (PyCFunction)IIDBI_date, METH_VARARGS, "ingresdbi.Date"
    },
    {
        "DateFromTicks", (PyCFunction)IIDBI_dateFromTicks, METH_VARARGS, "ingresdbi.DateFromTicks"
    },
    {
        "Time", (PyCFunction)IIDBI_time, METH_VARARGS, "ingresdbi.Time"
    },
    {
        "TimeFromTicks", (PyCFunction)IIDBI_timeFromTicks, METH_VARARGS, "ingresdbi.TimeFromTicks"
    },
    {
        "Timestamp", (PyCFunction)IIDBI_timeStamp, METH_VARARGS, "ingresdbi.Timestamp"
    },
    {
        "TimestampFromTicks", (PyCFunction)IIDBI_timeStampFromTicks, METH_VARARGS, "ingresdbi.TimeStampFromTicks"
    },
    {
        NULL, NULL, 0, NULL
    }
};

static PyMethodDef IIDBI_connMethods[] = 
{
    { 
        "cursor", (PyCFunction)IIDBI_connCursor, METH_NOARGS, "Cursor" 
    },
    {
        "commit", (PyCFunction)IIDBI_connCommit, METH_NOARGS, "Commit" 
    },
    {
        "rollback", (PyCFunction)IIDBI_connRollback, METH_NOARGS, "Rollback" 
    },
    {
        "close", (PyCFunction)IIDBI_connClose, METH_NOARGS, "Close" 
    },
    { NULL, NULL, 0, NULL }
};

static PyMethodDef IIDBI_cursorMethods[] = 
{
    { 
        "execute", (PyCFunction)IIDBI_cursorExecute, METH_VARARGS, "Execute" 
    },
    { 
        "executemany", (PyCFunction)IIDBI_cursorExecuteMany, METH_VARARGS, "Executemany" 
    },
    { 
        "callproc", (PyCFunction)IIDBI_cursorCallProc, METH_VARARGS, "Callproc" 
    },
    { 
        "fetchone", (PyCFunction)IIDBI_cursorFetch, METH_NOARGS, "Fetchone" 
    },
    { 
        "fetchall", (PyCFunction)IIDBI_cursorFetchAll, METH_NOARGS, "Fetchall" 
    },
    { 
        "fetchmany", (PyCFunction)IIDBI_cursorFetchMany, METH_VARARGS, "Fetchmany" 
    },
    { 
        "close", (PyCFunction)IIDBI_cursorClose, METH_NOARGS, "Close" 
    },
    { 
        "setinputsizes", (PyCFunction)IIDBI_cursorSetInputSizes, METH_VARARGS, "Setinputsizes" 
    },
    { 
        "setoutputsize", (PyCFunction)IIDBI_cursorSetOutputSize, METH_VARARGS, "Setoutputsize" 
    },
    { 
        "nextset", (PyCFunction)IIDBI_cursorNextSet, METH_NOARGS, "Nextset" 
    },
    { 
        "scroll", (PyCFunction)IIDBI_cursorScroll, METH_VARARGS, "Scroll" 
    },
    { NULL, NULL, 0, NULL }
};

static PyMemberDef IIDBI_cursorMembers[] = 
{
    {
        "description", T_OBJECT_EX, offsetof(IIDBI_CURSOR, description), 
        READONLY,
        "iidbi cursor description"
    },
    {
        "rowcount", T_INT, offsetof(IIDBI_CURSOR, rowcount), 
        READONLY,
        "iidbi rowcount"
    },
    {
        "arraysize", T_INT, offsetof(IIDBI_CURSOR, arraysize), 
        0,
        "iidbi arraysize"
    },
    {
        NULL
    }  /* Sentinel */
};


static PyGetSetDef IIDBI_cursorGetSetters[] = 
{
    {
        "connection", 
        (getter)IIDBI_cursorGetConnection, (setter)IIDBI_cursorSetConnection,
        "ingresdbi.cursor.connection",
        NULL
    },
    {
        "rownumber", 
        (getter)IIDBI_cursorGetRownumber, (setter)IIDBI_cursorSetRownumber,
        "ingresdbi.cursor.rownumber",
        NULL
    },
    {
        "messages", 
        (getter)IIDBI_cursorGetMessages, (setter)IIDBI_cursorSetMessages,
        "ingresdbi.cursor.messages",
        NULL
    },
    {
        "lastrowid", 
        (getter)IIDBI_cursorGetLastRowId, (setter)IIDBI_cursorSetLastRowId,
        "ingresdbi.cursor.lastrowid",
        NULL
    },
    {
        "errorhandler",
        (getter)IIDBI_cursorGetErrorHandler, (setter)IIDBI_cursorSetErrorHandler,
        "ingresdbi.cursor.errorhandler",
        NULL
    },
    {
        "prepared",
        (getter)IIDBI_cursorGetPrepared, (setter)IIDBI_cursorSetPrepared,
        "ingresdbi.cursor.prepared",
        NULL
    },
    {
        NULL
    }  /* Sentinel */
};

static PyGetSetDef IIDBI_connGetSetters[] = 
{
    {
        "messages", 
        (getter)IIDBI_connGetMessages, (setter)IIDBI_connSetMessages,
        "ingresdbi.connection.messages",
        NULL
    },
    {
        "Error", 
        (getter)IIDBI_connGetError, (setter)IIDBI_connSetError,
        "ingresdbi.connection.Error",
        NULL
    },
    {
        "InterfaceError", 
        (getter)IIDBI_connGetInterfaceError, (setter)IIDBI_connSetInterfaceError,
        "ingresdbi.connection.InterfaceError",
        NULL
    },
    {
        "DatabaseError", 
        (getter)IIDBI_connGetDatabaseError, (setter)IIDBI_connSetDatabaseError,
        "ingresdbi.connection.DatabaseError",
        NULL
    },
    {
        "InternalError", 
        (getter)IIDBI_connGetInternalError, (setter)IIDBI_connSetInternalError,
        "ingresdbi.connection.InternalError",
        NULL
    },
    {
        "OperationalError", 
        (getter)IIDBI_connGetOperationalError, (setter)IIDBI_connSetOperationalError,
        "ingresdbi.connection.OperationalError",
        NULL
    },
    {
        "ProgrammingError", 
        (getter)IIDBI_connGetProgrammingError, (setter)IIDBI_connSetProgrammingError,
        "ingresdbi.connection.ProgrammingError",
        NULL
    },
    {
        "IntegrityError", 
        (getter)IIDBI_connGetIntegrityError, (setter)IIDBI_connSetIntegrityError,
        "ingresdbi.connection.IntegrityError",
        NULL
    },
    {
        "DataError", 
        (getter)IIDBI_connGetDataError, (setter)IIDBI_connSetDataError,
        "ingresdbi.connection.DataError",
        NULL
    },
    {
        "NotSupportedError", 
        (getter)IIDBI_connGetNotSupportedError, (setter)IIDBI_connSetNotSupportedError,
        "ingresdbi.connection.NotSupportedError",
        NULL
    },
    {
        "Warning", 
        (getter)IIDBI_connGetWarning, (setter)IIDBI_connSetWarning,
        "ingresdbi.connection.Warning",
        NULL
    },
    {
        "errorhandler",
        (getter)IIDBI_connGetErrorHandler, (setter)IIDBI_connSetErrorHandler,
        "ingresdbi.connection.errorhandler",
        NULL
    },
    {
        NULL
    }  /* Sentinel */
};

static PyTypeObject IIDBI_connectType = 
{
    PyObject_HEAD_INIT(NULL)
    0,                              /*ob_size*/
    "Ingres DBI connection type",   /*tp_name*/
    sizeof(IIDBI_CONNECTION),       /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    /* methods */
    (destructor)IIDBI_connDestructor,       /*tp_dealloc*/
    (printfunc)IIDBI_connPrint,          /*tp_print*/
    (getattrfunc)0,                 /*tp_getattr*/
    (setattrfunc)0,                 /*tp_setattr*/
    (cmpfunc)0,                     /*tp_compare*/
    (reprfunc)0,                    /*tp_repr*/
    0,                              /* tp_as_number*/
    0,                              /* tp_as_sequence*/
    0,                              /* tp_as_mapping*/
    (hashfunc)0,                    /*tp_hash*/
    (ternaryfunc)0,                 /*tp_call*/
    (reprfunc)0,                    /*tp_str*/
    (getattrofunc)0,                /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Ingres connection object, DBI 2.0",  /* Documentation string */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,		                     /* tp_iter */
    0,		                     /* tp_iternext */
    IIDBI_connMethods,               /* tp_methods */
    0,                               /* tp_members */
    IIDBI_connGetSetters,            /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    0,                               /* tp_init */
    0,                               /* tp_alloc */
    0                                /* tp_new */
};

static PyTypeObject IIDBI_cursorType = 
{
    PyObject_HEAD_INIT(NULL)
    0,                              /*ob_size*/
    "Ingres DBI cursor type",       /*tp_name*/
    sizeof(IIDBI_CURSOR),           /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    /* methods */
    (destructor)IIDBI_cursorDestructor,  /*tp_dealloc*/
    (printfunc)IIDBI_cursorPrint,        /*tp_print*/
    (getattrfunc)0,                 /*tp_getattr*/
    (setattrfunc)0,                 /*tp_setattr*/
    (cmpfunc)0,                     /*tp_compare*/
    (reprfunc)0,                    /*tp_repr*/
    0,                              /* tp_as_number*/
    0,                              /* tp_as_sequence*/
    0,                              /* tp_as_mapping*/
    (hashfunc)0,                    /*tp_hash*/
    (ternaryfunc)0,                 /*tp_call*/
    (reprfunc)0,                    /*tp_str*/
    (getattrofunc)0,                /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "Ingres cursor object, DBI 2.0", /* Documentation string */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    0,                               /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    (getiterfunc) IIDBI_cursorIterator,/* tp_iter */
    (iternextfunc) IIDBI_cursorIterNext,/* tp_iternext */
    IIDBI_cursorMethods,             /* tp_methods */
    IIDBI_cursorMembers,             /* tp_members */
    IIDBI_cursorGetSetters,          /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    0,                               /* tp_init */
    0,                               /* tp_alloc */
    0                                /* tp_new */
};

/*{
** Name: IIDBI_binary
**
** Description:
**     Constructs a buffer object.
**
** Inputs:
**     self - pointer to module.
**     args - object to convert to buffer.
**
** Outputs:
**     None.
**
** Returns:
**     Buffer object.
**
** Exceptions:
**     Input object must be a valid Python object.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_binary(PyObject *self, PyObject *args) 
{
    PyObject *binary = NULL;
    PyObject *obj = NULL;

    DBPRINTF(DBI_TRC_RET)("IIDBI_binary {{{1\n");

    if (!PyArg_ParseTuple(args, "O", &obj)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for binary constructor");
        goto errorExit;
    }

    binary = PyBuffer_FromObject( obj, 0, PyObject_Length(obj));
    if (!binary)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create buffer object from input parameter");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_binary }}}1\n");
    return (PyObject *)binary;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_binary }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_timeFromTicks
**
** Description:
**     Converts ticks to datetime.time object.
**
** Inputs:
**     self - pointer to module.
**     args - ticks.
**
** Outputs:
**     None.
**
** Returns:
**     datetime.time object.
**
** Exceptions:
**     args must be a Python double object.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_timeFromTicks(PyObject *self, PyObject *args) 
{
    PyObject *pytime = NULL;
    double ticks = 0.0;
    time_t seconds = 0;

    struct tm *tm;

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeFromTicks {{{1\n");

    if (!PyArg_ParseTuple(args, "d", &ticks)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for timeFromTicks constructor");
        goto errorExit;
    }

    seconds = (time_t)ticks;

    tm = localtime((const time_t *)&seconds);

    pytime = PyTime_FromTime( tm->tm_hour, tm->tm_min, tm->tm_sec, 0 );
    if (!pytime)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create time object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeFromTicks }}}1\n");
    return (PyObject *)pytime;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_timeFromTicks }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_time
**
** Description:
**     Converts ticks to datetime.time object.
**
** Inputs:
**     self - pointer to module.
**     args - Python integers consisting of hour, minutes and seconds.
**
** Outputs:
**     None.
**
** Returns:
**     datetime.time object.
**
** Exceptions:
**     args must evaluate to three integer arguments and convertible to
**     datetime.time.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_time(PyObject *self, PyObject *args) 
{
    PyObject *pytime = NULL;
    int hour, min, sec;

    DBPRINTF(DBI_TRC_RET)("IIDBI_time {{{1\n");

    if (!PyArg_ParseTuple(args, "iii", &hour, &min, &sec)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for time constructor");
        goto errorExit;
    }

    pytime = PyTime_FromTime( hour, min, sec, 0 );
    if (!pytime)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create time object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_time }}}1\n");
    return (PyObject *)pytime;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_time }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_timestampFromTicks
**
** Description:
**     Converts epoch seconds to datetime.timestamp object.
**
** Inputs:
**     self - pointer to module.
**     args - ticks (epoch seconds)
**
** Outputs:
**     None.
**
** Returns:
**     datetime.datetime object.         
**
** Exceptions:
**     Argument must be a Python double and convertible to datetime.datetime.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_timeStampFromTicks(PyObject *self, PyObject *args) 
{
    PyObject *datetime = NULL;
    double ticks = 0.0;
    time_t seconds = 0;

    struct tm *tm;

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStampFromTicks {{{1\n");

    if (!PyArg_ParseTuple(args, "d", &ticks)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for timeStampFromTicks constructor");
        goto errorExit;
    }

    seconds = (time_t)ticks;

    tm = localtime((const time_t *)&seconds);

    datetime = PyDateTime_FromDateAndTime( tm->tm_year + 1900, tm->tm_mon + 1, 
        tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, 0 );

    if (!datetime)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create datetime object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStampFromTicks }}}1\n");
    return (PyObject *)datetime;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStampFromTicks }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_timestamp
**
** Description:
**     Converts YYMMDD HHMMSS to a datetime.datime object.
**
** Inputs:
**     self - pointer to module.
**     args - evaluates to year, month, day, hours, minutes and seconds.
**
** Outputs:
**     None.
**
** Returns:
**     datetime.datetime object.
**
** Exceptions:
**     args must evaluate to YYMMDD HHMMSS.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_timeStamp(PyObject *self, PyObject *args) 
{
    PyObject *pytime = NULL;
    int month, year, day, hour, min, sec;

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStamp {{{1\n");

    if (!PyArg_ParseTuple(args, "iiiiii", &year, &month, &day, &hour, 
        &min, &sec)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for timeStamp constructor");
        goto errorExit;
    }

    pytime = PyDateTime_FromDateAndTime( year, month, day, hour, min, sec, 0 );
    if (!pytime)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create datetime object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStamp }}}1\n");
    return (PyObject *)pytime;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_timeStamp }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_dateFromTicks
**
** Description:
**     Convert datetime.date from ticks.
**
** Inputs:
**     self - pointer to module.
**     ticks - epoch seconds. 
**
** Outputs:
**     None.
**
** Returns:
**     datetime.date object.    
**
** Exceptions:
**     args must be a double and convertible to datetime.date.         
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject * IIDBI_dateFromTicks(PyObject *self, PyObject *args) 
{
    PyObject *date = NULL;
    double ticks = 0.0;
    time_t seconds = 0;

    struct tm *tm;

    DBPRINTF(DBI_TRC_RET)("IIDBI_dateFromTicks {{{1\n");

    if (!PyArg_ParseTuple(args, "d", &ticks)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for dateFromTicks constructor");
        goto errorExit;
    }

    seconds = (time_t)ticks;

    tm = localtime((const time_t *)&seconds);

    date = PyDate_FromDate( tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday );
    if (!date)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create date object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_dateFromTicks }}}1\n");
    return (PyObject *)date;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_dateFromTicks }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_date
**
** Description:
**     Convert datetime.date from year, month, and day.
**
** Inputs:
**     self - pointer to module.
**     args - evaluates to:
**            year - years.
**            month - months.
**            day - days.
**
** Outputs:
**     None.
**
** Returns:
**     datetime.date object.    
**
** Exceptions:
**     args must evaluate to three integers.  Arguments must form a valid date. 
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


static PyObject * IIDBI_date(PyObject *self, PyObject *args) 
{
    PyObject *date = NULL;
    int year, month, day;

    DBPRINTF(DBI_TRC_RET)("IIDBI_date {{{1\n");

    if (!PyArg_ParseTuple(args, "iii", &year, &month, &day)) 
    {
        PyErr_SetString(IIDBI_InterfaceError, "invalid argruments for date constructor");
        goto errorExit;
    }

    date = PyDate_FromDate( year, month, day );
    if (!date)
    {
        PyErr_SetString(IIDBI_InterfaceError, "could not create date object from input parameters");
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_date }}}1\n");
    return (PyObject *)date;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_date }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_connect
**
** Description:
**     Connect to target database.
**
** Inputs:
**     args - connection arguments. 
**
** Outputs:
**     None.
**
** Returns:
**     Connection object.         
**
** Exceptions:
**     Arguments must be valid connection arguments.  Must be able to 
**     connect to target database.  Construction of connection object
**     must succeed.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
**     28-Apr-08 grant.croker@ingres.com
**         Extend search for odbcinst.ini, look in $II_SYSTEM/ingres/files.
**         Use MAX_PATH as the maximum path length
**     10-Jul-08 grant.croker@ingres.com
**         Fix string corruption for server types
}*/

static PyObject * IIDBI_connect(PyObject *self, PyObject *args, 
    PyObject *keywords) 
{
    char *dsn=NULL;
    char *database=NULL;
    char *username=NULL;
    char *password=NULL;
    char *vnode=NULL;
    char *autocommit=NULL;
    char *selectloops=NULL;
    char *servertype=NULL;
    char *driver=NULL;
    char *rolename=NULL;
    char *rolepwd=NULL;
    char *group=NULL;
    char *blankdate=NULL;
    char *date1582=NULL;
    char *catconnect=NULL;
    char *numeric_overflow=NULL;
    char *catschemanull=NULL;
    char *dbms_pwd=NULL;
    char *connectstr=NULL;
    char *pooled=NULL;
    PyObject *traceObject=NULL;
    int  trace = 0;
    char *traceFile=NULL;
    IIDBI_CONNECTION *conn=NULL;
    IIDBI_DBC *IIDBIpdbc = dbi_newdbc();
    RETCODE rc=DBI_SQL_SUCCESS;
    int i;
    int result = FALSE;
    struct stat buf;
    char odbcconfig[MAX_PATH];
    char *ii_system;
    static char *kwlist[] = 
    {
        "dsn", "database", "vnode", "uid", "pwd", "autocommit", "selectloops",
        "servertype","driver","rolename","rolepwd", "group", "blankdate", "date1582", "catconnect", "numeric_overflow", "catschemanull", "dbms_pwd", "connectstr", "pooled", "trace", NULL
    };
    /* The ODBC driver is restricted to the following server classes */
    static char *servertypes[] =
    {
        "INGRES", "DCOM", "IDMS", "DB2", "IMS", "VSAM", "RDB", "STAR", "RMS",
        "ORACLE", "INFORMIX", "SYBASE", "ALB","INGDSK", "MSSQL", "ODBC", 
        "DB2UDB"
    };
    int servertypesLen = sizeof(servertypes) / sizeof(servertypes[0]);
    char *tmp_servertype;

    if (!PyArg_ParseTupleAndKeywords(args, keywords, "|ssssssssssssssssssssO", kwlist, 
        &dsn, &database, &vnode, &username, &password, &autocommit,
        &selectloops, &servertype, &driver, &rolename, &rolepwd, &group,
        &blankdate, &date1582, &catconnect, &numeric_overflow, &catschemanull, 
        &dbms_pwd, &connectstr, &pooled, &traceObject))
    {
        PyErr_SetString(IIDBI_InterfaceError, "usage: connection.connect(dsn=dsnname, database=dbname, uid=username, pwd=password, autocommit=Y|N, selectloops=Y|N, servertype=serverType, driver=server, rolename=rolename, rolepwd=rolepwd, group=group, catconnect=Y | N, numeric_overflow=Y|N, catschmeanull=Y|N, dbms_pwd=dbms_pwd, connectstr=connectionString, pooled=Y|N, [trace=traceLevel]");
        goto errorExit;
    }
    if (traceObject)
    {
        if (!PyArg_ParseTuple(traceObject, "i|z", &trace, &traceFile)) 
        {
            PyErr_SetString(IIDBI_InterfaceError, "format of trace is (traceLevel,[traceFile])");
            goto errorExit;
        }
        if (!dbi_trace(trace, traceFile))
        {
            PyErr_SetFromErrnoWithFilename(IIDBI_IOError, traceFile);
            goto errorExit;
        }
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_connect {{{1\n");

    if ((conn = IIDBI_connConstructor()) != NULL)
    {
        conn->trace = trace;
        if (connectstr)
            conn->connectstr = strdup(connectstr);
        else
        {
            if (dsn)
                conn->dsn = strdup(dsn);
            if (username)
                conn->uid = strdup(username);
            if (password)
                conn->pwd = strdup(password);
            if (database)
                conn->database = strdup(database);
            if (vnode)
                conn->vnode = strdup(vnode);
            else if (!dsn)
                conn->vnode = strdup("(LOCAL)");
            
            if (autocommit)
                conn->autocommit=checkBooleanArg(autocommit);
    
            if (selectloops)
                conn->selectloops=checkBooleanArg(selectloops);
    
            if (servertype)
            {
                tmp_servertype = strdup(servertype);
                for (i = 0; i < (int)strlen(tmp_servertype); i++)
                {
                    tmp_servertype[i] = toupper(tmp_servertype[i]);
                }
                for (i = 0; i < servertypesLen; i++)
                {
                    if (!strcmp(tmp_servertype, servertypes[i]))
                    {
                        conn->servertype = tmp_servertype;
                        break;
                    }
                }
                if (i == servertypesLen)
                {
                    PyErr_SetString(IIDBI_InterfaceError, "Invalid servertype.\nMust be one of INGRES, DCOM, IDMS, DB2, IMS, VSAM, RDB, STAR, \nRMS, ORACLE, INFORMIX, SYBASE, ALB, INGDSK, MSSQL, ODBC, DB2UDB");
                    goto errorExit;
                }
            }
    
            if (driver)
                conn->driver = strdup(driver);        
            if (rolename)
                conn->rolename = strdup(rolename);        
            if (rolepwd)
                conn->rolepwd = strdup(rolepwd);        
            if (group)
                conn->group = strdup(group);        
            if (blankdate)
                conn->blankdate=checkBooleanArg(blankdate);
            if (date1582)
                conn->date1582=checkBooleanArg(date1582);
            if (catconnect)
                conn->catconnect=checkBooleanArg(catconnect);
            if (numeric_overflow)
                conn->numeric_overflow=checkBooleanArg(numeric_overflow);
            if (catschemanull)
                conn->catschemanull=checkBooleanArg(catschemanull);
            if (dbms_pwd)
                conn->dbms_pwd = strdup(dbms_pwd);
            if (pooled)
                conn->pooled=checkBooleanArg(pooled);
        }  /* if (connectstr) */
        
        IIDBIpdbc->conn = (void *)conn;
        if (!IIDBIenv.hdr.handle)
        {
#ifndef WIN32
            /* The Ingres ODBC CLI expects /usr/local/etc/odbcinst.ini to exist */
            /* if ODBCSYSINI is not defined. This is only true if unixODBC has */
            /* been built and installed from source. Redhat, SuSE, Debian etc. */
            /* typically use /etc or /etc/unixODBC. Ingres tends to keep a copy */
            /* of odbcinst.ini in $II_SYSTEM/ingres/files if it was not installed */
            /* to /usr/local/etc. */
            if ((ii_system = getenv("II_SYSTEM")) == NULL)
            {
                PyErr_SetString(IIDBI_Error, "II_SYSTEM is not set");
                goto errorExit;
            }
            if (getenv("ODBCSYSINI") == NULL)
            {
                snprintf(odbcconfig, MAX_PATH,"/usr/local/etc/odbcinst.ini");
                if (stat(odbcconfig, &buf)== -1)
                {
                    /* Try $II_SYSTEM/ingres/files/odbcinst.ini */
                    snprintf(odbcconfig, MAX_PATH, "%s/ingres/files/odbcinst.ini", ii_system);
                    if (stat(odbcconfig, &buf) == -1)
                    {
                        PyErr_SetString(IIDBI_Error, "Unable to load odbc configuration, please set the environment variable ODBCSYSINI.");
                        goto errorExit;
                    }
                    else
                    {
                        /* Use $II_SYSTEM/ingres/files/odbcinst.ini */
                        sprintf(odbcconfig,"%s/ingres/files", ii_system);
                        setenv("ODBCSYSINI",odbcconfig, 1);
                    }
                }
            }
#endif
            rc = dbi_alloc_env(&IIDBIenv, IIDBIpdbc);
   
            if (rc != DBI_SQL_SUCCESS)
            {
                result = IIDBI_mapError2exception((PyObject *)self,
                    &IIDBIpdbc->hdr.err, rc, NULL);
                goto errorExit;
            }
            IIDBIpdbc->env = IIDBIenv.hdr.handle;
        }
        else
        {
            IIDBIpdbc->env = IIDBIenv.hdr.handle;
        }

        Py_BEGIN_ALLOW_THREADS
        rc = dbi_connect(IIDBIpdbc);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
        {
            result = IIDBI_mapError2exception((PyObject *)conn, &IIDBIpdbc->hdr.err, 
                rc, NULL); 
            goto errorExit;
        }
        conn->IIDBIpdbc = IIDBIpdbc;
        goto exitLabel;
    }
    else
    {
        PyErr_SetString(IIDBI_Error, "Could not instantiate connection object");
        goto errorExit;
    }

exitLabel:
    DBPRINTF(DBI_TRC_RET)("IIDBI_connect }}}1\n");
    return (PyObject *)conn;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_connect }}}1\n");
    if (result)
    {        
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_connConstructor
**
** Description:
**     Constructor for ingresdbi.connection object.
**
** Inputs:
**     None. 
**
** Outputs:
**     None.
**
** Returns:
**    Connection object if successful, NULL on failure.         
**
** Exceptions:
**    None.         
**
** Side Effects:
**     Takes on references from module exception objects.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static IIDBI_CONNECTION *IIDBI_connConstructor()
{
    IIDBI_CONNECTION *conn = NULL;

    DBPRINTF(DBI_TRC_RET)("IIDBI_connConstructor {{{1\n");

    if ((conn = PyObject_New(IIDBI_CONNECTION, &IIDBI_connectType))) 
    {
        conn->IIDBIpdbc = NULL;
        conn->messages = PyList_New(0);
        conn->errorhandler = Py_None;
        Py_INCREF(conn->errorhandler);
        conn->closed = 0;
        conn->dsn = NULL;
        conn->database = NULL;
        conn->uid = NULL;
        conn->pwd = NULL;
        conn->vnode = NULL;
        conn->autocommit = FALSE;
        conn->selectloops = FALSE;
        conn->servertype = NULL;
        conn->driver = NULL;
        conn->rolename = NULL;
        conn->rolepwd = NULL;
        conn->group = NULL;
        conn->blankdate = FALSE;
        conn->date1582 = FALSE;
        conn->catconnect = FALSE;
        conn->numeric_overflow = FALSE;
        conn->catschemanull = FALSE;
        conn->dbms_pwd = NULL;
        conn->connectstr = NULL;
        conn->trace = 0;
        conn->pooled = FALSE;

        conn->Error = IIDBI_Error;
        Py_INCREF(conn->Error);

        conn->InterfaceError = IIDBI_InterfaceError;
        Py_INCREF(conn->InterfaceError);

        conn->Warning = IIDBI_Warning;
        Py_INCREF(conn->Warning);

        conn->DatabaseError = IIDBI_DatabaseError;
        Py_INCREF(conn->DatabaseError);

        conn->InternalError =IIDBI_InternalError;
        Py_INCREF(conn->InternalError);

        conn->OperationalError = IIDBI_OperationalError;
        Py_INCREF(conn->OperationalError);

        conn->ProgrammingError =IIDBI_ProgrammingError;
        Py_INCREF(conn->ProgrammingError);

        conn->IntegrityError = IIDBI_IntegrityError;
        Py_INCREF(conn->IntegrityError);

        conn->DataError = IIDBI_DataError;
        Py_INCREF(conn->DataError);

        conn->NotSupportedError = IIDBI_NotSupportedError;
        Py_INCREF(conn->NotSupportedError);

        conn->IOError = IIDBI_IOError;
        Py_INCREF(conn->IOError);
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_connConstructor }}}1\n");
    return conn;
}

/*{
** Name: IIDBI_connCursor
**
** Description:
**     Connection method for creating a cursor object.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**
** Returns:
**     Cursor object.         
**
** Exceptions:
**     Connection object must not be closed.  Cursor object must be
**     constructed.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connCursor(IIDBI_CONNECTION *self)
{
    IIDBI_CURSOR *cursor = NULL;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("IIDBI_ConnCursor 2{{{1\n");

    if (self->closed)
    {
        exception = IIDBI_Error; 
        errMsg = "connection is closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    if ((cursor = IIDBI_cursorConstructor()) != NULL)
    {
        cursor->connection = self;
        Py_INCREF(cursor->connection);
        cursor->closed = FALSE;
        goto exitLabel;
    }
    else
    {
        exception = IIDBI_Error;
        errMsg = "Could not instantiate cursor object";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

exitLabel:
    DBPRINTF(DBI_TRC_RET)("IIDBI_connCursor }}}1\n");
    return (PyObject *)cursor;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_connCursor }}}1\n");
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_cursorConstructor
**
** Description:
**     Cursor constructor.
**
** Inputs:
**     None. 
**
** Outputs:
**     None.
**
** Returns:
**     Cursor object on success, NULL on failure.    
**
** Exceptions:
**     None.         
**
** Side Effects:
**     Accepts reference to parent connection object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static IIDBI_CURSOR *IIDBI_cursorConstructor()
{
    IIDBI_CURSOR *cursor;

    DBPRINTF(DBI_TRC_RET)("IIDBI_cursorConstructor {{{1\n");

    if ((cursor = PyObject_New(IIDBI_CURSOR, &IIDBI_cursorType))) 
    {
        cursor->rowcount = -1;
        cursor->rownumber = Py_None;
        Py_INCREF(cursor->rownumber);
        cursor->rowindex = 0;
        cursor->arraysize = 1;
        cursor->IIDBIpstmt = NULL;
        cursor->closed = 0;
        cursor->description = Py_None;
        Py_INCREF(cursor->description);
        cursor->inputSegmentSize = NULL;
        cursor->inputSegmentLen = 0;
        cursor->outputSegmentSize = 0;
        cursor->outputColumnIndex = 0;
        cursor->messages = PyList_New(0);
        cursor->errorhandler = Py_None;
        Py_INCREF(cursor->errorhandler);
        cursor->prepared = Py_None;
        Py_INCREF(cursor->prepared);
        cursor->prepareRequested = FALSE;
        cursor->szSqlStr = NULL;
    }
    else
        goto errorExit;

    DBPRINTF(DBI_TRC_RET)("IIDBI_cursorConstructor }}}1\n");

    return cursor;

errorExit:
    DBPRINTF(DBI_TRC_RET)("IIDBI_cursorConstructor }}}1\n");
    return NULL;
}

/*{
** Name: IIDBI_cursorGetConnection
**
** Description:
**     Get method for cursor returning parent connection object.
**
** Inputs:
**     self - cursor object. 
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**    New reference to connection object.        
**
** Exceptions:
**     None.         
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetConnection(IIDBI_CURSOR *self, void *closure)
{
    IIDBI_CONNECTION *connection;

    IIDBI_handleWarning ( "DB-API extension cursor.connection used",
        NULL) ;

    connection = self->connection;
    Py_INCREF(connection);
    return (PyObject *)connection;
} 

/*{
** Name: IIDBI_cursorSetConnection
**
** Description:
**     Set a cursor.connection object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     -1 (error).         
**
** Exceptions:
**     Always returns an exception; read-only attribute.         
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetConnection(IIDBI_CURSOR *self, PyObject * value, void *closure)
{
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    exception = IIDBI_InterfaceError; 
    errMsg =  "cursor.connection is a read-only attribute";
    result = IIDBI_handleError((PyObject *)self, exception, errMsg);
    Py_DECREF(value);
    if (result)
        return 0;
    else
        return -1;
}

/*{
** Name: IIDBI_getRownumber
**
** Description:
**     Get a cursor.rownumber object.
**
** Inputs:
**     self - cursor object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference to cursor.rownumber.        
**
** Exceptions:
**     None.        
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetRownumber(IIDBI_CURSOR *self, void *closure)
{
    char *errMsg = "DB-API extension cursor.rownumber used"; 

    IIDBI_handleWarning ( errMsg, NULL ) ;
    Py_INCREF(self->rownumber);
    return self->rownumber;
} 

/*{
** Name: IIDBI_cursorSetRownumber
**
** Description:
**     Set a cursor.rownumber object.
**
** Inputs:
**     self - cursor object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     -1 (error).         
**
** Exceptions:
**     Always returns an exception; read-only attribute.         
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetRownumber(IIDBI_CURSOR *self, PyObject *value, 
    void *closure)
{
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    exception = IIDBI_InterfaceError; 
    errMsg =  "rownumber is a read-only attribute";
    result = IIDBI_handleError((PyObject *)self, exception, errMsg);
    Py_DECREF(value);
    if (result)
        return 0;
    else
        return -1;
}

/*{
** Name: IIDBI_cursorGetMessages
**
** Description:
**     Get a cursor.messages object.
**
** Inputs:
**     self - cursor object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetMessages(IIDBI_CURSOR *self, void *closure) 
{
    char *errMsg = "DB-API extension cursor.messages used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->messages);
    return self->messages;
}

/*{
** Name: IIDBI_cursorSetMessages
**
** Description:
**     Set a cursor.messages object.
**
** Inputs:
**     self - cursor object.
**     value - new message object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetMessages(IIDBI_CURSOR *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension cursor.messages used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->messages);
    Py_INCREF(value);
    self->messages = value;

    return 0;
}

/*{
** Name: IIDBI_cursorGetLastRowId
**
** Description:
**     Get a cursor.lastrowid object.
**
** Inputs:
**     self - cursor object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     -1 (error).         
**
** Exceptions:
**     Always returns an exception; not supported.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetLastRowId(IIDBI_CURSOR *self, void *closure)
{
    char *errMsg;
    PyObject *exception;
    int result;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_GetLastRowId {{{1\n", self);
    IIDBI_handleWarning ( "DB-API extension cursor.lastrowid() used", 
        NULL ) ;
    errMsg = "cursor.lastrowid is not supported in Ingres";
    exception = IIDBI_NotSupportedError;
    result = IIDBI_handleError((PyObject *)self, exception, errMsg);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorGetLastRowId }}}1\n", self);
    if (result)
        return Py_None;
    else
        return((PyObject *)NULL);
}

/*{
** Name: IIDBI_cursorSetLastRowId
**
** Description:
**     Set a cursor.lastrowid object.
**
** Inputs:
**     self - cursor object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     -1 (error).         
**
** Exceptions:
**     Always returns an exception; not supported.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetLastRowId(IIDBI_CURSOR *self, PyObject *value,
    void *closure)
{
    char *errMsg;
    PyObject *exception;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_SetLastRowId {{{1\n", self);
    IIDBI_handleWarning ( "DB-API extension cursor.lastrowid() used", 
        NULL ) ;
    errMsg = "cursor.lastrowid is not supported in Ingres";
    exception = IIDBI_NotSupportedError;
    result = IIDBI_handleError((PyObject *)self, exception, errMsg);
      
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetLastRowId }}}1\n", self);

    Py_DECREF(value);
    if (result)
        return 0;
    else
        return(-1);
}

/*{
** Name: IIDBI_cursorDestructor
**
** Description:
**     Construct a cursor object.  
**
** Inputs:
**     self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**    void.        
**
** Exceptions:
**    None.     
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static void IIDBI_cursorDestructor(IIDBI_CURSOR *self)
{
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorDestructor {{{1\n", self);

    if (!self->closed)
        IIDBI_cursorCleanup(self);

    Py_XDECREF(self->description);
    Py_XDECREF(self->rownumber);
    Py_XDECREF(self->connection);
    Py_XDECREF(self->messages);
    Py_XDECREF(self->errorhandler);
    Py_XDECREF(self->prepared);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorDestructor }}}1\n", self);

    self->ob_type->tp_free((PyObject*)self);
}

/*{
** Name: IIDBI_cursorPrint
**
** Description:
**     Print cursor name to standard output.
**
** Inputs:
**     self - cursor object.
**     fp - pointer to standard output.
**     flags - I/O modifiers.
**
** Outputs:
**     None.
**
** Returns:
**         
** Exceptions:
**         
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


static int IIDBI_cursorPrint(IIDBI_CURSOR *self, FILE *fp, int flags)
{
    if (flags & Py_PRINT_RAW) 
    {
        fprintf(fp, "<ingresdbi.cursor object>");
    }
    else 
    {
        fprintf(fp, "\"<ingresdbi.cursor object>\"");
    }
    
    return 0;
}

/*{
** Name: initingresdbi
**
** Description:
**     Module constructor for ingresdbi.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**
** Returns:
**     ingresdbi module.    
**
** Exceptions:
**     Must initialize connection and cursor types successfully.
**     Must construct module error exception objects successfully.  Must
**     import Decimal module successfully and obtain Decimal type
**     successfully.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

PyMODINIT_FUNC SQL_API initingresdbi(void)
{
    char *apilevel = DBI_APILEVEL;
    char *paramstyle= DBI_PARAMSTYLE;
    int threadsafety = DBI_THREADSAFETY;
    int string = DBI_STRING_TYPE;
    int binary = DBI_BINARY_TYPE; 
    int number = DBI_NUMBER_TYPE;
    int datetime = DBI_DATETIME_TYPE;
    int rowid = DBI_ROWID_TYPE;
    PyObject *decimalmod;

    char *version= DBIVERSION;

    if (PyType_Ready(&IIDBI_connectType) < 0 && 
        PyType_Ready(&IIDBI_cursorType) < 0)
        return;

    IIDBI_module = Py_InitModule3( "ingresdbi", IIDBI_methods, ingresdbi_doc);

    PyModule_AddStringConstant(IIDBI_module, "apilevel", apilevel);
    PyModule_AddStringConstant(IIDBI_module, "paramstyle", paramstyle);
    PyModule_AddIntConstant(IIDBI_module, "threadsafety", threadsafety);
    PyModule_AddStringConstant(IIDBI_module, "version", version);

    PyModule_AddIntConstant(IIDBI_module, "STRING", string);
    PyModule_AddIntConstant(IIDBI_module, "BINARY", binary);
    PyModule_AddIntConstant(IIDBI_module, "NUMBER", number);
    PyModule_AddIntConstant(IIDBI_module, "DATETIME", datetime);
    PyModule_AddIntConstant(IIDBI_module, "ROWID", rowid);

    IIDBI_connectType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&IIDBI_connectType) < 0)
        return;

    Py_INCREF(&IIDBI_connectType);
    PyModule_AddObject(IIDBI_module, "connection", 
         (PyObject *)&IIDBI_connectType);

    IIDBI_cursorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&IIDBI_cursorType) < 0)
        return;

    Py_INCREF(&IIDBI_cursorType);
    PyModule_AddObject(IIDBI_module, "cursor", 
         (PyObject *)&IIDBI_cursorType);

    if (!(IIDBI_Error = PyErr_NewException("ingresdbi.Error", 
        PyExc_StandardError, NULL)))
        Py_FatalError("Creation of ingresdbi.Error exception failed");
    else
        Py_INCREF(IIDBI_Error);

    PyModule_AddObject(IIDBI_module, "Error", IIDBI_Error);
    
    if (!(IIDBI_Warning = PyErr_NewException("ingresdbi.Warning", 
        PyExc_StandardError, NULL)))
        Py_FatalError("Creation of ingresdbi.Warning exception failed");
    else
        Py_INCREF(IIDBI_Warning);

    PyModule_AddObject(IIDBI_module, "Warning", IIDBI_Warning);
    
    if (!(IIDBI_InterfaceError = PyErr_NewException("ingresdbi.InterfaceError", 
        IIDBI_Error, NULL)))
        Py_FatalError("Creation of ingresdbi.InterfaceError exception failed");
    else
        Py_INCREF(IIDBI_InterfaceError);

    PyModule_AddObject(IIDBI_module, "InterfaceError", IIDBI_InterfaceError);
    
    if (!(IIDBI_DatabaseError = 
        PyErr_NewException("ingresdbi.IIDBI_DatabaseError", 
        IIDBI_Error, NULL)))
        Py_FatalError(
            "Creation of ingresdbi.IIDBI_DatabaseError exception failed");
    else
        Py_INCREF(IIDBI_DatabaseError);

    PyModule_AddObject(IIDBI_module, "DatabaseError", IIDBI_DatabaseError);
    
    if (!(IIDBI_DataError = PyErr_NewException("ingresdbi.DataError", 
        IIDBI_DatabaseError, NULL)))
        Py_FatalError("Creation of ingresdbi.DataError exception failed");
    else
        Py_INCREF(IIDBI_DataError);

    PyModule_AddObject(IIDBI_module, "DataError", IIDBI_DataError);
    
    if (!(IIDBI_OperationalError = 
        PyErr_NewException("ingresdbi.OperationalError", 
        IIDBI_DatabaseError, NULL)))
        Py_FatalError(
            "Creation of ingresdbi.OperationalError exception failed");
    else
        Py_INCREF(IIDBI_OperationalError);

    PyModule_AddObject(IIDBI_module, "OperationalError", IIDBI_OperationalError);
    
    if (!(IIDBI_IntegrityError = PyErr_NewException("ingresdbi.IntegrityError", 
        IIDBI_DatabaseError, NULL)))
        Py_FatalError("Creation of ingresdbi.IntegrityError exception failed");
    else
        Py_INCREF(IIDBI_IntegrityError);

    PyModule_AddObject(IIDBI_module, "IntegrityError", IIDBI_IntegrityError);
    
    if (!(IIDBI_InternalError = PyErr_NewException("ingresdbi.InternalError", 
        IIDBI_DatabaseError, NULL)))
        Py_FatalError("Creation of ingresdbi.InternalError exception failed");
    else
        Py_INCREF(IIDBI_InternalError);

    PyModule_AddObject(IIDBI_module, "InternalError", IIDBI_InternalError);
    
    if (!(IIDBI_NotSupportedError = 
        PyErr_NewException("ingresdbi.NotSupportedError", 
        IIDBI_DatabaseError, NULL)))
            Py_FatalError("Creation of ingresdbi.NotSupportedError exception failed");
    else
        Py_INCREF(IIDBI_NotSupportedError);

    PyModule_AddObject(IIDBI_module, "NotSupportedError", IIDBI_NotSupportedError);
    
    if (!(IIDBI_ProgrammingError = 
        PyErr_NewException("ingresdbi.ProgrammingError", 
        IIDBI_DatabaseError, NULL)))
            Py_FatalError("Creation of ingresdbi.ProgrammingError exception failed");
    else
        Py_INCREF(IIDBI_ProgrammingError);

    PyModule_AddObject(IIDBI_module, "ProgrammingError", IIDBI_ProgrammingError);

    if (!(IIDBI_IOError = PyErr_NewException("ingresdbi.IOError", 
        IIDBI_IOError, NULL)))
        Py_FatalError("Creation of ingresdbi.IOError exception failed");
    else
        Py_INCREF(IIDBI_IOError);

    PyModule_AddObject(IIDBI_module, "IOError", IIDBI_IOError);
    
    PyDateTime_IMPORT;

    decimalmod = PyImport_ImportModule("decimal");
    if (!decimalmod)
        Py_FatalError("Import of decimal module failed");

    decimalType = PyObject_GetAttrString(decimalmod, "Decimal");
    Py_DECREF(decimalmod);

    if (!decimalType)
        Py_FatalError("Cannot get decimal module attributes");

    IIDBIenv.hdr.handle = NULL;
}

/*{
** Name: IIDBI_connGetMessages
**
** Description:
**     Get connection.messages object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetMessages(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.messages used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->messages);
    return self->messages;
}

/*{
** Name: IIDBI_connSetMessages
**
** Description:
**     Update connection.messages object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetMessages(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.messages used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->messages);
    self->messages = value;
    Py_INCREF(value);

    return 0;
}

/*{
** Name: IIDBI_connGetError
**
** Description:
**     Get connection.Error exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.Error object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.Error used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->Error);
    return self->Error;
}

/*{
** Name: IIDBI_connSetError
**
** Description:
**     Update connection.Error object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.Error used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->Error);
    Py_INCREF(value);
    self->Error = value;

    return 0;
}

/*{
** Name: IIDBI_connGetInterfaceError
**
** Description:
**     Get connection.EInterfacerror exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.InterfaceError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetInterfaceError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.InterfaceError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->InterfaceError);
    return self->InterfaceError;
}

/*{
** Name: IIDBI_connSetInterfaceError
**
** Description:
**     Update connection.InterfaceRrror object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetInterfaceError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.InterfaceError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->InterfaceError);
    Py_INCREF(value);
    self->InterfaceError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetDatabaseError
**
** Description:
**     Get connection.DatabaseError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.DatabaseError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetDatabaseError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.DatabaseError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->DatabaseError);
    return self->DatabaseError;
}

/*{
** Name: IIDBI_connSetDatabaseError
**
** Description:
**     Update connection.DatabaseError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetDatabaseError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.DatabaseError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->DatabaseError);
    Py_INCREF(value);
    self->DatabaseError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetInternalError
**
** Description:
**     Get connection.InternalError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.InternalError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetInternalError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.InternalError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->InternalError);
    return self->InternalError;
}

/*{
** Name: IIDBI_connSetInternalError
**
** Description:
**     Update connection.InternalError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetInternalError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.InternalError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->InternalError);
    Py_INCREF(value);
    self->InternalError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetOperationalError
**
** Description:
**     Get connection.OperationalError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.OperationalError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetOperationalError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.OperationalError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->OperationalError);
    return self->OperationalError;
}

/*{
** Name: IIDBI_connSetOperationalError
**
** Description:
**     Update connection.OperationalError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetOperationalError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.OperationalError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->OperationalError);
    Py_INCREF(value);
    self->OperationalError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetProgrammingError
**
** Description:
**     Get connection.ProgrammingError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.ProgrammingError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetProgrammingError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.ProgrammingError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->ProgrammingError);
    return self->ProgrammingError;
}

/*{
** Name: IIDBI_connSetProgrammingError
**
** Description:
**     Update connection.ProgrammingError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetProgrammingError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.ProgrammingError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->ProgrammingError);
    Py_INCREF(value);
    self->ProgrammingError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetIntegrityError
**
** Description:
**     Get connection.IntegrityError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.IntegrityError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetIntegrityError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.IntegrityError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->IntegrityError);
    return self->IntegrityError;
}

/*{
** Name: IIDBI_connSetIntegrityError
**
** Description:
**     Update connection.IntegrityError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetIntegrityError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.IntegrityError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->IntegrityError);
    Py_INCREF(value);
    self->IntegrityError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetDataError
**
** Description:
**     Get connection.DataError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.DataError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetDataError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.DataError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->DataError);
    return self->DataError;
}

/*{
** Name: IIDBI_connSetDataError
**
** Description:
**     Update connection.DataError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetDataError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.DataError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->DataError);
    Py_INCREF(value);
    self->DataError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetNotSupportedError
**
** Description:
**     Get connection.NotSupportedError exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.NotSupportedError object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetNotSupportedError(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.NotSupportedError used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->NotSupportedError);
    return self->NotSupportedError;
}

/*{
** Name: IIDBI_connSetNotSupportedError
**
** Description:
**     Update connection.NotSupportedError object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetNotSupportedError(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.NotSupportedError used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->NotSupportedError);
    Py_INCREF(value);
    self->NotSupportedError = value;

    return 0;
}

/*{
** Name: IIDBI_connGetWarning
**
** Description:
**     Get connection.Warning exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     Owns new reference to ingresdbi.Warning object.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetWarning(IIDBI_CONNECTION *self, void *closure) 
{
    char *errMsg = "DB-API extension connection.Warning used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->Warning);
    return self->Warning;
}

/*{
** Name: IIDBI_connSetWarning
**
** Description:
**     Update connection.Warning object.
**
** Inputs:
**     self - connection object.
**     value - target value to update.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     zero.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connSetWarning(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.Warning used";
    
    IIDBI_handleWarning( errMsg, NULL);
    Py_DECREF(self->Warning);
    Py_INCREF(value);
    self->Warning = value;

    return 0;
}

/*{
** Name: IIDBI_connGetErrorHandler
**
** Description:
**     Get connection.errorhandler exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connGetErrorHandler(IIDBI_CONNECTION *self, void 
    *closure) 
{
    char *errMsg = "DB-API extension connection.errorhandler used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->errorhandler);
    return self->errorhandler;
}

/*{
** Name: IIDBI_connSetErrorHandler
**
** Description:
**     Set the error handler object.
**
** Inputs:
**     self - connection object.
**     value - reference to error handler object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**         
** Exceptions:
**     Value argument must be callable.
**
** Side Effects:
**     IIDBI_handleError() will return TRUE if this routine is successfully 
**     called.  This flags cursor and connection objects not to follow the
**     exception protocol.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


static int IIDBI_connSetErrorHandler(IIDBI_CONNECTION *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension connection.errorhandler used";
    PyObject *exception;

    IIDBI_handleWarning( errMsg, NULL);
    if (!PyCallable_Check(value))
    {
        errMsg = "connection.errorhandler attribute must be callable";
        exception = self->ProgrammingError;
        IIDBI_handleError((PyObject *)self, exception, errMsg);
        return -1;
    }
    Py_DECREF(self->errorhandler);  /* Dispose of previous callback */
    Py_INCREF(value);               /* Add a reference to new callback */
    self->errorhandler = value;     /* Remember new callback */

    return 0;
}

/*{
** Name: IIDBI_cursorGetErrorHandler
**
** Description:
**     Get cursor.errorhandler exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetErrorHandler(IIDBI_CURSOR *self, void 
    *closure) 
{
    char *errMsg = "DB-API extension cursor.errorhandler used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->errorhandler);
    return self->errorhandler;
}

/*{
** Name: IIDBI_cursorSetErrorHandler
**
** Description:
**     Set the error handler object.
**
** Inputs:
**     self - cursor object.
**     value - reference to error handler object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**         
** Exceptions:
**     Value argument must be callable.
**
** Side Effects:
**     IIDBI_handleError() will return TRUE if this routine is successfully 
**     called.  This flags cursor and connection objects not to follow the
**     exception protocol.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetErrorHandler(IIDBI_CURSOR *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension cursor.errorhandler used";
    PyObject *exception;

    IIDBI_handleWarning( errMsg, NULL);
    if (!PyCallable_Check(value))
    {
        Py_DECREF(value);  /* Discard current object */
        errMsg = "cursor.errorhandler attribute must be callable";
        exception = IIDBI_ProgrammingError;
        IIDBI_handleError((PyObject *)self, exception, errMsg);
        return -1;
    }
    Py_DECREF(self->errorhandler);  /* Dispose of previous callback */
    Py_INCREF(value);               /* Add a reference to new callback */
    self->errorhandler = value;     /* Remember new callback */

    return 0;
}

/*{
** Name: IIDBI_cursorGetPrepared
**
** Description:
**     Get cursor.prepared exception object.
**
** Inputs:
**     self - connection object.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorGetPrepared(IIDBI_CURSOR *self, void 
    *closure) 
{
    char *errMsg = "DB-API extension cursor.prepared used";

    IIDBI_handleWarning(errMsg, NULL);
    Py_INCREF(self->prepared);
    return self->prepared;
}

/*{
** Name: IIDBI_setPrepared
**
** Description:
**     Set the cursor.prepared attribute.
**
** Inputs:
**     self - cursor object.
**     value - an input string: 'y', 'n', 'yes', 'no', 'on', or 'off'.
**     closure - definition data (unused).
**
** Outputs:
**     None.
**
** Returns:
**     New reference.
**
** Exceptions:
**     Value must be a string of zero to three characters.  Value must 
**     evaluate to 'y', 'n', 'yes', 'no', 'on', or 'off'.         
**
** Side Effects:
**     Forces all queries executed via cursor.execute() or cursor.executemany()
**     to be prepared.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_cursorSetPrepared(IIDBI_CURSOR *self, PyObject *value, 
    void *closure)
{
    char *errMsg = "DB-API extension cursor.prepared used";
    PyObject *exception;
    char *preparedStr;
    char tmp[4];
    int i, p, result;
    
    IIDBI_handleWarning( errMsg, NULL);
    if (!PyString_Check(value))
    {
        Py_DECREF(value); 
        errMsg = "cursor.prepared attribute must be a string";
        exception = IIDBI_ProgrammingError;
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        if (result)
            return 0;
        else
            return -1;
    }
    preparedStr = PyString_AsString(value);
    if (!strlen(preparedStr) || strlen(preparedStr) > 3)
    {
        errMsg = "cursor.prepared attribute must specify at 0 - 3 characters";
        exception = IIDBI_ProgrammingError;
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        if (result)
            return 0;
        else
            return -1;
    }
    for (i = 0; i < (int)strlen(preparedStr); i++)
    {
        p = toupper(preparedStr[i]);
        tmp[i] = (char) p;
    }
	tmp[i] = '\0';
    if (!strcmp(tmp, "Y") || !strcmp(tmp, "ON") || !strcmp(tmp, "YES"))
        self->prepareRequested = TRUE;
    else if (!strcmp(tmp, "N") || !strcmp(tmp, "OFF") || !strcmp(tmp, "NO"))
        self->prepareRequested = FALSE;
    else
    {
        errMsg = "valid settings for cursor.prepared attribute are: 'on', 'off', 'yes', 'no', 'y', or 'n'";
        exception = IIDBI_ProgrammingError;
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        if (result)
            return 0;
        else
            return -1;
    }
    Py_DECREF(self->prepared);
    Py_INCREF(value);        
    self->prepared = value; 

    return 0;
}

/*{
** Name: IIDBI_connDestructor
**
** Description:
**     Destroy an ingresdbi.connection object.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**
** Returns:
**     Void.    
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static void IIDBI_connDestructor(IIDBI_CONNECTION *self)
{
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connDestructor {{{1\n", self);

    self->closed = TRUE;
 
    IIDBI_connCleanup(self);

    Py_XDECREF(self->messages);
    Py_XDECREF(self->errorhandler);
    Py_XDECREF(self->Error);
    Py_XDECREF(self->Warning);
    Py_XDECREF(self->InterfaceError);
    Py_XDECREF(self->DatabaseError);
    Py_XDECREF(self->DataError);
    Py_XDECREF(self->OperationalError);
    Py_XDECREF(self->IntegrityError);
    Py_XDECREF(self->InternalError);
    Py_XDECREF(self->ProgrammingError);
    Py_XDECREF(self->NotSupportedError);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connDestructor }}}1\n",self);

    self->ob_type->tp_free((PyObject*)self);
}

/*{
** Name: IIDBI_connPrint
**
** Description:
**     Print connection name to standard output.
**
** Inputs:
**     self - connection object.
**     fp - pointer to standard output.
**     flags - I/O modifiers.
**
** Outputs:
**     None.
**
** Returns:
**         
** Exceptions:
**         
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static int IIDBI_connPrint(IIDBI_CONNECTION *self, FILE *fp, int flags)
{
    if (flags & Py_PRINT_RAW) 
    {
        fprintf(fp, "<ingresdbi.cursor object>");
    }
    else 
    {
        fprintf(fp, "\"<ingresdbi.cursor object>\"");
    }
    return 0;
}

/*{
** Name: IIDBI_cursorExecuteMany
**
** Description:
**     Execute queries from an array of input parameters.
**
** Inputs:
**     self - cursor object.
**     args - Evaluates to:
**            szSqlStr - query to be executed.
**            paramSet - nested sequence containing input parameters.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     Query must be executed successfully.  ParamSet must be a sequence
**     type.   
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorExecuteMany(IIDBI_CURSOR *self, PyObject *args)
{
    char *szSqlStr = NULL;
    PyObject *paramSet = NULL, *params = NULL;
    int paramSetSize = 0;
    IIDBI_STMT *IIDBIpstmt = NULL;
    IIDBI_CONNECTION *connection = NULL;
    IIDBI_DBC *IIDBIpdbc = NULL;
    RETCODE rc;
    int j;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;
 
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecuteMany }}}1\n", self);

    if (!PyArg_ParseTuple(args, "s|O", &szSqlStr, &paramSet)) 
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "usage: cursor.execute()";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  

    IIDBI_clearMessages(self->messages);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  
    if (!self->IIDBIpstmt)
    {
        IIDBIpstmt = (IIDBI_STMT *)calloc(1,sizeof(IIDBI_STMT));
        self->IIDBIpstmt = IIDBIpstmt;
    }
    else
    {
        IIDBIpstmt = self->IIDBIpstmt;
    }

    self->rowcount = -1;
    Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
    Py_INCREF(self->rownumber);
    self->rowindex = 0;

    connection = self->connection;
    if (connection == (IIDBI_CONNECTION *)Py_None)
    {
        exception = IIDBI_InternalError; 
        errMsg = "Invalid connection object";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    IIDBIpdbc = connection->IIDBIpdbc;

    if (paramSet)
    {
        paramSetSize = PyObject_Length(paramSet);
        if (paramSetSize <= 0)
        {
            exception = IIDBI_InterfaceError, 
            errMsg = "usage: cursor.executemany() parameter set";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }  
        if (!PySequence_Check(paramSet))
        {
            exception = IIDBI_InterfaceError; 
            errMsg ="parameters must be in sequence format";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        for (j = 0; j < paramSetSize; j++)
        {
            params = PySequence_GetItem(paramSet, j); 
            if (!params)
            {
                exception = IIDBI_InternalError;
                errMsg = "failed to retrieve input parameter sequence";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
            else
            {
                rc = IIDBI_sendParameters(self, params);
                switch (rc)
                {
                    case DBI_SQL_SUCCESS:
                        break;
                    
                    case DBI_SQL_SUCCESS_WITH_INFO:
                        result = 1;
                        goto errorExit;
                        break;

                    default:
                        result = 0;
                        goto errorExit;
                        break;
                 } 
            }
         
            if (self->szSqlStr == NULL)
            {
                self->szSqlStr = strdup(szSqlStr);
                IIDBIpstmt->prepareRequested = self->prepareRequested;
            }
            else if (strcmp(szSqlStr, self->szSqlStr) && self->prepareRequested)
            {
                IIDBI_handleWarning ( "DB-API query has changed, reverting to direct execution", self->messages ) ;
                IIDBIpstmt->prepareRequested = FALSE;
                IIDBIpstmt->prepareCompleted = FALSE;
            }
            else
                IIDBIpstmt->prepareRequested = self->prepareRequested;
            Py_BEGIN_ALLOW_THREADS
            rc = dbi_cursorExecute( IIDBIpdbc, IIDBIpstmt, szSqlStr, FALSE );
            Py_END_ALLOW_THREADS
            if (rc != DBI_SQL_SUCCESS)
            {
                Py_XDECREF(params);
                result = IIDBI_mapError2exception((PyObject *)self, 
                    &IIDBIpstmt->hdr.err, rc, szSqlStr);
                goto errorExit;
            }

            if (IIDBIpstmt->hasResultSet)
            {
                Py_XDECREF(params);
                exception = IIDBI_InterfaceError;
                errMsg = "result sets not allowed for executemany method";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
            else
            {
                Py_XDECREF(self->description);
                self->description = Py_None;
                Py_INCREF(self->description);
            }
            dbi_freeData(IIDBIpstmt);   
            Py_XDECREF(params);
        } /* for (i = 0; i < paramSetSize; i++) */
    } /* if (params) */
    else
    {
        Py_XDECREF(params);
        exception = IIDBI_OperationalError; 
        errMsg = "cursor.executemany() requires parameters";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    self->rowcount = paramSetSize;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecuteMany }}}1\n", self);
    
    Py_INCREF(Py_None);
    return(Py_None);

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecuteMany }}}1\n", self);
    if (self->szSqlStr)
    {
        free(self->szSqlStr);
        self->szSqlStr = NULL;
    }
    dbi_freeData(IIDBIpstmt);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_cursorExecute
**
** Description:
**     Execute query.
**
** Inputs:
**     self - cursor object.
**     args - Evaluates to:
**            szSqlStr - query to be executed.
**            params - optional sequence of input parameters.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     Cursor must be open.  Query must be executed successfully.  Params 
**     must be a sequence type.   
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


static PyObject *IIDBI_cursorExecute(IIDBI_CURSOR *self, PyObject *args)
{
    char *szSqlStr = NULL;
    PyObject *params = NULL;
    IIDBI_STMT *IIDBIpstmt = NULL;
    IIDBI_CONNECTION *connection = NULL;
    IIDBI_DBC *IIDBIpdbc = NULL;
    RETCODE rc;
    int i;
    PyObject *temp;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;
    
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecute }}}1\n", self);

    if (!PyArg_ParseTuple(args, "s|O", &szSqlStr, &params)) 
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "usage: cursor.execute()";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  

    IIDBI_clearMessages(self->messages);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
	
    if (!self->IIDBIpstmt)
    {
        IIDBIpstmt = (IIDBI_STMT *)calloc(1,sizeof(IIDBI_STMT));
        self->IIDBIpstmt = IIDBIpstmt;
    }
    else
    {
        IIDBIpstmt = self->IIDBIpstmt;
    }

    connection = self->connection;
    if (connection == (IIDBI_CONNECTION *)Py_None)
    {
        exception = IIDBI_InternalError; 
        errMsg = "Invalid connection object";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    IIDBIpdbc = connection->IIDBIpdbc;

    Py_XDECREF(self->description);
    self->description = Py_None;
    Py_INCREF(self->description);

    Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
    Py_INCREF(self->rownumber);

    self->rowcount = -1;
    self->rowindex = 0;

    IIDBIpstmt->inputSegmentSize = self->inputSegmentSize;
    IIDBIpstmt->inputSegmentLen = self->inputSegmentLen;
    IIDBIpstmt->outputSegmentSize = self->outputSegmentSize;
    IIDBIpstmt->outputColumnIndex = self->outputColumnIndex;
    if (self->szSqlStr == NULL)
    {
        self->szSqlStr = strdup(szSqlStr);
        IIDBIpstmt->prepareRequested = self->prepareRequested;
    }
    else if (strcmp(szSqlStr, self->szSqlStr) && self->prepareRequested)
    {
        IIDBI_handleWarning ( "DB-API query has changed, reverting to direct execution", self->messages ) ;
        IIDBIpstmt->prepareRequested = FALSE;
        IIDBIpstmt->prepareCompleted = FALSE;
    }
    else
        IIDBIpstmt->prepareRequested = self->prepareRequested;

    if (params)
    {
         rc = IIDBI_sendParameters(self, params);
         switch (rc)
         {
             case DBI_SQL_SUCCESS:
                 break;
                    
             case DBI_SQL_SUCCESS_WITH_INFO:
                 result = 1;
                 goto errorExit;
                 break;

             default:
                 result = 0;
                 goto errorExit;
                 break;
         } 
    }  /* if (params) */
    Py_BEGIN_ALLOW_THREADS
    rc = dbi_cursorExecute( IIDBIpdbc, IIDBIpstmt, szSqlStr, FALSE );
    Py_END_ALLOW_THREADS
    if (rc != DBI_SQL_SUCCESS)
    {
        result = IIDBI_mapError2exception((PyObject *)self, 
            &IIDBIpstmt->hdr.err, rc, szSqlStr); 
        goto errorExit;
    }
    self->rowcount = IIDBIpstmt->rowCount;

    if (IIDBIpstmt->hasResultSet)
    {
        IIDBI_DESCRIPTOR *descriptor;
        PyObject *descRow = NULL;
        int type = -1;

        Py_XDECREF(self->description);
        self->description = PyList_New(0);
        rc = dbi_allocDescriptor(IIDBIpstmt, IIDBIpstmt->descCount, 0);
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError; 
            errMsg = "Could not allocate descriptor";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        Py_BEGIN_ALLOW_THREADS
        rc = dbi_describeColumns(IIDBIpstmt);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError, 
            errMsg = "Could not describe columns";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError, 
            errMsg = "Could not allocate data";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        for (i = 0; i < IIDBIpstmt->descCount; i++)
        {
            descriptor = IIDBIpstmt->descriptor[i];
            descRow = PyTuple_New(7);
	    temp = PyString_FromString(descriptor->columnName);
            rc = PyTuple_SetItem( descRow, 0, temp);
            type = IIDBI_mapType(descriptor->type);
            if (type == DBI_UNKNOWN_TYPE)
            {
                exception = IIDBI_OperationalError; 
                errMsg = "Invalid data type";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }

            temp = PyInt_FromLong((long)type);
            rc = PyTuple_SetItem( descRow, 1, temp);

	    temp = PyInt_FromLong((long)descriptor->displaySize);
            rc = PyTuple_SetItem( descRow, 2, temp);

            temp = PyInt_FromLong((long)descriptor->internalSize);
            rc = PyTuple_SetItem( descRow, 3, temp);

            temp = PyInt_FromLong((long)descriptor->precision);
            rc = PyTuple_SetItem( descRow, 4, temp);

            temp = PyInt_FromLong((long)descriptor->scale);
            rc = PyTuple_SetItem( descRow, 5, temp);

            temp = PyInt_FromLong((long)descriptor->nullable);
            rc = PyTuple_SetItem( descRow, 6, temp);

            rc = PyList_Append( self->description, descRow);
            Py_XDECREF(descRow);
        }
    }
    else
    {
        Py_XDECREF(self->description);
        self->description = Py_None;
        Py_INCREF(self->description);
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecute }}}1\n", self);
    
    dbi_freeData(IIDBIpstmt);   
    Py_INCREF(Py_None);
    return(Py_None);

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorExecute }}}1\n", self);
    if (self->szSqlStr)
    {
        free(self->szSqlStr);
        self->szSqlStr = NULL;
    }
    dbi_freeData(IIDBIpstmt);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else
        return NULL;
}

/*{
** Name: IIDBI_cursorCallProc
**
** Description:
**     Execute a stored procedure.
**
** Inputs:
**     self - cursor object.
**     args - Evaluates to:
**            szSqlStr - name of procedure.
**            params - an optional sequence of input parameters.
**
** Outputs:
**     BYREF parameters may be modified.
**
** Returns:
**     None.
**
** Exceptions:
**     The cursor must be open. The ODBC statement handle must be non-null.
**     The cursor.prepared Query must succeed.  Cursor attribute "prepared"
**     cannot be set to 'y', 'yes', or 'on".  If the procedure returns
**     rows, the returned descriptor must be valid and rows must have valid
**     data.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorCallProc(IIDBI_CURSOR *self, PyObject *args)
{
    char *szSqlStr = NULL;
    PyObject *params = NULL;
    IIDBI_STMT *IIDBIpstmt = NULL;
    IIDBI_CONNECTION *connection = NULL;
    IIDBI_DBC *IIDBIpdbc = NULL;
    RETCODE rc;
    int i;
    int parmCount = 0;
    char szProcStr[1024];
    char *procFmt="{ call %s (";
    int j;
    short int k;
    char l;
    long m;
    PyObject *row = NULL;
    IIDBI_DESCRIPTOR **parameter = NULL;
    SQLBIGINT n;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;
    
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorCallProc }}}1\n", self);

    if (!PyArg_ParseTuple(args, "s|O", &szSqlStr, &params)) 
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "usage: cursor.callproc()";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  

    IIDBI_clearMessages(self->messages);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  
    if (self->prepareRequested)
    {
        exception = IIDBI_ProgrammingError; 
        errMsg = "prepared attribute of (on, y, yes) not valid for execution of procedures";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  
    if (!self->IIDBIpstmt)
    {
        IIDBIpstmt = (IIDBI_STMT *)calloc(1,sizeof(IIDBI_STMT));
        self->IIDBIpstmt = IIDBIpstmt;
    }
    else
    {
        IIDBIpstmt = self->IIDBIpstmt;
    }
    connection = self->connection;
    if (connection == (IIDBI_CONNECTION *)Py_None)
    {
        exception = IIDBI_InternalError; 
        errMsg = "Invalid connection object";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    IIDBIpdbc = connection->IIDBIpdbc;

    sprintf(szProcStr, procFmt, szSqlStr);
    if (params)
    {
        rc = IIDBI_sendParameters(self, params);
        switch (rc)
        {
            case DBI_SQL_SUCCESS:
                break;
                    
            case DBI_SQL_SUCCESS_WITH_INFO:
                result = 1;
                goto errorExit;
                break;

            default:
                result = 0;
                goto errorExit;
                break;
        } 
        parmCount = IIDBIpstmt->parmCount;
        for (i = 0; i < parmCount; i++)
        {
            if (i == parmCount - 1)
                strcat(szProcStr,"? ");
            else
                strcat(szProcStr," ?,");
        } 
       
        strcat(szProcStr, " ) }");

        Py_BEGIN_ALLOW_THREADS
        rc = dbi_cursorExecute( IIDBIpdbc, IIDBIpstmt, szProcStr, TRUE);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
        {
            result = IIDBI_mapError2exception((PyObject*)self, &IIDBIpstmt->hdr.err, rc, 
                szProcStr); 
            goto errorExit;
        }

        /*
        ** Return (possibly modified) parameters back to the caller.   
        */
        row = PyTuple_New(IIDBIpstmt->parmCount);

        parameter = IIDBIpstmt->parameter;
        for (i = 0; i < parmCount; i++)
        {
            switch (parameter[i]->type)
            {
            case SQL_BIGINT:
                n = *(ODBCINT64 *)parameter[i]->data;
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, PyLong_FromLongLong(n));
                break;
    
            case SQL_INTEGER:
                j = *(SQLINTEGER *)parameter[i]->data;
                m = (long)j;
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;
    
             case SQL_SMALLINT:
                k = *(SQLSMALLINT *)parameter[i]->data;
                m = (long)k;
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;
    
            case SQL_TINYINT:
                l = *(SQLCHAR *)parameter[i]->data;
                m = (long)l;
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;
      
            case SQL_FLOAT:
            case SQL_REAL:
            case SQL_DOUBLE:
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i,
                        PyFloat_FromDouble(*(double *)parameter[i]->data));
                break;
    
            case SQL_LONGVARCHAR:
            case SQL_LONGVARBINARY:
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, (PyObject *)
                        PyString_FromStringAndSize( parameter[i]->data, 
                             parameter[i]->precision));
                break;
    
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                      PyTuple_SetItem(row, i, (PyObject *)
                        PyUnicode_Decode((const char *)parameter[i]->data, 
                        (int)wcslen(parameter[i]->data)*sizeof(SQLWCHAR),
                        "utf_16","strict"));
                break;
    
            default:
                if (parameter[i]->isNull)
                {
                    PyTuple_SetItem(row, i, Py_None);
                }
                else
                    PyTuple_SetItem(row, i, 
                        PyString_FromString(parameter[i]->data));
                break;
            }
        } /* for (i = 0; i < parmCount; i++) */
    }  /* if (params) */
    else
    {
        strcat(szProcStr, " ) }");

        Py_BEGIN_ALLOW_THREADS
        rc = dbi_cursorExecute( IIDBIpdbc, IIDBIpstmt, szProcStr, TRUE);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
        {
            result = IIDBI_mapError2exception((PyObject*)self, &IIDBIpstmt->hdr.err, 
                rc, szProcStr);
            goto errorExit;
        }
    }
    self->rowcount = IIDBIpstmt->rowCount;

    if (IIDBIpstmt->hasResultSet)
    {
        IIDBI_DESCRIPTOR *descriptor;
        PyObject *descRow = NULL;
        int type = -1;

        self->description = PyList_New(0);
        rc = dbi_allocDescriptor(IIDBIpstmt, IIDBIpstmt->descCount, 0);
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError; 
            errMsg = "Could not allocate descriptor";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        Py_BEGIN_ALLOW_THREADS
        rc = dbi_describeColumns(IIDBIpstmt);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError;
            errMsg = "Could not describe columns";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        if (rc != DBI_SQL_SUCCESS)
        {
            exception = IIDBI_OperationalError;
            errMsg = "Could not allocate data";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        for (i = 0; i < IIDBIpstmt->descCount; i++)
        {
            descriptor = IIDBIpstmt->descriptor[i];
            descRow = PyTuple_New(7);

            rc = PyTuple_SetItem( descRow, 0, 
                PyString_FromString(descriptor->columnName));
            type = IIDBI_mapType(descriptor->type);
            if (type == DBI_UNKNOWN_TYPE)
            {
                exception = IIDBI_OperationalError; 
                errMsg = "Invalid data type";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
            rc = PyTuple_SetItem( descRow, 1, PyInt_FromLong((long)type));
            rc = PyTuple_SetItem( descRow, 2, Py_None);
            rc = PyTuple_SetItem( descRow, 3, Py_None);
            rc = PyTuple_SetItem( descRow, 4, Py_None);
            rc = PyTuple_SetItem( descRow, 5, Py_None);
            rc = PyTuple_SetItem( descRow, 6, 
                PyInt_FromLong((long)descriptor->nullable));

            rc = PyList_Append( self->description, descRow);
            Py_XDECREF(descRow);
        }
    }
    else
    {
        self->description = Py_None;
        Py_INCREF(self->description);
    }

    dbi_freeData(IIDBIpstmt);   

    if (!parmCount)
    {
        Py_INCREF(Py_None);
        row = Py_None;
    }
    else
        dbi_freeDescriptor(IIDBIpstmt, 1);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorCallProc }}}1\n", self);
    return row;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorCallProc }}}1\n", self);
    dbi_freeData(IIDBIpstmt);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;

}

/*{
** Name: IIDBI_cursorFetch
**
** Description:
**     Fetch one row from the DBMS.
**
** Inputs:
**     self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     A tuple containing the data.
**     
** Exceptions:
**     The cursor must be open. The ODBC statement handle must be non-null.
**     The cursor object must have a valid descriptor and descriptor count.
**     The fetch must succeed.
**         
** Side Effects:
**     cursor->rowcount is incremented.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorFetch(IIDBI_CURSOR *self)
{
    IIDBI_STMT *IIDBIpstmt = NULL;
    RETCODE rc;
    int i;
    SQLINTEGER j;
    SQLSMALLINT k;
    SQLCHAR l;
    long m;
    SQLBIGINT n;
    PyObject *row = NULL;
    IIDBI_DESCRIPTOR **descriptor = NULL;
    SQL_TIMESTAMP_STRUCT *ts = NULL;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetch {{{1\n", self);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (!self->IIDBIpstmt)
    {
        self->rowcount = -1;
        Py_DECREF(self->rownumber);
        exception = IIDBI_InternalError;
        errMsg = "Invalid DBI statement handle";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    else
        IIDBIpstmt = self->IIDBIpstmt;

    if (!IIDBIpstmt->descCount)
    {
        self->rowcount = -1;
        exception = IIDBI_InterfaceError;
        errMsg = "No tuple descriptor count";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    
    if (!IIDBIpstmt->descriptor)
    {
        self->rowcount = -1;
        exception = IIDBI_InterfaceError;
        errMsg = "Missing tuple descriptor";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    else
        descriptor = IIDBIpstmt->descriptor;

    dbi_allocData(IIDBIpstmt);
    Py_BEGIN_ALLOW_THREADS
    rc = dbi_cursorFetchone(IIDBIpstmt);
    Py_END_ALLOW_THREADS
    if (rc == DBI_SQL_NO_DATA)
    {
        Py_XDECREF(row);
        row = Py_None;
        Py_INCREF(row);

        goto exitLabel;
    }
    else if (rc != DBI_SQL_SUCCESS)
    {
        Py_XDECREF(row);
        self->rowcount = -1;
        Py_DECREF(self->rownumber);
        self->rownumber = Py_None;
        Py_INCREF(self->rownumber);
        self->rowindex = 0;
        dbi_freeDescriptor(IIDBIpstmt, 0);
        result = IIDBI_mapError2exception((PyObject *)self, &IIDBIpstmt->hdr.err, 
            rc, NULL); 
        goto errorExit;
    }

    self->rowcount = IIDBIpstmt->rowCount;
    row = PyTuple_New(IIDBIpstmt->descCount);

    for (i = 0; i < IIDBIpstmt->descCount; i++)
    {
        if (descriptor[i]->isNull)
        {
            Py_INCREF(Py_None);
            PyTuple_SetItem(row, i, Py_None);
        }
        else
        {
            switch (descriptor[i]->type)
            {
            case SQL_BIGINT:
                n = *(ODBCINT64 *)descriptor[i]->data;
                PyTuple_SetItem(row, i, PyLong_FromLongLong(n));
                break;

            case SQL_INTEGER:
                j = *(SQLINTEGER *)descriptor[i]->data;
                m = (long)j;
                PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;

             case SQL_SMALLINT:
                k = *(SQLSMALLINT *)descriptor[i]->data;
                m = (long)k;
                PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;

            case SQL_TINYINT:
                l = *(SQLCHAR *)descriptor[i]->data;
                m = (long)l;
                PyTuple_SetItem(row, i, PyInt_FromLong(m));
                break;
      
            case SQL_FLOAT:
            case SQL_REAL:
            case SQL_DOUBLE:
                PyTuple_SetItem(row, i,
                        PyFloat_FromDouble(*(double *)descriptor[i]->data));
                break;

            case SQL_TYPE_TIME:
            case SQL_TYPE_TIMESTAMP:
            case SQL_TYPE_DATE:
            case SQL_DATE:
            case SQL_TIME:
            case SQL_TIMESTAMP:
            {
                  ts = (SQL_TIMESTAMP_STRUCT *)descriptor[i]->data;

                  PyTuple_SetItem(row, i,
                      (PyObject *)PyDateTime_FromDateAndTime( ts->year,
                      ts->month, ts->day, ts->hour, ts->minute, ts->second,
                      ts->fraction));
                break;
            }
            case SQL_LONGVARCHAR:
            case SQL_LONGVARBINARY:
                    PyTuple_SetItem(row, i, (PyObject *)
                        PyString_FromStringAndSize( descriptor[i]->data, 
                             descriptor[i]->precision));
                break;

            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                      PyTuple_SetItem(row, i, (PyObject *)
                        PyUnicode_Decode((const char *)descriptor[i]->data, 
                        (int)wcslen(descriptor[i]->data)*sizeof(SQLWCHAR),
                        "utf_16","strict"));
                break;
            case SQL_DECIMAL:
                    PyTuple_SetItem(row, i, 
                        (PyObject *)PyObject_CallFunction(decimalType, "s", descriptor[i]->data));
                    break;

            default:
                PyTuple_SetItem(row, i, 
                    PyString_FromString(descriptor[i]->data));
                break;
            }
        }
    }

exitLabel:
    dbi_freeData(IIDBIpstmt);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetch }}}1\n", self);

    if (row && row != Py_None)
        self->rowindex++;

    Py_XDECREF(self->rownumber);
    self->rownumber = PyInt_FromLong(self->rowindex);
    return row;

errorExit:
    dbi_freeData(IIDBIpstmt);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetch }}}1\n", self);
    Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
    Py_INCREF(self->rownumber);
    self->rowindex = 0;
    if (row)
    {
        Py_XDECREF(row);
    }
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_cursorNextSet
**
** Description:
**     Returns the next result set from a multi-result set stored procedure.
**
** Inputs:
**    self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     NULL.
**
** Exceptions:
**     Always returns "not supported" exception.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorNextSet(IIDBI_CURSOR *self)
{
    char *errMsg;
    PyObject *exception;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorNextSet {{{1\n", self);
    IIDBI_handleWarning ( "DB-API extension cursor.nextset() used", 
        NULL ) ;
    errMsg = "cursor.nextset() is not supported in Ingres";
    exception = IIDBI_NotSupportedError;
    IIDBI_handleError((PyObject *)self, exception, errMsg);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorNextSet }}}1\n", self);
    return(NULL);
}

/*{
** Name: IIDBI_cursorScroll
**
** Description:
**     Scroll a cursor according to the input direction and value. 
**
** Inputs:
**    self - cursor object.
**    args - evaluates to value (number of rows) and optional mode 
**           ('relative' or 'absolute')
**
** Outputs:
**     None.
**
** Returns:
**     NULL.
**
** Exceptions:
**     Always returns "not supported" exception.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorScroll(IIDBI_CURSOR *self, PyObject *args)
{
    char *errMsg;
    PyObject *exception;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorScroll {{{1\n", self);

    IIDBI_handleWarning ( "DB-API extension cursor.scroll() used", 
        NULL ) ;
    errMsg = "Not yet supported natively in Ingres";
    exception = IIDBI_NotSupportedError;
    result = IIDBI_handleError((PyObject *)self, exception, errMsg);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorScroll }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else
        return NULL;
}

/*{
** Name: IIDBI_cursorSetInputSizes
**
** Description:
**    Determines the segment size for sending blob parameters.
**
** Inputs:
**     self - cursor object.
**     args - evaluates to a sequence of input segment sizes for each
**            input parameter.  Applies only to the underlying segment size
**            for blob columns.
**
** Outputs:
**     None.
**
** Returns:
**         
** Exceptions:
**         
** Side Effects:
**     Determines segment size in SQLPutData() in the ODBC layer.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorSetInputSizes(IIDBI_CURSOR *self, PyObject *args)
{
    int len = 0;
    int i;
    PyObject *obj = NULL;
    PyObject *item = NULL;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetInputSizes {{{1\n", self);

    if (!PyArg_ParseTuple(args, "O", &obj)) 
    {
        exception = IIDBI_InterfaceError;
        errMsg = "invalid argruments for cursor.setinputsizes";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (!PySequence_Check(obj) )
    {
        exception = IIDBI_InterfaceError;
        errMsg = "argument to cursor.setinputsizes must be a sequence type";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    len = PySequence_Length( obj );

    self->inputSegmentSize = malloc(len*sizeof(int));
    self->inputSegmentLen = len;
    
    for (i = 0; i < len; i++)
    {
        item = PySequence_GetItem(obj, i);
        if (item != Py_None)
        {
            self->inputSegmentSize[i] = PyInt_AsLong(item);
            if (self->inputSegmentSize[i] < 0)
            {  
                exception = IIDBI_InterfaceError;
                errMsg = "argument has invalid type";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
        }
		else
            self->inputSegmentSize[i] = 0;
        Py_DECREF(item);
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetInputSizes }}}1\n", self);
    Py_INCREF(Py_None);
    return(Py_None);

errorExit:
    dbi_freeData(self->IIDBIpstmt);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetInputSizes }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return NULL;
}

/*{
** Name: IIDBI_cursorSetOutputSize
**
** Description:
**    Determines the segment size for fetching blob data.
**
** Inputs:
**     self - cursor object.
**     args - evaluates to output segment size, with optional index into
**            the blob column.
**
** Outputs:
**     args must evaluate to at least one, or at most two, integers.
**
** Returns:
**     None.
**         
** Exceptions:
**         
** Side Effects:
**     Determines segment size in SQLGetData() in the ODBC layer.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


static PyObject *IIDBI_cursorSetOutputSize(IIDBI_CURSOR *self, PyObject *args)
{
    int size = 0;
    int column = 0;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetOutputSize {{{1\n", self);

    if (!PyArg_ParseTuple(args, "i|i", &size, &column)) 
    {
        exception = IIDBI_InterfaceError;
        errMsg = "invalid argruments for cursor.setoutputsize";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    self->outputSegmentSize = size;
    self->outputColumnIndex = column;
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetOutputSize }}}1\n", self);

    Py_INCREF(Py_None);
    return(Py_None);

errorExit:
    dbi_freeData(self->IIDBIpstmt);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorSetOutputSize }}}1\n", self);

    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_fetchMany
**
** Description:
**     Fetch a pre-determined number of rows.
**
** Inputs:
**     self - cursor object.
**     args - evaluates to the number of rows.
**
** Outputs:
**     None.
**
** Returns:
**     A list of rows up to the specified maximum.
**    
** Exceptions:
**     Args must evaluate to an integer.  The cursor must be open. The 
**     ODBC statement handle must be non-null.  The cursor object must have 
**     a valid descriptor and descriptor count.  The fetch must succeed.
**         
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorFetchMany(IIDBI_CURSOR *self, PyObject *args)
{
    PyObject *list = NULL;
    PyObject *row = NULL;
    int dbistat = 0;
    int arraySize = 0;
    int arrayCount = 0;
    int resultSetSize = 0;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;
 
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchMany {{{1\n", self);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (!self->IIDBIpstmt)
    {
        exception = IIDBI_InternalError;
        errMsg = "Invalid DBI statement handle";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    if (self->arraysize < 0)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor.arraysize attribute must not be a negative value";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    if (!PyArg_ParseTuple(args, "|i", &resultSetSize)) 
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "usage: cursor.fetchmany([rows])";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }  

    if (resultSetSize < 0)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor.fetchmany() argument must not be a negative value";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    list = PyList_New(0);
    if (PyObject_Length(args) == 1)
        arraySize = resultSetSize;
    else
        arraySize = self->arraysize;

    while (row != Py_None && arrayCount != arraySize)
    {
        row = IIDBI_cursorFetch(self);
        if (row == NULL)
        {
            Py_DECREF(list);
            exception = IIDBI_DatabaseError;
            errMsg = "Error fetching rows";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        if (row != Py_None)
        {
            dbistat = PyList_Append(list, row);
            arrayCount++;
        }
	Py_XDECREF(row);
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchMany }}}1\n", self);
    return list;

errorExit:
    self->rowcount = -1;
        Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
        Py_INCREF(self->rownumber);
    self->rowindex = 0;
    dbi_freeData(self->IIDBIpstmt);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchMany }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_cursorFetchAll
**
** Description:
**     Fetches all rows in the target table(s).
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**
** Returns:
**     A list of tuples containing all available rows.
**     
** Exceptions:
**     Args must evaluate to an integer.  The cursor must be open. The 
**     ODBC statement handle must be non-null.  The cursor object must have 
**     a valid descriptor and descriptor count.  The fetch must succeed.
**         
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorFetchAll(IIDBI_CURSOR *self)
{
    PyObject *list = NULL;
    PyObject *row = NULL;
    int dbistat = 0;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;
 
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchAll {{{1\n", self);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    if (!self->IIDBIpstmt)
    {
        exception = IIDBI_InternalError;
        errMsg = "Invalid DBI statement handle";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    row = IIDBI_cursorFetch(self);

    if (row == NULL)
    {
        exception = IIDBI_DatabaseError;
        errMsg = "Error fetching rows";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    list = PyList_New(0);

    while (row != Py_None)
    {
        dbistat = PyList_Append(list, row);
        Py_XDECREF(row);
        row = IIDBI_cursorFetch(self);
        if (row == NULL)
        {
            Py_XDECREF(list);
            exception = IIDBI_DatabaseError;
            errMsg = "Error fetching rows";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
    }
    Py_XDECREF(row);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchAll (success) %d }}}1\n", self,          self->rowcount);
    return list;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorFetchAll (err) %d }}}1\n", self,
        self->rowcount);
    dbi_freeData(self->IIDBIpstmt);
    self->rowcount = -1;
    Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
    Py_INCREF(self->rownumber);
    self->rowindex = 0;
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_connCommit
**
** Description:
**     Commit a transaction.
**
** Inputs:
**     self - connection object.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     ODBC connection handle must be valid.  Connection object must be open.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connCommit(IIDBI_CONNECTION *self)
{
    RETCODE rc;
    IIDBI_DBC *IIDBIpdbc = self->IIDBIpdbc;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connCommit {{{1\n", self);
    if (self->closed)
    {
        exception = self->Error;
        errMsg = "connection is closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (!self->IIDBIpdbc)
    {
        exception = self->InterfaceError;
        errMsg = "Invalid DBI connection handle";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    Py_BEGIN_ALLOW_THREADS
    rc = dbi_connectionCommit(self->IIDBIpdbc);
    Py_END_ALLOW_THREADS
    if (rc != DBI_SQL_SUCCESS)
    {
        result = IIDBI_mapError2exception((PyObject *)self,&IIDBIpdbc->hdr.err, 
            rc, NULL);
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connCommit }}}1\n", self);
    Py_INCREF(Py_None);
    return Py_None;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connCommit }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_connRollback
**
** Description:
**     Roll back a transaction.
**
** Inputs:
**     self - connection object.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     ODBC connection handle must be valid.  Connection object must be open.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connRollback(IIDBI_CONNECTION *self)
{
    RETCODE rc;
    IIDBI_DBC *IIDBIpdbc = self->IIDBIpdbc;
    PyObject *exception;
    char *errMsg; 
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connRollback {{{1\n", self);

    if (self->closed)
    {
        exception = self->Error; 
        errMsg = "connection is closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (!self->IIDBIpdbc)
    {
        exception = self->InterfaceError;
        errMsg = "Invalid DBI connection handle";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    Py_BEGIN_ALLOW_THREADS
    rc = dbi_connectionRollback(self->IIDBIpdbc);
    Py_END_ALLOW_THREADS
    if (rc != DBI_SQL_SUCCESS)
    {
        result = IIDBI_mapError2exception((PyObject *)self, &IIDBIpdbc->hdr.err, 
            rc, NULL);
        goto errorExit;
    }
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connRollback }}}1\n", self);
    Py_INCREF(Py_None);
    return Py_None;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connRollback }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_mapError2Exception
**
** Description:
**     Convert an ODBC error to an ingresdbi exception object.
**
** Inputs:
**     self - connection or cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/


int IIDBI_mapError2exception(PyObject *self, IIDBI_ERROR *err, RETCODE rc, 
    char *queryText)
{
    PyObject *errObj;
    PyObject *exceptObj;
    PyObject *errTuple;
    IIDBI_CONNECTION *conn = NULL;
    IIDBI_CURSOR *cursor = NULL;
    int result = FALSE;

    if (self && PyObject_TypeCheck(self, &IIDBI_connectType))
        conn = (IIDBI_CONNECTION *)self;
    else
        cursor = (IIDBI_CURSOR *)self;

    switch (rc)
    {
    case DBI_SQL_SUCCESS_WITH_INFO:
        exceptObj = conn ? conn->Warning : IIDBI_Warning;
        break;

    case DBI_INTERNAL_ERROR:
        exceptObj = conn ? conn->OperationalError : 
            IIDBI_OperationalError;
        break;

    case DBI_SQL_ERROR:
        if (!strcmp(err->sqlState, "23501"))
            exceptObj = conn ? conn->IntegrityError :
               IIDBI_IntegrityError;
        else
            exceptObj = conn ? conn->DataError : IIDBI_DataError;
        break;

    default:
        exceptObj = conn ? conn->InternalError : IIDBI_InternalError;
        break;
    }
    if (conn && conn->errorhandler != Py_None)
    {
         result = IIDBI_handleError(self, exceptObj, err->messageText);
         goto handler_exit;
    }
    else if (cursor && cursor->errorhandler != Py_None)
    {
         result = IIDBI_handleError(self, exceptObj, err->messageText);
         goto handler_exit;
    }

    errObj = Py_BuildValue("iissz", rc, err->native, err->sqlState,  
        err->messageText, queryText);

    errTuple = Py_BuildValue("OO", exceptObj, errObj);

    PyList_Append( conn ? conn->messages : cursor->messages, errTuple);
    Py_DECREF(errTuple);

    PyErr_SetObject(exceptObj, errObj);

    return FALSE;

handler_exit:
    return result;
}

/*{
** Name: IIDBI_connClose
**
** Description:
**      Mark a connection object as closed and deallocate connection data.
**
** Inputs:
**     self - connection object.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**
** Exceptions:
**     Connection object must be open.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_connClose(IIDBI_CONNECTION *self) 
{
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connClose {{{1\n", self);

    if (self->closed)
    {
        exception = self->Error;
        errMsg = "connection is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    self->closed = TRUE;
    IIDBI_connCleanup(self);

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connClose }}}1\n", self);
    Py_INCREF(Py_None);
    return Py_None;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connClose }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    else
        return NULL;
}

/*{
** Name: IIDBI_cursorClose
**
** Description:
**     Close a cursor object.
**
** Inputs:
**     self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     None.
**         
** Exceptions:
**     Cursor object must be open.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorClose(IIDBI_CURSOR *self) 
{
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorClose {{{1\n", self);

    if (self->closed)
    {
        exception = IIDBI_InterfaceError;
        errMsg = "cursor is already closed";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
   
    self->closed = TRUE;

    if (IIDBI_cursorCleanup(self) != DBI_SQL_SUCCESS)
    {
        exception = IIDBI_InternalError;
        errMsg = "Error releasing cursor resources";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorClose }}}1\n", self);
    Py_INCREF(Py_None);
    return Py_None;

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorClose }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_cursorCleanup
**
** Description:
**     Deallocate internal cursor resources and mark closed.
**
** Inputs:
**     self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     DBI_SQL_SUCCESS;
**
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

int IIDBI_cursorCleanup(IIDBI_CURSOR *self)
{
    IIDBI_CONNECTION *connection = self->connection;
    int rc = DBI_SQL_SUCCESS;
    IIDBI_STMT *IIDBIpstmt = NULL;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorCleanup }}}1\n", self);

    IIDBIpstmt = self->IIDBIpstmt;

    self->closed = 1;

    if (!IIDBIpstmt)
        goto exitLabel;

    if (!connection->closed)
    {
        Py_BEGIN_ALLOW_THREADS
        rc = dbi_cursorClose(self->IIDBIpstmt);
        Py_END_ALLOW_THREADS
        if (rc != DBI_SQL_SUCCESS)
            goto exitLabel;
    }
    if (IIDBIpstmt->descCount)
    {
        rc = dbi_freeDescriptor(IIDBIpstmt, 0);
        if (rc != DBI_SQL_SUCCESS)
            goto exitLabel;
    }

    if (IIDBIpstmt->parmCount)
    {
        rc = dbi_freeDescriptor(IIDBIpstmt, 1);
        if (rc != DBI_SQL_SUCCESS)
            goto exitLabel;
    }

exitLabel:

    if (self->szSqlStr)
    {
         free(self->szSqlStr);
         self->szSqlStr = NULL;
    }

    IIDBI_clearMessages(self->messages);
    if (IIDBIpstmt)
	free(IIDBIpstmt);
    self->IIDBIpstmt = NULL;

    self->rowcount = -1;
    Py_XDECREF(self->rownumber);
    self->rownumber = Py_None;
    Py_INCREF(self->rownumber);
    self->rowindex = 0;

    if (self->inputSegmentSize)
        free(self->inputSegmentSize);
    self->inputSegmentSize = NULL;
    self->inputSegmentLen = 0;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorCleanup {{{1\n", self);
    return rc;
}

/*{
** Name: IIDBI_mapType
**
** Description:
**     Map an ODBC data type to one of the five ingresdbi module attribute
**     type codes: NUMBER, BINARY, STRING, UNICODE, or DATETIME.
**
** Inputs:
**     ODBC SQL type.
**
** Outputs:
**     None.
**
** Returns:
**     DBI_NUMBER_TYPE
**     DBI_BINARY_TYPE
**     DBI_STRING_TYPE
**     DBI_UNICODE_TYPE
**     DBI_DATETIME_TYPE
**
** Exceptions:
**     None.      
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

int IIDBI_mapType(int type)
{
    int retType = DBI_UNKNOWN_TYPE;

    switch (type)
    {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_NUMERIC:
       retType = DBI_STRING_TYPE;
       break;

    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
       retType = DBI_UNICODE_TYPE;
       break;
    case SQL_FLOAT:
    case SQL_REAL:
    case SQL_DOUBLE:
    case SQL_BIGINT:
    case SQL_SMALLINT:
    case SQL_TINYINT:
    case SQL_INTEGER:
    case SQL_DECIMAL:
        retType = DBI_NUMBER_TYPE;
        break;

    case SQL_TYPE_TIMESTAMP:
    case SQL_TYPE_DATE:
    case SQL_TYPE_TIME:
        retType = DBI_DATETIME_TYPE;
        break;

    case SQL_BIT:
    case SQL_BIT_VARYING:
    case SQL_LONGVARBINARY:
    case SQL_LONGVARCHAR:
    case SQL_BINARY:
    case SQL_VARBINARY:
        retType = DBI_BINARY_TYPE;
        break;

    default:
        retType = DBI_UNKNOWN_TYPE;
        break;
    }
    return retType;
}

/*{
** Name: IIDBI_cursorIterator
**
** Description:
**     Iterator method for cursor object.
**
** Inputs:
**     self - cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     New reference to cursor object.
**         
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorIterator(IIDBI_CURSOR *self) 
{
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorIterator {{{1\n", self);
    IIDBI_handleWarning ( "DB-API extension cursor.__iter__() used",
        NULL ) ;
    Py_INCREF(self); 
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorIterator }}}1\n", self);
    return ((PyObject *)self);
}

/*{
** Name: IIDBI_cursorIterNext
**
** Description:
**     Iterator method for cursor object.  Returns a row from a
**     previously executed select query.
**
** Inputs:
**     self - cursor object. 
**
** Outputs:
**     None.
**
** Returns:
**    row - a row of data from the DBMS.
**
** Exceptions:
**    Raises PyExc_StopIteration when EOD is reached.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

static PyObject *IIDBI_cursorIterNext(IIDBI_CURSOR *self)
{
    PyObject *row = NULL;
    PyObject *exception;
    char *errMsg;
    int result = FALSE;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_cursorIterNext {{{1\n", self);
    IIDBI_handleWarning ( "DB-API extension cursor.next() used",
        NULL ) ;
    row = IIDBI_cursorFetch(self);
    if (row == NULL)
    {
        exception = IIDBI_DatabaseError;
        errMsg = "Error fetching rows";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    if (row == Py_None)
    {
	Py_XDECREF(row);
        PyErr_SetString(PyExc_StopIteration, "EOD");
        goto errorExit;
    }    
 
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_iterNext }}}1\n", self);
    
    return (row);

errorExit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_iterNext }}}1\n", self);
    if (result)
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
    return NULL;
}

/*{
** Name: IIDBI_handleError
**
** Description:
**      Raise an error if self->errorhandler is NULL.  If self->errorhandler
**      is non-NULL, execute the errorhandler routine and return.  Populate
**      self->messages with error information.
**
** Inputs:
**     self - connection or cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     TRUE if self->errorhandler is non-NULL, FALSE otherwise.
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

int IIDBI_handleError(PyObject *self, PyObject *exception, char *errMsg)
{
    PyObject  *errTuple;
    IIDBI_CONNECTION *conn = NULL;
    IIDBI_CURSOR *cursor = NULL;
    PyObject *calledObj = NULL;
    PyObject *args = NULL; 
  
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_handleError {{{1\n", self);

    if (self && PyObject_TypeCheck(self, &IIDBI_connectType))
        conn = (IIDBI_CONNECTION *)self;
    else
        cursor = (IIDBI_CURSOR *)self;
     
    errTuple = Py_BuildValue("Os", exception, errMsg);
    if (conn)
       PyList_Append( conn->messages, errTuple);
    else
       PyList_Append( cursor->messages, errTuple);
    Py_XDECREF(errTuple);

    if (conn && conn->errorhandler != Py_None)
    {
        args = Py_BuildValue("OOOs",self, Py_None, exception, errMsg);
        if (args)
        {
            calledObj = PyEval_CallObject(conn->errorhandler, args);
            Py_XDECREF(args);
            if (calledObj)
            {               
                Py_XDECREF(calledObj);
                goto handler_exit;
            }
            else
            {
                exception = IIDBI_ProgrammingError;
                errMsg = "Usage: connection.errorhandler(connection, cursor, exception, errMsg)";
            }
        }
        else
        {
            exception = IIDBI_InternalError;
            errMsg = "Could not build internal arguments from connection.errorhandler()";
        }
    }
    else if (cursor && cursor->errorhandler != Py_None)
    {
        args = Py_BuildValue("OOOs",cursor->connection, 
            cursor, exception, errMsg);
        if (args)
        {
            calledObj = PyEval_CallObject(cursor->errorhandler, args);
            Py_XDECREF(args);
            if (calledObj)
            {
                 Py_XDECREF(calledObj);
                 goto handler_exit;
            }
            else
            {
                exception = IIDBI_ProgrammingError;
                errMsg = "Usage: cursor.errorhandler(connection, cursor, exception, errMsg)";
            }
        }
        else
        {
            exception = IIDBI_InternalError;
            errMsg = "Could not build internal arguments from cursor.errorhandler()";
        }
    }
    if (calledObj != Py_None) 
    {
        PyErr_SetString(exception, errMsg);

        DBPRINTF(DBI_TRC_RET)("%p: IIDBI_handleError }}}1\n", self);
        return FALSE;
    }

handler_exit:
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_handleError }}}1\n", self);
    return TRUE;
}

/*{
** Name: IIDBI_handleWarning
**
** Description:
**     Raise a warning exception and optionally populate self->messages with
**     warning information.
**
** Inputs:
**     self - connection or cursor object.
**
** Outputs:
**     None.
**
** Returns:
**     Void.    
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

void IIDBI_handleWarning(char *errMsg, PyObject *messages)
{
    PyObject  *errTuple;

    DBPRINTF(DBI_TRC_RET)("IIDBI_handleWarning {{{1\n");

    PyErr_Warn( PyExc_Warning, errMsg ) ;

    if (messages)
    {
        errTuple = Py_BuildValue("Os",PyExc_Warning, errMsg);
        PyList_Append( messages, errTuple);
        Py_DECREF(errTuple);
    }

    DBPRINTF(DBI_TRC_RET)("IIDBI_handleWarning }}}1\n");
}

/*{
** Name: IIDBI_clearMessages
**
** Description:
**     Clear the message queue.
**
** Inputs:
**     messages - a list of cursor or connection error and warning messages.
**
** Outputs:
**     None.
**
** Returns:
**     Void.    
**
** Exceptions:
**     None.    
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

void IIDBI_clearMessages(PyObject *messages)
{
    int count;

    DBPRINTF(DBI_TRC_RET)("IIDBI_clearMessages {{{1\n");

    count = PyList_Size(messages);

    if (count)
        PySequence_DelSlice(messages, 0, count);

    DBPRINTF(DBI_TRC_RET)("IIDBI_clearMessages }}}1\n");
}

/*{
** Name: IIDBI_checkBoolean
**
** Description:
**     Evaluate argument and return TRUE or FALSE.  
**
** Inputs:
**     arg - argument string.
**
** Outputs:
**     None.
**
** Returns:
**     TRUE if first character is "y" or string evaluates to "ON".
**     FALSE otherwise.
**
** Exceptions:
**     None.     
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

int checkBooleanArg(char *arg)
{
    int i;
    int result;

    for (i = 0; i < (int)strlen(arg); i++)
    {
        result = toupper(arg[i]);
        arg[i] = result;
    }
    if (!strcmp(arg, "ON") || arg[0] == 'Y')
        return TRUE;
    else
        return FALSE;
}

/*{
** Name: IIDBI_connCleanup
**
** Description:
**     Deallocate connection object resources and mark closed.
**
** Inputs:
**     self - connection object.
**
** Outputs:
**     None.
**
** Returns:
**    Void.     
**
** Exceptions:
**    None.     
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

void IIDBI_connCleanup(IIDBI_CONNECTION * self)
{
    IIDBI_DBC *IIDBIpdbc = self->IIDBIpdbc;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connCleanup {{{1\n", self);

    self->closed = TRUE;

    if (self->IIDBIpdbc) 
    {
        Py_BEGIN_ALLOW_THREADS
        dbi_connectionRollback(IIDBIpdbc);
        Py_END_ALLOW_THREADS

        Py_BEGIN_ALLOW_THREADS
        dbi_connectionClose(IIDBIpdbc);
        Py_END_ALLOW_THREADS
    }

    if (self->dsn)
    {
        free(self->dsn);
        self->dsn = NULL;
    }
    if (self->uid)
    {
        free(self->uid);
        self->uid = NULL;
    }
    if (self->pwd)
    {	   
        free(self->pwd);
	self->pwd = NULL;
    }

    if (self->database)
    {
        free(self->database);
	self->database=NULL;
    }

    if (self->vnode)
    {	    
        free(self->vnode);
	self->vnode = NULL;
    }
    if (self->servertype)
    {
        free(self->servertype);
	self->servertype = NULL;
    }

    if (self->driver)
    {
        free(self->driver);
	self->driver = NULL;
    }
 
    if (self->group)
    {
        free(self->group);
        self->group = NULL;
    }
  
    if (self->dbms_pwd)
    {
        free(self->dbms_pwd);
	self->dbms_pwd = NULL;
    }
  
    if (self->rolename)
    {
        free(self->rolename);
	self->rolename = NULL;
    }
  
    if (self->rolepwd)
    {
        free(self->rolepwd);
	self->rolepwd = NULL;
    }
  
    if (self->connectstr)
    {
        free(self->connectstr);
	self->connectstr = NULL;
    }
    
    IIDBI_clearMessages(self->messages);

    self->trace = 0;

    if (self->IIDBIpdbc)
    {
        free(self->IIDBIpdbc);
        self->IIDBIpdbc = NULL;
    }

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_connCleanup }}}1\n", self);
}

/*{
** Name: IIDBI_sendParameters
**
** Description:
**     Prepare input parameters for execution.
**
** Inputs:
**     self - cursor object. 
**     params - sequence of parameter data.
**
** Outputs:
**     None.
**
** Returns:
**     None.         
**
** Exceptions:
**     Params must be in sequence format with a size greater than zero. 
**     A parameter descriptor must be successfully allocated.
**
** Side Effects:
**     None.
**
** History:
**     26-May-06 Ralph.Loen@ingres.com
**         Created.
}*/

int IIDBI_sendParameters(IIDBI_CURSOR *self, PyObject *params)
{
    PyObject *exception;
    char *errMsg;
    int result;
    int rc;
    int parmCount;
    int i;
    int len;
    PyObject *elem = NULL;
    IIDBI_DESCRIPTOR **parameter=NULL;
    SQL_TIME_STRUCT *tm=NULL;
    SQL_DATE_STRUCT *date=NULL;
    SQL_TIMESTAMP_STRUCT *ts=NULL;
    IIDBI_STMT *IIDBIpstmt = self->IIDBIpstmt;
    void *data;

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_sendParameters {{{1\n", self);
    if (!PySequence_Check(params))
    {
        exception = IIDBI_InterfaceError; 
        errMsg = "parameters must be in sequence format";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }
    parmCount = PySequence_Size( params );
    if (parmCount == -1)
    {
        Py_XDECREF(params);
        exception = IIDBI_InternalError; 
        errMsg = "could not determine parameter count";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    if (!parmCount)
    {
        Py_XDECREF(params);
        exception = IIDBI_OperationalError; 
        errMsg = "cursor.execute() specified parameters, but the parameter count is zero";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    IIDBIpstmt->parmCount = parmCount;

    rc = dbi_allocDescriptor(IIDBIpstmt, parmCount, 1);
    if (rc != DBI_SQL_SUCCESS)
    {
        Py_XDECREF(params);
        exception = IIDBI_OperationalError;
        errMsg = "could not allocate descriptor";
        result = IIDBI_handleError((PyObject *)self, exception, errMsg);
        goto errorExit;
    }

    parameter = IIDBIpstmt->parameter;
    for (i = 0; i < parmCount; i++)
    {
        elem = PySequence_GetItem(params, i);
        if (!elem)
        {
            Py_XDECREF(params);
            exception = IIDBI_InternalError;
            errMsg = "failed to retrieve input parameter sequence";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }

        if (PyInt_Check(elem)) 
        {
            if (!parameter[i]->data)
                parameter[i]->data = (int *)calloc(1,sizeof(int));
            *(int *)parameter[i]->data = PyInt_AsLong(elem);
            parameter[i]->type = SQL_INTEGER;
            parameter[i]->cType = SQL_C_LONG;
            parameter[i]->precision = sizeof(int);
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
            parameter[i]->isNull = 0;
        }
        else if (PyLong_Check(elem)) 
        {
            if (!parameter[i]->data)
                parameter[i]->data = 
                    (ODBCINT64 *)calloc(1,sizeof(ODBCINT64));
            *(ODBCINT64 *)parameter[i]->data = PyLong_AsLongLong(elem);
            parameter[i]->type = SQL_BIGINT;
            parameter[i]->cType = SQL_C_SBIGINT;
            parameter[i]->precision = 0;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
            parameter[i]->isNull = 0;
        }
        else if (PyFloat_Check(elem)) 
        {
            if (!parameter[i]->data)
                parameter[i]->data = (double *)calloc(1,sizeof(double));
            *(double *)parameter[i]->data = PyFloat_AsDouble(elem);
            parameter[i]->type = SQL_DOUBLE;
            parameter[i]->cType = SQL_C_DOUBLE;
            parameter[i]->precision = 8;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
            parameter[i]->isNull = 0;
        }
        else if (PyString_Check(elem)) 
        {
            parameter[i]->data = PyString_AsString(elem);
            parameter[i]->type = SQL_VARCHAR;
            parameter[i]->cType = SQL_C_CHAR;
            parameter[i]->precision = 
                (int)strlen((char *)parameter[i]->data);
            parameter[i]->scale = 0;
            parameter[i]->orInd = SQL_NTS;
            parameter[i]->isNull = 0;
        }
        else if (PyUnicode_Check(elem)) 
        {
            parameter[i]->data = (void *)PyUnicode_AS_UNICODE(elem);
            parameter[i]->type = SQL_WVARCHAR;
            parameter[i]->cType = SQL_C_WCHAR;
            parameter[i]->precision = PyUnicode_GET_DATA_SIZE(elem);
            parameter[i]->scale = 0;
            parameter[i]->orInd = SQL_NTS;
            parameter[i]->isNull = 0;
        }
        else if (PyBuffer_Check(elem)) 
        {
            if (!PyObject_CheckReadBuffer( elem ) )
            {
                exception = IIDBI_InternalError; 
                errMsg = "Read buffer is not contiguous";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
            if (PyObject_AsReadBuffer( elem, (const void **)&data, &len )
               == -1 )
            {
                exception = IIDBI_InternalError; 
                errMsg = "Could not convert buffer object to pointer";
                result = IIDBI_handleError((PyObject *)self, exception, errMsg);
                goto errorExit;
            }
            parameter[i]->type = SQL_LONGVARBINARY;
            parameter[i]->cType = SQL_C_BINARY;
            parameter[i]->orInd = SQL_NTS;
            parameter[i]->data = data;
            parameter[i]->precision = len;
            parameter[i]->scale = 0;
            parameter[i]->isNull = 0;
        }
        else if (elem == Py_None)
        {
            parameter[i]->isNull = 1;
            parameter[i]->data = NULL;
            parameter[i]->type = SQL_TYPE_NULL;
            parameter[i]->cType = 0;
            parameter[i]->precision = 0;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
        }
        else if (PyDateTime_Check(elem))
        {
            parameter[i]->isNull = 0;
            ts = malloc(sizeof(SQL_TIMESTAMP_STRUCT));
            ts->year = PyDateTime_GET_YEAR(elem);
            ts->month = PyDateTime_GET_MONTH(elem);
            ts->day = PyDateTime_GET_DAY(elem);
            ts->hour = PyDateTime_DATE_GET_HOUR(elem);
            ts->minute = PyDateTime_DATE_GET_MINUTE(elem);
            ts->second = PyDateTime_DATE_GET_SECOND(elem);
        ts->fraction = 0;
            parameter[i]->data = ts;
            parameter[i]->type = SQL_TYPE_TIMESTAMP;
            parameter[i]->cType = SQL_C_TYPE_TIMESTAMP;
            parameter[i]->precision = 0;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
        }
        else if (PyDate_Check(elem))
        {
            parameter[i]->isNull = 0;
            date = malloc(sizeof(SQL_DATE_STRUCT));
            date->year = PyDateTime_GET_YEAR(elem);
            date->month = PyDateTime_GET_MONTH(elem);
            date->day = PyDateTime_GET_DAY(elem);
            parameter[i]->data = date;
            parameter[i]->type = SQL_TYPE_DATE;
            parameter[i]->cType = SQL_C_TYPE_DATE;
            parameter[i]->precision = 0;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
        }
        else if (PyTime_Check(elem))
        {
            parameter[i]->isNull = 0;
            tm = malloc(sizeof(SQL_TIME_STRUCT));
            tm->hour = PyDateTime_TIME_GET_HOUR(elem);
            tm->minute = PyDateTime_TIME_GET_MINUTE(elem);
            tm->second = PyDateTime_TIME_GET_SECOND(elem);
            parameter[i]->data = tm;
            parameter[i]->type = SQL_TYPE_TIME;
            parameter[i]->cType = SQL_C_TYPE_TIME;
            parameter[i]->precision = 0;
            parameter[i]->scale = 0;
            parameter[i]->orInd = 0;
        }
        else if (PyObject_IsInstance(elem, decimalType))
        {
            char *decimal = NULL;
            parameter[i]->data = 
                strdup(PyString_AsString(PyObject_Str(elem)));
            parameter[i]->type = SQL_DECIMAL;
            parameter[i]->cType = SQL_C_CHAR;
            parameter[i]->precision =
                (int)strlen((char *)parameter[i]->data);
            decimal = strstr(parameter[i]->data,".");
            if (!decimal)
                decimal = strstr(parameter[i]->data, ",");
            if (decimal)
                decimal++;
            if (decimal)
                parameter[i]->scale = (int)strlen(decimal);
            else
                 parameter[i]->scale = 0;
            parameter[i]->orInd = SQL_NTS;
            parameter[i]->isNull = 0;
        } 
        else 
        {
            Py_XDECREF(params);
            exception = IIDBI_InternalError;
            errMsg = "unknown python parameter type";
            result = IIDBI_handleError((PyObject *)self, exception, errMsg);
            goto errorExit;
        }
        Py_DECREF(elem);
    } /* for (i = 0; i < parmCount; i++) */

    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_sendParameters }}}1\n", self);
    return DBI_SQL_SUCCESS;

errorExit:
    Py_XDECREF(elem);
    DBPRINTF(DBI_TRC_RET)("%p: IIDBI_sendParameters }}}1\n", self);
    if (result)
        return DBI_SQL_SUCCESS_WITH_INFO;
    else
        return DBI_SQL_ERROR;
}
