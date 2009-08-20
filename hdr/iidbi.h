/*
** vim:filetype=c:ts=4:sw=4:et:nowrap
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

/**
** Name:
**
** Description:
**
** History:
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add structures provided by loera01.
**      10-Jul-2004 (jeremy.peel@ca.com)
**          Build changes for Linux
**          Define II_APIENTRY as empty for Linux
**      10-Jul-2004 (raymond.fan@ca.com)
**          Modified iiDBIenv reference.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Change IIDBIenv to pointer to structure.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Add native error to the error structure.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Add defines for errors returned from the iidbi library.
**      15-Jul-2004 (raymond.fan@ca.com)
**          Add 64-bit integer type.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Add orInd field to descriptor.  This field is used to determine
**          if a char or string field is null terminated or is fixed length.
**      21-Oct-2004 (loera01)
**          Added "fetchDone" field to the cursor statement handle, so that
**          unnecessary calls to SQLCancel() are prevented.
**      22-Oct-2004 (Ralph.Loen@ca.com)
**          Add TRUE and FALSE defines, as Linux seems to lax these macro
**          definitions.
**      13-Dec-2004 (Ralph.Loen@ca.com)
**          Discard type objects ROWID, STRING, BINARY, NUMBER and DATETIME,
**          and replace them with module attributes.
**      21-Dec-2004 (Ralph.Loen@ca.com)
**          Added autocommit field to IIDBI_CONNECT type object structure.
**      14-Jan-2005 (Ralph.Loen@ca.com)
**          Added DBI_UNICODE_TYPE.
**      01-Feb-2005 (Ralph.Loen@ca.com)
**          Added arraysize and rowcount for cursors.
**      22-Feb-2005 (Ralph.Loen@ca.com)
**          Added IIdbi_cursorFetchMany().
**      26-Jul-2005 (loera01)
**          Changed selectloops attribute to a char pointer.  Added
**          servertype connection attribute.
**      22-Nov-2005 (loera01)
**          Added cursor methods IIDBI_cursorIterator() and 
**          IIDBI_cursorIterNext().
**      20-may-2006 (Ralph.Loen@ingres.com)
**          Added cursor.setinputsizes() and cursor.setoutputsize().
**      28-Apr-2008 (grant.croker@ingres.com)
**          Define MAX_PATH as 256 if not already defined
**/

# ifndef __IIDBI_H_INCLUDED
# define __IIDBI_H_INCLUDED

# define RETURN_TYPE int

# ifdef WIN32
# include <windows.h>
# endif

# include <Python.h>
# include <sql.h>
# include <sqlext.h>

# ifndef MAX_PATH
# define MAX_PATH 256
# endif /* MAX_PATH */

/*
** Name: IIDBI_ERROR - DBI error message structure.
**
** Description:
**    Stores error messages and associated SQLSTATE for error processing.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/
typedef struct
{
    char      sqlState[6];
    char      messageText[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER native;
}  IIDBI_ERROR;

# ifndef TRUE
# define TRUE 1
# endif

# ifndef FALSE
# define FALSE 0
# endif

#define IIDBI_MAX_ERROR_ROWS 10

#define DBI_SQL_SUCCESS             0
#define DBI_SQL_SUCCESS_WITH_INFO   1
#define DBI_INTERNAL_ERROR          2
#define DBI_SQL_ERROR               3
#define DBI_SQL_NO_DATA             4
#define DBI_SQL_UNKNOWN             5

/*
** DBI module definitions
*/
#define DBI_APILEVEL "2.0"
#define DBI_PARAMSTYLE "qmark"
#define DBI_THREADSAFETY 3

/*
** DBI Datatype codes
*/
#define DBI_UNKNOWN_TYPE   0
#define DBI_STRING_TYPE    1	
#define DBI_BINARY_TYPE    2
#define DBI_NUMBER_TYPE    3
#define DBI_DATETIME_TYPE  4
#define DBI_ROWID_TYPE     5
#define DBI_UNICODE_TYPE   6

/*
** Name: IIDBI_ERROR_HDR - DBI error header.
**
** Description:
**    A fixed table for storing error information.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/
typedef struct
{
    RETCODE rc;
    SQLINTEGER error_count;
    IIDBI_ERROR err[IIDBI_MAX_ERROR_ROWS];
}  IIDBI_ERROR_HDR;

/*
** Name: IIDBI_HDR - DBI header info.
**
** Description:
**   Used by all DBI handles to map common information.  
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/
typedef struct
{
    SQLHANDLE handle;
    IIDBI_ERROR err;
} IIDBI_HDR;


/*
** Name: IIDBI_ENV - DBI environment handle
**
** Description:
**    Defines fields used for DBI environment handles.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/

typedef struct 
{
    IIDBI_HDR hdr; 
} IIDBI_ENV;

/*
** Name: IIDBI_DBC - DBI connection handle
**
** Description:
**    Defines fields used for DBI connection handles.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/

typedef struct 
{
   IIDBI_HDR hdr;
   void *conn;
   SQLHANDLE *env;
} IIDBI_DBC, *pDBC;

/*
** Name: IIDBI_DESCRIPTOR - Generic parameters.
**
** Description:
**      This structure defines description of a data value.  The data vaue
**      being described can have one of the following usages:
**
**      * A tuple data returned by the DBMS server in response to a 'copy'
**        or 'select' commands.
**
**      * Reference of a procedure parameter when a procedure parameter is
**        used as a updatable input.
**
**      * Procedure parameter when a procedure parameter is used as a
**        read-only input.
**
**      * Service parameter when the parameter is required as an input
**        to an API service, not to a SQL command.
**
**      * Query parameter when the parameter has a parameter marker
**        in the query sent.
**
**      All parameters within this structure must be prefixed with ds_.
**
**      ds_dataType
**          data type of the data value.  The valid values are defined
**          in dbms.h.
**      ds_nullable
**          a flag stating whether the data type is nullable.
**      ds_length
**          length of the data value
**      ds_precision
**          precision of the data value if the data type is decimal or float.
**      ds_scale
**          scale of the data value if the data type is decimal.
**      ds_columnType
**          The usage of the data value.
**      ds_columnName
**          This parameter contains the symbolic name of the data value
**          if the column type is IIDPI_DS_PROCBYREFPARM, IIDPI_PROCPARM,
**          and IIDPI_DS_TUPLE.  The column name represents a procedure
**          parameter name when the column type is IIDPI_DS_PROCBYREFPARM
**          or IIDPI_DS_PROCPARM.  The column name represents the column
**          name in a copy file if the column type is IIDPI_TUPLE and
**          the query was 'copy from'.
**
**          Warning:
**          The data types defined here must be kept consistent with
**          the INGRES data types defined in iicommon.h.
** History:
**      12-jul-04 (loera01)
**          creation
**      16-Jul-2004 (raymond.fan@ca.com)
**          Make orInd field part of the descriptor as ODBC depends on its
**          persistence.
**          Changed the nullable field type to remove compiler warnings.
*/

typedef struct _IIDBI_DESCRIPTOR
{
    char            *columnName;
    int             type;  /* ODBC DBMS type */
    int             cType; /* The ODBC C type used for "transport" */
    int 	    precision;
    int 	    scale;
    void            *data;
    int             nullable;
    int             displaySize;
    int             internalSize; /* Used for internal memory malloc */
    unsigned char   isNull;
    long            orInd;
} IIDBI_DESCRIPTOR; 

/*
** Name: IIDBI_STMT - DBI statement handle
**
** Description:
**    Defines fields used for DBI statement handles.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/

typedef struct 
{
    IIDBI_HDR hdr; 
    unsigned char prepareRequested;
    unsigned char prepareCompleted;
    unsigned char hasResultSet;
    unsigned char fetchDone;
    int descCount;
    int rowCount;
    int arraySize;
    IIDBI_DESCRIPTOR **descriptor;
    int parmCount;
    IIDBI_DESCRIPTOR **parameter;
    unsigned int *inputSegmentSize;
    unsigned int inputSegmentLen;
    unsigned int outputSegmentSize;
    unsigned int outputColumnIndex;
} IIDBI_STMT, *pSTMT;

/*
** Name: IIDBI_DESC - DBI descriptor handle
**
** Description:
**    Defines fields used for DBI descriptor handles.
**
** History:
**   07-Jul-04 (loera01)
**      Created.
*/

typedef struct 
{
   IIDBI_HDR hdr; 
} IIDBI_DESC, *pDESC;

typedef struct
{
    PyObject_HEAD
    IIDBI_DBC *IIDBIpdbc;
    unsigned char closed;
    char *dsn;
    char *uid;
    char *pwd;
    char *database;
    char *vnode;
    int trace;
    unsigned char autocommit;
    unsigned char selectloops;
    char *servertype;
    char *driver;
    char *rolename;
    char *rolepwd;
    char *group;
    unsigned char  blankdate;
    unsigned char date1582;
    unsigned char catconnect;
    unsigned char numeric_overflow;
    unsigned char catschemanull;
    char *dbms_pwd;
    char *connectstr;
    PyObject *messages;
    PyObject *Error; 
    PyObject *Warning; 
    PyObject *InterfaceError;
    PyObject *DatabaseError;
    PyObject *DataError;
    PyObject *OperationalError;
    PyObject *IntegrityError;
    PyObject *InternalError;
    PyObject *ProgrammingError;
    PyObject *NotSupportedError;
    PyObject *errorhandler;
    PyObject *IOError;
    int pooled;
} IIDBI_CONNECTION;

typedef struct
{
    PyObject_HEAD
    PyObject *description;
    IIDBI_CONNECTION *connection;
    PyObject *prepared;
    unsigned char prepareRequested;
    char *szSqlStr;
    int rowcount;
    int arraysize;
    PyObject *rownumber;
    long rowindex;
    unsigned char closed;
    IIDBI_STMT *IIDBIpstmt;
    unsigned int inputSegmentLen;
    unsigned int *inputSegmentSize;
    unsigned int outputSegmentSize;
    unsigned int outputColumnIndex;
    PyObject *messages;
    PyObject *errorhandler;
} IIDBI_CURSOR;

#endif  /* __IIDBI_H_INCLUDED */
