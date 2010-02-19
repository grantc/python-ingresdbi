/*
** vim:filetype=c:ts=4:sw=4:et:nowrap
**
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

# ifdef WIN32
# include <windows.h>
# endif
# include <sql.h>
# include <sqlext.h>
# include <iidbi.h>
# include <iidbiconn.h>
# include <iidbiutil.h>

# ifndef TRUE
# define TRUE 0
# endif

# ifndef FALSE
# define FALSE 0
# endif

/**
** Name: iidbiconn.c - Ingres Python DB API connection functions
**
** Description:
**     This file defines:
**(E
**   dbi_connect() Create a connection to the specified database.
**   dbi_connectionClose() Close the connection & issue a rollback.
**   dbi_connectionCommit() Commit the current transaction.
**   dbi_connectionCursor() Declare a cursor. Return the handle.
**   dbi_connectionRollback() Rollback the transaction.
** 		
**)E
**
** History:
**     08-jul-04 (peeje01)
**         Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add ODBC body to stubs.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Moved print_err to iidbiutil.c.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Pointer (self) from python was not declared to be pointer.
**          Corrected indirection error and replaced use of global IIDBIenv
**          with (self).
**      11-Jul-2004 (raymond.fan@ca.com)
**          Correct multiple connection handling.
**      11-Jul-2004 (raymond.fan@ca.com)
**          Add iidbutil.h for internal tracing.          
**      12-Jul-2004 (raymond.fan@ca.com)
**          Add return and tracing of SQL errors.
**      13-Jul-2004 (chris.clark@ca.com)
**          Centralize the determination of returned error status.
**      14-Jul-2004 (raymond.fan@ca.com)
**          Add pdbc references to the trace prints to show owners.
**      21-Oct-2004 (grant.croker@ca.com)
**          Corrected driver to call with an r3 installation
**      04-Nov-2004 (ralph.loen@ca.com)
**          In dbi_connectionClose(), free the connection handle
**          after disconnection.
**      21-Dec-2004 (ralph.loen@ca.com)
**          Added connection attribute "autocommit".        
**      15-dec-2005 (loera01)
**          Changed dbi_connect() so that only one argument is passed.
**          Add selectloops connection attribute.
**      26-jul-2005 (loera01)
**          Add servertype connection attribute.
**      18-Mar-2009 (clach04)
**          Changed default ODBC driver name to use from
**          "INGRES 3.0" to "INGRES". This means the default Ingres
**          ODBC driver really will be used.
**/

/*{
** Name: dbi_alloc_env
**
** Description:
**
**
** Inputs:
**     penv - DBI environment handle.
**     pdbc - DBI connectio handle.
**
** Outputs:
**     New ODBC environment handle.
**
** Returns:
**     SQL_SUCCESS
**     SQL_INVALID_HANDLE
**     SQL_ERROR
** 
** Side Effects:
**     Updates global DBI environment handle.
**
** History:
**      05-June-2006 (loera01)
**          Created.
}*/

RETCODE
dbi_alloc_env( IIDBI_ENV *penv, IIDBI_DBC *pdbc)
{
    HENV henv;
    RETCODE rc, return_code = DBI_SQL_SUCCESS;
    IIDBI_CONNECTION *conn = pdbc->conn;

    DBPRINTF(DBI_TRC_ENTRY)( "%p: dbi_alloc_env }}}1\n", pdbc );

    for (;;)
    {
        if (conn->pooled)
        {
            rc = SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING,
                (SQLPOINTER)SQL_CP_ONE_PER_HENV,  sizeof(SQLINTEGER));
            if (rc != SQL_SUCCESS)
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, NULL, &pdbc->hdr.err );
                DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLSetEnvAttr (%d) %s %s %x\n", 
                    pdbc, rc, __LINE__, pdbc->hdr.err.sqlState, 
                    pdbc->hdr.err.messageText,
                    pdbc->hdr.err.native );
                break;
            }
        }
        rc = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv);
        if (rc != SQL_SUCCESS)
        {
            return_code = IIDBI_ERROR( rc, henv, NULL, NULL, &pdbc->hdr.err );
                DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLAllocHandle SQL_HANDLE_ENV (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
                break;
        }

        penv->hdr.handle = henv;

        rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
            (void*)SQL_OV_ODBC3, 0);
        if (rc != SQL_SUCCESS)
        {
            return_code = IIDBI_ERROR( rc, henv, NULL, NULL, &pdbc->hdr.err );
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLSetEnvAttr (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
            break;
        }
        break;
    }
    DBPRINTF(DBI_TRC_ENTRY)( "%p: dbi_alloc_env {{{1\n", pdbc );
    return return_code;
}

/*{
** Name: dbi_connect	-Create a connection to the specified database.
**
** Description:
**		Create a connection to the specified database.
**              The parameter dsn is the only required parameter, all others
**              are optional. The parameter list is DBMS dependent/defined(?)
**
** Assumptions:
**     The following global variables must be set:
**(E
**     Name           Meaning (possible values)
**     apilevel      -DB API Level ('1.0' or '2.0')
**     thread safety -Thread safety (0, 1, 2, 3)
**     paramstyle    -Parameter style ('qmark', 'numeric', 'format', 'pyformat')
**)E
**
** Inputs:
**     IIDBI_DBC pdbc Control block
**     char *dsn      Data Source Name
**     char *user     User name
**     char *password Password
**     char *host     Hostname
**     char *database Database
**
** Outputs:
**     Returns:
**         Database Connection
**     Exceptions:
**        Warning           E.G. for data truncations
**        InterfaceError    Database interface errors
**        DatabaseError     Database errors
**        OperationalError  Errors in database operation
**        IntegrityError    Relational Integrity failure
**        InternalError     Internal DB error
**        ProgrammingError  Nonexistant Table, syntax, ...
**        NotSupportedError Unsupported API method used
**        Error             All other errors
** 	
** Side Effects:
**
** History:
**      08-Jul-2004 (peeje01)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add ODBC connect code from clach04, loera01 
**      11-Jul-2004 (raymond.fan@ca.com)
**          Assign IIDBIenv with initial value on initialization.
**          Initialize henv from IIDBIenv.
}*/
RETCODE
dbi_connect( IIDBI_DBC *pdbc)
{
    RETCODE     rc = SQL_SUCCESS;
    HDBC        hdbc=NULL;
    HENV        henv=pdbc->env;
    IIDBI_CONNECTION *conn = pdbc->conn;
    char        szConnStrIn[300]= "\0";
    char        szConnStrOut[300] = "\0";
    SQLSMALLINT cbConnStrOut;
    char        *serverStr="SERVER=%s;";
    char        *dsnStr="DSN=%s;";
    char        *uidStr="UID=%s;";
    char        *pwdStr="PWD=%s;";
    char        *dbStr="DATABASE=%s;";
    char        *sloopStr="SELECTLOOPS=%s;";
    char        *stypeStr="SERVERTYPE=%s;";
    char        *driverStr="DRIVER=%s;";
    char        *rolenameStr="ROLENAME=%s;";
    char        *rolepwdStr="ROLEPWD=%s;";
    char        *groupStr="GROUP=%s;";
    char        *blankdateStr="BLANKDATE=%s;";
    char        *date1582Str="DATE1582=%s;";
    char        *catconnectStr="CATCONNECT=%s;";
    char        *numeric_overflowStr="NUMERIC_OVERFLOW=%s;";
    char        *catschemanullStr="CATSCHEMANULL=%s;";
    char        *dbms_pwdStr="DBMS_PWD=%s;";
    char        connStr[300];
    int         return_code = DBI_SQL_ERROR;

    DBPRINTF(DBI_TRC_ENTRY)( "%p: dbi_connect {{{1\n", pdbc );
    for(;;)
    {
        conn = (IIDBI_CONNECTION *)pdbc->conn;
        rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
        if (rc == SQL_INVALID_HANDLE)
        {
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLAllocHandle SQL_HANDLE_DBC (%d)\n", pdbc,
                rc, __LINE__ );
            return_code = DBI_INTERNAL_ERROR;
            (void) dbi_error_withtext( rc, NULL, NULL, NULL, &pdbc->hdr.err, "SQLAllocHandle - Invalid handle." ); 
            break;
        }
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR(rc, NULL, hdbc, NULL, &pdbc->hdr.err );
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLDriverConnect (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
            break;
        }
        
        if (conn->connectstr && *conn->connectstr)
        {
            strcpy(szConnStrIn, conn->connectstr);
        }
        else
        {
            if (conn->dsn && *conn->dsn)
            {
                sprintf(szConnStrIn, dsnStr, conn->dsn);
            }
            else if (!conn->driver)
            {
                sprintf(szConnStrIn, "%s", "DRIVER=INGRES;");
            }
            else if (conn->driver && *conn->driver)
            {
                sprintf(connStr, driverStr, conn->driver);
                strcat(szConnStrIn, connStr);
            }
            if (conn->vnode && *conn->vnode)
            {
                sprintf(connStr, serverStr, conn->vnode);
                strcat(szConnStrIn, connStr);
            }
            if (conn->database && *conn->database)
            {
                sprintf(connStr, dbStr, conn->database);
                strcat(szConnStrIn, connStr);
            }
            if (conn->uid && *conn->uid)
            {
                sprintf(connStr, uidStr, conn->uid);
                strcat(szConnStrIn, connStr);
            }
            if (conn->pwd && *conn->pwd)
            {
                sprintf(connStr, pwdStr, conn->pwd);
                strcat(szConnStrIn, connStr);
            }
            if (conn->selectloops)
            {
                sprintf(connStr, sloopStr, "Y");
                strcat(szConnStrIn, connStr);
            }
            else
            {
                sprintf(connStr, sloopStr, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->blankdate)
            {
                sprintf(connStr, blankdateStr, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->date1582)
            {
                sprintf(connStr, date1582Str, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->catconnect)
            {
                sprintf(connStr, catconnectStr, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->numeric_overflow)
            {
                sprintf(connStr, numeric_overflowStr, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->catschemanull)
            {
                sprintf(connStr, catschemanullStr, "N");
                strcat(szConnStrIn, connStr);
            }
            if (conn->servertype && *conn->servertype)
            {
                sprintf(connStr, stypeStr, conn->servertype);
                strcat(szConnStrIn, connStr);
            }
            if (conn->rolename && *conn->rolename)
            {
                sprintf(connStr, rolenameStr, conn->rolename);
                strcat(szConnStrIn, connStr);
            }
            if (conn->rolepwd && *conn->rolepwd)
            {
                sprintf(connStr, rolepwdStr, conn->rolepwd);
                strcat(szConnStrIn, connStr);
            }
            if (conn->group && *conn->group)
            {
                sprintf(connStr, groupStr, conn->group);
                strcat(szConnStrIn, connStr);
            }
            if (conn->dbms_pwd && *conn->dbms_pwd)
            {
                sprintf(connStr, dbms_pwdStr, conn->dbms_pwd);
                strcat(szConnStrIn, connStr);
            }
        }
        DBPRINTF(DBI_TRC_STAT)( "%p: Connecting to %s (%d)\n", pdbc, szConnStrIn, __LINE__ );
        rc = SQLDriverConnect(hdbc, 0,
            (SQLCHAR *)szConnStrIn, SQL_NTS, (SQLCHAR *)szConnStrOut, 
            sizeof(szConnStrOut), &cbConnStrOut, 
            SQL_DRIVER_NOPROMPT);
        if (rc == SQL_INVALID_HANDLE)
        {
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLDriverConnect (%d)\n", pdbc, rc, __LINE__ );
            return_code = DBI_INTERNAL_ERROR;
            (void) dbi_error_withtext( rc, NULL, NULL, NULL, &pdbc->hdr.err, "SQLDriverConnect - Invalid handle." ); 
            break;
        }
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, hdbc, NULL, &pdbc->hdr.err );
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLDriverConnect (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
            break;
        }
        DBPRINTF(DBI_TRC_STAT)( "%p: Connected to %s (%d)\n", pdbc, szConnStrOut, __LINE__ );
        pdbc->hdr.handle = hdbc;
        if (conn->autocommit)
        {
             DBPRINTF(DBI_TRC_STAT)
                 ( "%p: Connected. Setting autocommit to ON (%d)\n", pdbc,
                 __LINE__ );
            rc = SQLSetConnectAttr(hdbc,SQL_ATTR_AUTOCOMMIT,
                (SQLPOINTER)SQL_AUTOCOMMIT_ON, sizeof(SQLINTEGER));
        }
        else
        {
            DBPRINTF(DBI_TRC_STAT)
                ( "%p: Connected. Setting autocommit to OFF (%d)\n", pdbc,
                __LINE__ );
            rc = SQLSetConnectAttr(hdbc,SQL_ATTR_AUTOCOMMIT,
                (SQLPOINTER)SQL_AUTOCOMMIT_OFF, sizeof(SQLINTEGER));
        }
        if (rc == SQL_INVALID_HANDLE)
        {
            DBPRINTF(DBI_TRC_STAT)
                ( "%p: %d = SQLSetConnectAttr SQL_ATTR_AUTOCOMMIT (%d)\n", pdbc,
                rc, __LINE__ );
            return_code = DBI_INTERNAL_ERROR;
            (void) dbi_error_withtext( rc, NULL, NULL, NULL, 
                &pdbc->hdr.err, "SQLSetConnectAttr - Invalid handle." ); 
            break;
        }
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, hdbc, NULL, &pdbc->hdr.err );
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLConnectAttr SQL_AUTOCOMMIT_OFF (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
            break;
        }
        return_code = DBI_SQL_SUCCESS;
        break;
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connect }}}1\n", pdbc);
    return return_code;
}

/*{
** Name: dbi_connectionClose - Issue a rollback and close the connection.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**
** Side Effects:
**
** History:
**      08-Jul-2004 (peeje01)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add ODBC connect code from clach04, loera01 
}*/
RETCODE
dbi_connectionClose( IIDBI_DBC *pdbc )
{
    RETCODE rc;
    HDBC hdbc = pdbc->hdr.handle;
    int         return_code = DBI_SQL_SUCCESS;

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionClose {{{1\n", pdbc);
    
    if (hdbc)
    {
        rc = SQLDisconnect(hdbc);
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, hdbc, NULL, &pdbc->hdr.err );
            DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLDisconnect (%d) %s %s %x\n", pdbc,
                rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
                pdbc->hdr.err.native );
        }
        else
        {
            DBPRINTF(DBI_TRC_STAT)( "%p: Disconnected successfully (%d)\n", pdbc, __LINE__ );
        }
    }

    rc = SQLFreeConnect( hdbc );
    if (rc == SQL_INVALID_HANDLE)
    {
        DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC connection handle (%d)\n", 
            pdbc, __LINE__ );
        if (return_code == DBI_SQL_SUCCESS)
            return_code = DBI_INTERNAL_ERROR;
    } 
    else if (rc == SQL_ERROR)
    {
        if (return_code == DBI_SQL_SUCCESS)
            return_code = IIDBI_ERROR( rc, hdbc, NULL, 
                NULL, &pdbc->hdr.err );
    
        DBPRINTF(DBI_TRC_STAT)( "%p: %d = dbi_connectionClose (%d) %s %s %x\n",
            pdbc, rc, __LINE__, pdbc->hdr.err.sqlState, 
            pdbc->hdr.err.messageText, pdbc->hdr.err.native );
    }

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionClose }}}1\n", pdbc);
    return return_code;
}

/*{
** Name: dbi_connectionCommit -Issue a commit
**
** Description:
**     Commit pending transaction.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**         Returns:
** 	       void
** Exceptions:
**     None.
**
** Side Effects:
**     None.
**
** History:
**     08-Jul-2004 (peeje01)
**         Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add ODBC connect code from clach04, loera01 
}*/
RETCODE
dbi_connectionCommit( IIDBI_DBC *pdbc )
{
    RETCODE rc;
    HDBC hdbc = pdbc->hdr.handle;
    int         return_code = DBI_SQL_ERROR;

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionCommit {{{1\n", pdbc);
    rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
    if (rc != SQL_SUCCESS) 
    {
        return_code = IIDBI_ERROR( rc, NULL, hdbc, NULL, &pdbc->hdr.err );
        DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLEndTran SQL_COMMIT (%d) %s %s %x\n", pdbc,
            rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
            pdbc->hdr.err.native );
    }
    else
    {
        DBPRINTF(DBI_TRC_STAT)( "%p: Committed successfully (%d)\n", pdbc, __LINE__ );
        return_code = DBI_SQL_SUCCESS;
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionCommit }}}1\n", pdbc);
    return return_code;
}

/*{
** Name: dbi_connectionCursor - Return a cursor object
**
** Description:
**     Return a new cursor object using the connection.
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**     Returns:
** 	   The new cursor object.
**     Exceptions:
**         None.
**
** Side Effects:
**
** History:
**     08-Jul-2004 (peeje01)
**         Created.
}*/
RETCODE
dbi_connectionCursor( IIDBI_DBC *pdbc )
{
    int         return_code = DBI_SQL_SUCCESS;
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionCursor {{{1\n", pdbc);
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionCursor }}}1\n", pdbc);
    return(return_code);
}

/*{
** Name: dbi_connectionRollback - Issue a rollabck
**
** Description:
**     Issue a rollback on the connection
**
** Inputs:
**     None.
**
** Outputs:
**     None.
**     Returns:
** 	   None.
**     Exceptions:
**         None.
**
** Side Effects:
**
** History:
**      08-Jul-2004 (peeje01)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Corrected iiDBIenv to resolve.
}*/
RETCODE
dbi_connectionRollback( IIDBI_DBC *pdbc )
{
    RETCODE rc;
    HDBC hdbc = pdbc->hdr.handle;
    int         return_code = DBI_SQL_ERROR;
    
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionRollback {{{1\n", pdbc);

    rc = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
    if (rc != SQL_SUCCESS) 
    {
        return_code = IIDBI_ERROR( rc, NULL, hdbc, NULL, &pdbc->hdr.err );
        DBPRINTF(DBI_TRC_STAT)( "%p: %d = SQLEndTran SQL_ROLLBACK (%d) %s %s %x\n", pdbc,
            rc, __LINE__, pdbc->hdr.err.sqlState, pdbc->hdr.err.messageText,
            pdbc->hdr.err.native );
    }
    else
    {
        DBPRINTF(DBI_TRC_STAT)( "%p: Rolled back successfully (%d)\n", pdbc, __LINE__ );
        return_code = DBI_SQL_SUCCESS;
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_connectionRollback }}}1\n", pdbc);
    return return_code;
}
