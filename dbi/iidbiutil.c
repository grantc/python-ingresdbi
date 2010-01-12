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
# ifdef WIN32
# include <windows.h>
# endif

# include <sql.h>
# include <sqlext.h>
# include <iidbi.h>
# include <iidbiutil.h>

/*
** Global data
*/
int dbi_trclevel = DBI_TRC_OFF;
FILE *dbi_dbgfd = NULL;
int dbi_tracerefs = 0;
char* dbi_trcfile = NULL;

/**
** Name: iidbiutil.c - Ingres Python DB API utility functions
**
** Description:
**     This file defines:
**(E
**          dbi_trace() Enable tracing.
** 	    dbi_new_pdbc() Construct an object to hold a long string value.
** 		
**)E
**
** History:
**      10-Jul-2004 (raymond.fan@ca.com)
**         Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add IIDBItrace function and global trace flags.
**      11-Jul-2004 (raymond.fan@ca.com)
**          Add internal trace function and add trace stamp in output file.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Add function to return errors from ODBC.
**      14-Jul-2004 (raymond.fan@ca.com)
**          Only close log when all sessions have stopped tracing.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Add close on each trace statement to give more messages in the
**          event of an exception.
**      22-Jul-2004 (raymond.fan@ca.com)
**          Multiple traces running to standard error causes a problem.
**          When tracing to standard error don't close the descriptor.
**          Print fold marker for very first instance of trace start.
**      30-Jul-2008 (clach04)
**          Trac Ticket #160 - "No such file or directory" on disable trace
**          dbi_trace() was returning an uninitialized variable causing
**          crashes when Tracing was set to off "(0, None)".
**      19-Aug-2009 (Chris.Clark@ingres.com)
**          Fixed issue with tracing to file, every other trace message
**          was lost due to if-then block of code ALWAYS being ran due
**          to short cut on IF check (trailing semi-colon).
**      01-Sep-2009 (Chris.Clark@ingres.com)
**          Attempting to add comments/details on how tracing works.
**/

/*{
** Name: iidbitrace() - Set tracing in the driver.
**
** Description:
**      Sets the tracing level for debug tracing in the driver and optionally
**      specifies the target for debug messages.
**
** Inputs:
**      int dbglevel    (bitmask) enables the level of trace messages
**                      0 - disabled.
**                      1 - entry/exit  (DBI_TRC_ENTRY)
**                      2 - ??????????  (DBI_TRC_RET) 1 and 2 appear to be mixed up.
**                      4 - status      (DBI_TRC_STAT)
**      char*   trcfile path and filename for trace messages
**
** Outputs:
**     None.
**
** Returns:
** 	   None.
**
** Exceptions:
**     None.
**
** Side Effects:
**
** History:
**      10-Jul-2004 (raymond.fan@ca.com)
**          Created.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Save the filename of the file that is opened.
**          Keep a reference of open requests.
**      30-Jul-2008 (clach04)
**          Ensure ret_val is initialized before return.
}*/
short int
dbi_trace( int dbglevel, char* trcfile )
{
    FILE* fd;
    time_t ltime;
    short int ret_val=TRUE;

    time( &ltime );

    if (dbi_trclevel <= dbglevel)
    {
        dbi_trclevel = dbglevel;
    }
    if (dbglevel != DBI_TRC_OFF)
    {
        if ((dbi_dbgfd == NULL) && (trcfile) && (*trcfile))
        {
            dbi_trcfile = strdup( trcfile );
            if((fd = fopen( trcfile, "a" )) != NULL)
            {
                dbi_dbgfd = fd;
                dbi_tracerefs += 1;
            }
            else
            {
                ret_val = FALSE;
                goto dbi_trace_end;
            }

        }
        else
        {
            if (dbi_dbgfd == NULL)
            {
                dbi_dbgfd = stderr;         
            }
            dbi_tracerefs += 1;
        }
        if (!dbi_format( "Ingres DBI trace - started %s%s\n", 
            ctime( &ltime ), (dbi_tracerefs == 1) ? "{{{" : ""))
        {
            ret_val = FALSE;
            goto dbi_trace_end;
        }
    }
    else
    {
        if (dbi_dbgfd != NULL)
        {
            if ( dbi_tracerefs > 0)
            {
                dbi_tracerefs -= 1;
            }
            if (dbi_tracerefs <= 0)
            {
                free( dbi_trcfile );
                dbi_trcfile = NULL;
                dbi_format( "Ingres DBI trace - stopped %s%s\n",
                    ctime( &ltime ), (dbi_tracerefs == 0) ? "}}}" : "" );
                fclose( dbi_dbgfd );
                dbi_dbgfd = NULL;
                dbi_tracerefs = 0;
            }
        }
    }
dbi_trace_end:
    return ret_val;
}

/*{
** Name: iidbiutil() - Construct an object to hold a database control block
**
** Description:
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
**      08-Jul-2004 (raymond.fan@ca.com)
**          Created.
}*/
IIDBI_DBC*
dbi_newdbc( void )
{
    return ((IIDBI_DBC *) calloc(1,sizeof(IIDBI_DBC)));
}

/*{
** Name: dbi_error() - Populates structure with SQL errors from ODBC.
**
** Description:
**      Populates structure with SQL errors from ODBC.
**
** Inputs:
**      RETCODE     rc      Error code
**      HENV        henv    ODBC handle
**      HDBC        hdbc    Connection handle
**      SQLHSTMT    hstmt   Statement handle
**
** Outputs:
**     IIDBI_ERROR*     err     structure initialized with error codes.
**
** Returns:
** 	   None.
**
** Exceptions:
**     None.
**
** Side Effects:
**
** History:
**      12-Jul-2004 (raymond.fan@ca.com)
**          Created.
}*/
RETCODE
dbi_error_withtext( RETCODE status, HENV henv, HDBC hdbc, SQLHSTMT hstmt, IIDBI_ERROR* err, char *err_str )
{
    SQLCHAR*    SqlState;
    SQLCHAR*    Msg;
    SQLINTEGER* NativeError;
    SQLSMALLINT i=1;
    SQLSMALLINT MsgLen;
    SQLSMALLINT txtLen;
    int         rc;

    switch (status)
    {
        case SQL_SUCCESS :
                rc = DBI_SQL_SUCCESS;
                break;
        case SQL_SUCCESS_WITH_INFO :
                rc = DBI_SQL_SUCCESS_WITH_INFO;
                break;
        case SQL_INVALID_HANDLE :
                rc = DBI_INTERNAL_ERROR;
                break;
        default:
                rc = DBI_SQL_ERROR;
                break;
    }
    if (err != NULL)
    {
        SqlState    = (SQLCHAR *)err->sqlState;
        Msg         = (SQLCHAR *)err->messageText;
        NativeError = &err->native;
        txtLen      = sizeof( err->messageText );
        if (henv != NULL)
        {
            while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_ENV, henv, i,
                SqlState, NativeError, Msg, txtLen, &MsgLen)))
            {
                /*
                ** Will only return the first message in a list of error
                ** records for now.
                ** In the furture for more detailed errors the raised exception
                ** can instantiate an error class with a next method with
                ** which the host language can iterate.
                */
                break;
            }
        }
        else if (hdbc != NULL)
        {
            while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, i,
                SqlState, NativeError, Msg, txtLen, &MsgLen)))
            {
                /*
                ** Will only return the first message in a list of error
                ** records for now.
                ** In the furture for more detailed errors the raised exception
                ** can instantiate an error class with a next method with
                ** which the host language can iterate.
                */
                break;
            }
        }
        else if (hstmt != NULL)
        {
            while (SQL_SUCCEEDED(SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, i,
                SqlState, NativeError, Msg, txtLen, &MsgLen)))
            {
                /*
                ** Will only return the first message in a list of error
                ** records for now.
                ** In the furture for more detailed errors the raised exception
                ** can instantiate an error class with a next method with
                ** which the host language can iterate.
                */
                break;
            }
        }
        else if ( err_str != NULL)
        {
            strncpy(err->messageText, err_str, txtLen-1);
        }
	else
	{
	    strcpy(err->messageText, "Internal error not caught.");
	}
    }
    return rc;
}

/*{
** Name: dbi_format() - Format and output dbi trace messages.
**
** Description:
**      Function formats and outputs dbi trace messages.
**
** Inputs:
**      char*   fmt     Format string.
**              ...     Variable list of parameters.
** Outputs:
**     None.
**
** Returns:
** 	   None.
**
** Exceptions:
**     None.
**
** Side Effects:
**
** History:
**      11-Jul-2004 (raymond.fan@ca.com)
**          Created.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Add close on each trace message.
}*/
short int
dbi_format( char* fmt, ... )
{
    va_list p;
    short int ret_val;

    va_start( p, fmt );
    ret_val = TRUE;

    if ((dbi_dbgfd != NULL) || (dbi_trcfile != NULL))
    {        
        if (dbi_dbgfd == NULL)
        {
            if ((dbi_dbgfd = fopen( dbi_trcfile, "a" )) == NULL)
            {
                ret_val = FALSE;
                goto dbi_format_end;
            }
        }
        vfprintf( dbi_dbgfd, fmt, p );
        fflush( dbi_dbgfd );
        if ((dbi_dbgfd != NULL) && (dbi_dbgfd != stderr))
        {
            fclose( dbi_dbgfd );
            dbi_dbgfd = NULL;
        }

    }
dbi_format_end:
    return ret_val;
}
