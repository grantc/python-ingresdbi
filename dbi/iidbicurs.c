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
# include <iidbicurs.h>
# include <iidbiutil.h>

#define MAX_DISPLAY_SIZE 0x7fffffff
/**
** Name: iidbicurs.c - Ingres Python DB API cursor functions
**
** Description:
**     This file defines:
**(E
** 	 dbi_cursorClose() Close the cursor.
** 	 dbi_cursorExecute() Prepare and execute a statement with vargs.
** 	 dbi_cursorFetchone() Fetch one row from a cursor.
** 		
**)E
**
** History:
**      08-jul-04 (peeje01)
**          Created.
**      11-Jul-2004 (raymond.fan@ca.com)
**          Add trace messages.
**      13-Jul-2004 (raymond.fan@ca.com)
**          Allow reuse same python cursor for SQL statements.
**      14-Jul-2004 (raymond.fan@ca.com)
**          Add pstmt references to the trace prints to show owners.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Fix up heap and memory corruptions.
**      17-Jul-2004 (komve01@ca.com)
**          Fixed the float and real data type interpretation.
**      17-Jul-2004 (komve01@ca.com)
**          Fixed dbi_freeDescriptor. Clean parameter if isParam is one.
**          else clean descriptor block.
**      17-Jul-2004 (raymond.fan@ca.com)
**          First attempt at select of binary data, as long as the blob fits 
**          in the initial segment.
**      17-Jul-2004 (komve01@ca.com)
**          Added support for float,real and double type parameters in
**          BindParameters.
**      19-Jul-2004 (komve01@ca.com)
**          Added support for long BindParameters.
**      19-Jul-2004 (raymond.fan@ca.com)
**          Add bind parameter handling of binary types and handing of
**          passing data as part of the execute.
**      19-Jul-2004 (raymond.fan@ca.com)
**          Keep track of amount of data retrieved from the database for
**          binary types.
**      19-Jul-2004 (raymond.fan@.ca.com)
**          Correct function sequence error caused by early termination of
**          an SQLPutParam loop.
**      20-Jul-2004 (chris.clark@.ca.com)
**          Return errors if Execute fail (i.e. errors from DBMS for any 
**          reason), e.g. Syntax errors, duplicate key on insert, etc.
**      21-Jul-2004 (raymond.fan@ca.com)
**          Use memcpy for binary copies.
**      21-Jul-2004 (raymond.fan@ca.com)
**          Modifed select of binary types to enable blobs greater then the
**          specfied segment size.
**      22-Jul-2004 (raymond.fan@ca.com)
**          Modified put data loop to remove memcpy.
**      22-Jul-2004 (komve01@ca.com)
**          Fixed BIGINT conversion.
**      23-Jul-2004 (komve01@ca.com)
**          Commented a debug printf statement.
**     17-Sep-2004 (Ralph.Loen@ca.com)
**         In dbi_fetchone(), added support for BIGINT.
**     21-Oct-2004 (Ralph.Loen@ca.com)
**         Cleaned up treatment of SQLBindParameter() for ints and bigints.
**         Set new "fetchDone" flag in the cursor statement handle so that
**         unnecessary calls to SQLCancel() are prevented.
**     22-Oct-2004 (Ralph.Loen@ca.com)
**         Fixed up support for FLOAT4 and FLOAT8.
**     08-Nov-2004 (grant.croker@ca.com)
**         Fixed data truncation in SQL_CHAR and SQL_VARCHAR types.
**     13-Dec-2004 (Ralph.Loen@ca.com)
**         Bind "None" parameters as typeless nulls.  
**     07-Jan-2004 (Ralph.Loen@ca.com)
**         Add dbi_freeData().
**     14-Jan-2005 (Ralph.Loen@ca.com)
**        Initial support for Unicode data types.
**     31-Jan-2005 (Ralph.Loen@ca.com)
**          In dbi_cursorFetchone(), allow for terminating character in 
**          segments when fetching long varchars.
**     01-Feb-2005 (Ralph.Loen@ca.com)
**        Added cursor rowcount attribute.
**     07-Feb-2004 (Ralph.Loen@ca.com)
**        Increased blob segment size to 100,000 and changed some calloc's to
**        malloc's to improve performance.
**     19-may-2005 (Ralph.Loen@ca.com)
**        Added support for row-returning procedures in Cursor.callproc().
**        Return input parameters.  Output and BYREF parameters are still
**        not supported because the DBI specs aren't flexible enough to
**        work with Ingres limitations.:
**     22-nov-2005 (loera01)
**        Initialize rowcount to -1.
**     26-jan-2006 (loera01)
**        Set the cursor description's "display size" attribute to -1 if
**        specified as 2G (applies to blobs).
**     27-Feb-2006 (Ralph.Loen@ingres.com)
**        Added detail to dbi_allocDescriptor() and dbi_freeDescriptor()
**        DBI trace messages.
**   20-may-2006 (Ralph.Loen@ingres.com)
**        Added cursor.setinputsizes() and cursor.setoutputsize().
**      30-Jul-2008 (clach04)
**          Trac Ticket #243 - errors on bind ignored
**          dbi_cursorExecute() failed to check (and report) errors on calls
**          to BindParameters(). Added simple check.
**      03-Dec-2008 (clach04)
**          Trac Ticket #159 - SELECT of Unicode results are truncated
**          Wrong (byte) length was being used.
**          Removed newline from trace output, easier to parse single line
**          trace files.
**      05-Aug-2009 (Chris.Clark@ingres.com)
**          Removed unused dbi_cursorFetchall()
**      19-Aug-2009 (Chris.Clark@ingres.com) 
**          Added vim hints. 
**          Fixed bug with select of decimal values. 
**          Leave "precision" alone, precision means something and should 
**          not be altered. Now set internalSize to the size of the buffer
**          space for decimal on fetch. Ensure the correct (max) string
**          length is used for fetching decimal values from the DBMS. Added
**          new macro MAX_STRING_LEN_FOR_DECIMAL_PRECISION for calculating
**          string buffer length.
**  15-Sep-2009 (clach04)
**      Trac ticket 417 None/NULL binds crash (or fail under
**      Microsoft ODBC manager).
**      Ingres CLI doesn't care about cType, but MS ODBC manager does, set
**      string type for NULL (type).
**      Crash was caused by access to uninitialized pointer.
**      Bindparam code for NULL/None removed, now uses generic bind code
**      as used for strings and decimal.
**  24-Nov-2009 (clach04)
**      Trac ticket 502 empty string returned for LONG NVARCHAR/NCLOB columns
**      LONG NVARCHAR length indicator orInd was not being set
**      resulting in empty strings being returned. Noticed same
**      issue with other LONG types (LONG BYTE and LONG VARCHAR),
**      lack of correct orInd was not an issue for these types
**      but is inconsistent hence the change.
**/

/* 
** Decimal values are sent/received as strings. 
** Define macro that calculates the largest string needed for 
** a decimal with a given precision. 
** 
** max string length/size of decimal value is based on: 
**      precision (not the precision/scale encoded combo) for digits 
**      +1 for NULL terminator, ODBC auto null terminates when using SQLGetData()
**      +1 for period/dot/comma 
**      +1 for possible negative sign. 
**      +1 for possible leading zero where precision=scale,
**          e.g Ingres literal Decimal(0.1, 1, 1)
*/ 
#define MAX_STRING_LEN_FOR_DECIMAL_PRECISION(x) (x+4)

RETCODE BindParameters(IIDBI_STMT *pstmt, unsigned char isProc);

/*{
** Name: dbi_cursorClose - Close the cursor
**
** Description:
**     Close the ODBC cursor (not the DBI cursor)
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
**     08-Jul-2004 (peeje01)
**         Created.
}*/

RETCODE
dbi_cursorClose( IIDBI_STMT* pstmt )
{
    RETCODE rc = SQL_SUCCESS, return_code = DBI_SQL_SUCCESS;
    HSTMT hstmt = NULL;
    
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorClose {{{1\n", pstmt);
    if (!pstmt)
    {
        DBPRINTF(DBI_TRC_STAT)( "%p: Invalid cursor statement handle (%d)\n",
             pstmt, __LINE__ );
        return_code = DBI_INTERNAL_ERROR; 
        goto exitCloseCursor;
    }
    pstmt->rowCount = -1;
    hstmt = pstmt->hdr.handle;
    if (hstmt)
    {
        if (pstmt->hasResultSet)
        {
            if (!pstmt->fetchDone)
            {
                rc = SQLCancel(hstmt);
                if (rc == SQL_INVALID_HANDLE)
                {
                    DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC statement handle (%d)\n", 
                        pstmt, __LINE__ );
                    return_code = DBI_INTERNAL_ERROR;
                    goto exitCloseCursor;
                }
                if (rc == SQL_ERROR)
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
    
                    DBPRINTF(DBI_TRC_STAT)( "%p: %d = dbi_cursorClose (%d) %s %s %x\n",
                    pstmt, rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText,
                        pstmt->hdr.err.native );
                    goto exitCloseCursor;
                }   
            }
            pstmt->hasResultSet = 0;
        }
exitCloseCursor:
        if (hstmt)
        {
            rc = SQLFreeStmt( hstmt, SQL_DROP );
            if (rc == SQL_INVALID_HANDLE)
            {
                DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC statement handle (%d)\n", 
                    pstmt, __LINE__ );
                if (return_code == DBI_SQL_SUCCESS)
                    return_code = DBI_INTERNAL_ERROR;
            }
            else if (rc == SQL_ERROR)
            {
                if (return_code == DBI_SQL_SUCCESS)
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
    
                DBPRINTF(DBI_TRC_STAT)( "%p: %d = dbi_cursorClose (%d) %s %s %x\n",
                    pstmt, rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText,
                    pstmt->hdr.err.native );
            }
        }
    }

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorClose }}}1\n", pstmt);
    return( return_code );
}

/*{
** Name: dbi_cursorExecute - Execute an (optionally prepared) statement
**
** Description:
**     Execute an SQL statement. Parameters may be provided as a
**     sequence or mapping and will be bound to variables in the operation.
**
**     The cursor retains a reference to the operation (object?). If the
**     same operation object is passed in again, the cursor can optimize
**     its behaviour.
**    
**     If so requested, prepare the statement prior to execution.
**
** Example:
**(E
**     my_cur.execute('SELECT :a FROM t1 WHERE :a = :b', a='name', b='fred');
**)E
** Inputs:
**     char *operation 	The statement to be executed
**     char *parameter 	Optional parameter(s) to be passed to the statement
**
** Outputs:
**     <param name> 	<output value description>
**     Returns:
** 	   <function return values>
**     Exceptions:
**         <exception codes>
**
** Side Effects:
**
** History:
**     08-Jul-2004 (peeje01)
**         Created.
**      11-Jul-2004 (raymond.fan@ca.com)
**          Replace initialization of henv with python object rather than
**          global environment.
**	    13-Jul-2004 (raymond.fan@ca.com)
**	        Initialize hstmt if we determine that there is already
**	        a statement handle allocated.
**      15-Jul-2004 (raymond.fan@ca.com)
**          Removed bigint type code to return value as bigint.
**          Instead use default code to return as string and have
**          a wrapper in the host language layer.
**          This overcomes mapping deficiencies in our SWIG implementation.
**      20-Jul-2004 (raymond.fan@ca.com)
**          Update put data loop to correct function execute sequence errors.
**      21-Jul-2004 (raymond.fan@ca.com)
**          Replace strncpy with memcpy.
**      22-Jul-2004 (raymond.fan@ca.com)
**          Restructured to remove memcpy on put data.
**      22-Jul-2004 (raymond.fan@ca.com)
**          For blobs where the last segment does not fall on a segment
**          boundary include an increment of the data pointer.
**      30-Jul-2008 (clach04)
**          dbi_cursorExecute() now checks for failed calls to BindParameters()
}*/

RETCODE
dbi_cursorExecute( IIDBI_DBC* pdbc, IIDBI_STMT* pstmt, char *stmnt,
    unsigned char isProc )
{
    HDBC hdbc = NULL;
    HSTMT hstmt = NULL;
    RETCODE rc = SQL_SUCCESS;
#define PUTSEGMENT_SIZE 100000
    SQLUINTEGER colNbr;
    SQLSMALLINT numCols = 0;
    SQLINTEGER numRows = -1;
    int return_code = DBI_SQL_SUCCESS;
    int lenCntr = 0;
    int idx=0;
    IIDBI_DESCRIPTOR **parameter = pstmt->parameter;
    char* data;
    int len=0;
    int colPrev = -1;
    int putSegmentSize = 0;

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorExecute {{{1\n", pstmt);

    if (pdbc)
        hdbc = pdbc->hdr.handle;
    else
    {
        DBPRINTF(DBI_TRC_STAT)( "%p: Invalid connection handle (%d)\n",
             pdbc, __LINE__ );
        return DBI_INTERNAL_ERROR;
    }

    for(;;)
    {
        hstmt = pstmt->hdr.handle ? pstmt->hdr.handle : NULL;
        pstmt->rowCount = -1;
        if (pstmt->hasResultSet)
        {
            if (!pstmt->fetchDone)
                rc = SQLCancel(hstmt);
            pstmt->hasResultSet = 0;
        }
        if (pstmt->descCount)
        {
            rc = dbi_freeDescriptor(pstmt, 0);
            pstmt->hasResultSet = 0;
        }
        if (!pstmt->prepareRequested)
        {
            if (hstmt)
            {
                rc = SQLFreeStmt(hstmt, SQL_DROP);
                if (rc == SQL_INVALID_HANDLE)
                {
                    DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC handle (%d)\n", 
                        pstmt, __LINE__ );
                    return_code = DBI_INTERNAL_ERROR;
                    break; 
                }
    
                if (rc != SQL_SUCCESS) 
                {
                    return_code = IIDBI_ERROR( rc, NULL, hdbc, hstmt, 
                        &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                        ( "%d = SQLAllocHandle SQL_HANDLE_STMT (%d) %s %s %x\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, pstmt->hdr.err.native );
                    break; 
                }
            }
            rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
            if (rc == SQL_INVALID_HANDLE)
            {
                DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC handle (%d)\n", pstmt, __LINE__ );
                return_code = DBI_INTERNAL_ERROR;
                break; 
            }
    
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, hdbc, hstmt, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLAllocHandle SQL_HANDLE_STMT (%d) %s %s %x\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native );
                break; 
            }
         
            pstmt->hdr.handle = hstmt; 
        } /* if (!pstmt->prepareRequested */
        else
        {
            if (!hstmt)
            {
                rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
                if (rc == SQL_INVALID_HANDLE)
                {
                    DBPRINTF(DBI_TRC_STAT)( "%p: Invalid ODBC handle (%d)\n", 
			pstmt, __LINE__ );
                    return_code = DBI_INTERNAL_ERROR;
                    break; 
                }
    
                if (rc != SQL_SUCCESS) 
                {
                    return_code = IIDBI_ERROR( rc, NULL, hdbc, hstmt, 
                        &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                        ( "%d = SQLAllocHandle SQL_HANDLE_STMT (%d) %s %s %x\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, pstmt->hdr.err.native );
                    break; 
                }
                pstmt->hdr.handle = hstmt; 
            }
            if (!pstmt->prepareCompleted) 
            {
                DBPRINTF(DBI_TRC_STAT)("Preparing the query %s\n",stmnt);
                rc = SQLPrepare(hstmt, (SQLCHAR *)stmnt, SQL_NTS);
                if (rc != SQL_SUCCESS) 
                {
		    return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, 
                        &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                        ( "%d = SQLPrepare SQL_HANDLE_STMT (%d) %s %s %x\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, pstmt->hdr.err.native );
                    pstmt->prepareRequested = FALSE;
                    break; 
                }
                pstmt->prepareCompleted = TRUE;
            }
        }
        if (pstmt->parmCount)
            return_code = BindParameters(pstmt, isProc);
        if (return_code != DBI_SQL_SUCCESS)
            break;

        DBPRINTF(DBI_TRC_STAT)("Executing the query %s\n",stmnt);

        if (pstmt->prepareCompleted)
            rc = SQLExecute(hstmt);
        else
            rc = SQLExecDirect(hstmt, (SQLCHAR *)stmnt, SQL_NTS);
        if( rc == SQL_NEED_DATA)
        {
            while(rc == SQL_NEED_DATA)
            {
                rc = SQLParamData( hstmt, (SQLPOINTER)&colNbr );
                if (rc == SQL_NEED_DATA)
                {
                    if (colPrev > 0 && colNbr != colPrev)
                        len = 0;
                    colPrev = colNbr;

                    /*
                    ** Bindparameter stored a '1' based index of parameter
                    ** passed.  Use this number to get the parameter
                    ** descriptor
                    */
                    idx = (int)colNbr - 1;
                    data = parameter[idx]->data;
                    lenCntr = parameter[idx]->precision;
                    /*
                    ** Use do/while loop.  A zero length binary would cause
                    ** a function execute error
                    */
                    if (pstmt->inputSegmentSize && pstmt->inputSegmentLen &&
                        colNbr <= pstmt->inputSegmentLen)
                        putSegmentSize = pstmt->inputSegmentSize[colNbr - 1];
                    else
                        putSegmentSize = PUTSEGMENT_SIZE;

                    do
                    {
                        int putrc;
                        if (lenCntr < putSegmentSize)
                        {
                            /*
                            ** Bump the data pointer for the last write.
                            */
                            data += len;
                            len = lenCntr;
                            lenCntr = 0;
                        }
                        else
                        {
                            if (len)
                                data += len;
                            if (lenCntr < putSegmentSize)
                                len = lenCntr;
                            else
                                len = putSegmentSize;
                            lenCntr -= len;
                        }
                        /*
                        ** Don't update rc from the SQLPutData as the 
                        ** loop relies on rc and early termination of the 
                        ** loop, without error, causes an execute sequence 
                        ** error.
                        */
                        if ((putrc = SQLPutData( hstmt, data, len)) == 
                            SQL_ERROR)
                        {
                            DBPRINTF(DBI_TRC_STAT)
                            ( "%d = IIDBIExecute (%d) %s %s %x\n    %s\n",
                                putrc, __LINE__, pstmt->hdr.err.sqlState,
                                pstmt->hdr.err.messageText, 
                                pstmt->hdr.err.native, stmnt );
                            /*
                            ** Error returned. Change return code to break loop
                            */
                            rc = putrc;
                            break;
                        }
                    }while (lenCntr);
                } /* if (rc == SQL_NEED_DATA) from SQLParamData */
            } /* while rc == SQL_NEED_DATA */
        }
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
            DBPRINTF(DBI_TRC_STAT)
                ( "%d = IIDBIExecute (%d) %s %s %x\n    %s\n",
                rc, __LINE__, pstmt->hdr.err.sqlState, 
                pstmt->hdr.err.messageText, pstmt->hdr.err.native, stmnt );
            break;
        }
        if ( !isProc && pstmt->parmCount)
        {
            rc = dbi_freeDescriptor(pstmt, 1);
        }
        rc = SQLNumResultCols(hstmt, &numCols);
        if (SQL_SUCCEEDED(rc) && numCols)
        {
            DBPRINTF(DBI_TRC_STAT)
                ("Number of cols is %d\n",numCols);
            pstmt->hasResultSet = 1;
            pstmt->descCount = numCols;
            break; 
        }
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
            DBPRINTF(DBI_TRC_STAT)
                ( "%d = IIDBIExecute (%d) %s %s %x\n    %s\n",
                rc, __LINE__, pstmt->hdr.err.sqlState, 
                pstmt->hdr.err.messageText, pstmt->hdr.err.native, stmnt );
            break;
        }
        rc = SQLRowCount(hstmt, &numRows);
        if (SQL_SUCCEEDED(rc) && numRows)
        {
            DBPRINTF(DBI_TRC_STAT)
                ("Number of rows is %d\n",numRows);
            pstmt->rowCount = numRows;
            break;
        }
        else
            pstmt->rowCount = -1;
        
        if (rc != SQL_SUCCESS) 
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
            DBPRINTF(DBI_TRC_STAT)
                ( "%d = IIDBIExecute (%d) %s %s %x\n    %s\n",
                rc, __LINE__, pstmt->hdr.err.sqlState, 
                pstmt->hdr.err.messageText, pstmt->hdr.err.native, stmnt );
            break;
        }
        
        break; 

    } /* end for (;;) */

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorExecute }}}1\n", pstmt);

    return( return_code );
}

/*{
** Name: dbi_cursorFetchone - Fetch one row from a statement
**
** Description:
**     Fetch one (remaining) row returned from a statement
**
** Inputs:
**     None.
**
** Outputs:
**     Returns the tuple
**     Returns:
** 	   ODBC error status
**
**     Exceptions:
**         An error is raised if the previous call to executeXXX() did
**         not produce any result set or no call was issued yet.
**
** Side Effects:
**
** History:
**     08-Jul-2004 (peeje01)
**         Created.
**     12-Jul-2004 (loera01@ca.com)
**         Implemented IIDBI_DESCRIPTOR.
**     17-Jul-2004 (komve01@ca.com)
**         Fixed the float and real data type interpretation.
**     17-Jul-2004 (raymond.fan@ca.com)
**         Change C type for LONGVARBINARY.
**     19-Jul-2004 (loera01@ca.com)
**         Added puts of binary blob data. 
**     19-Jul-2004 (raymond.fan@ca.com)
**          Add tracking of length to binary objects and report total length
**          in descriptor field.
**     20-Jul-2004 (loera01@ca.com)
**         Fixed puts of binary blob data. 
**      21-Jul-2004 (raymond.fan@ca.com)
**          Modified the alogorithm for selecting blobs greater than segment
**          size.  Fetch direct into the memory buffer - remove copies.
**          Uses realloc call - this may be problematic when we want to move
**          to the CL.
**    21-Jul-2004 (Ralph.Loen@ca.com)
**          When binding blobs, do not invoke SQL_DATA_LEN_AT_EXEC macro if 
**          the blob has zero length.
**    23-Jul-2004 (Ralph.Loen@ca.com)
**    19-Oct-2004 (Ralph.Loen@ca.com)
**          Fetch long varchars into SQL_C_CHAR, not SQL_C_BINARY.
**    31-Jan-2005 (Ralph.Loen@ca.com)
**          Allow for terminating character in segments when fetching long 
**          varchars.
}*/
RETCODE
dbi_cursorFetchone( IIDBI_STMT *pstmt )
{
    RETCODE rc, return_code;
    HSTMT hstmt = pstmt->hdr.handle;
    int i, nType;
    SQLINTEGER orind=0;
#define SEGMENT_SIZE 1000000
    char *segment = NULL;
    int count;
    char* ptr;
    int newalloc = 0;
    int segment_size = 0;

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorFetchone {{{1\n", pstmt);
    rc = SQLFetch(hstmt);
    if (rc == SQL_NO_DATA)
    {
        pstmt->fetchDone = TRUE;
        return DBI_SQL_NO_DATA;
    }
    else
        pstmt->fetchDone = FALSE;

    if (SQL_SUCCEEDED(rc))
    {
        if (pstmt->rowCount == -1)
             pstmt->rowCount = 1;
        else
             pstmt->rowCount++;

        for (i = 0; i < pstmt->descCount; i++)
        {
            switch (pstmt->descriptor[i]->type)
            {
            case SQL_WLONGVARCHAR:
                /*
                ** Initialization of descriptor and temporaries
                */
                count = 0;
                pstmt->descriptor[i]->data = NULL;
                pstmt->descriptor[i]->isNull = 0;
                pstmt->descriptor[i]->precision = 0;
                pstmt->descriptor[i]->orInd = 0;
                if (pstmt->outputColumnIndex && (i+1) == 
                    pstmt->outputColumnIndex)
                    segment_size = pstmt->outputSegmentSize;
                else if (pstmt->outputColumnIndex && (i+1) != 
                    pstmt->outputColumnIndex)
                    segment_size = SEGMENT_SIZE;
                else if (pstmt->outputSegmentSize)
                    segment_size = pstmt->outputSegmentSize;
                else
                    segment_size = SEGMENT_SIZE;
                segment = malloc( segment_size + 1);
                ptr = segment; 
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_WCHAR, 
                    ptr, segment_size, &orind);
                if (orind == SQL_NULL_DATA)
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    pstmt->descriptor[i]->isNull = 1;
                    rc = SQL_SUCCESS;
                }
                else
                {
                    while(SQL_SUCCEEDED(rc))
                    {
                        /*
                        ** orind contains length of data left in the driver
                        ** Not sure that this is correct.
                        */
                        if (orind > segment_size)
                        {
                            /*
                            ** We have segment_size in the buffer
                            */
                            count += segment_size;                            
                            /*
                            ** Add another segment to the buffer
                            ** include adjustment for null terminator
                            ** for long varchar.
                            */
                            newalloc = count + segment_size + 1;
                            segment = realloc( segment, newalloc );
                            ptr = segment + count;
                            rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_CHAR, 
                                ptr, segment_size, &orind);
                        }
                        else
                        {
                            count += orind;
                            break;
                        }
                    }
                    if (SQL_SUCCEEDED(rc))
                    {
                        pstmt->descriptor[i]->data = segment;
                        pstmt->descriptor[i]->precision = count;
                        pstmt->descriptor[i]->orInd = count;
                    }
                }
                if (!SQL_SUCCEEDED(rc))
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                }
                break;


            case SQL_LONGVARCHAR:
                /*
                ** Initialization of descriptor and temporaries
                */
                count = 0;
                pstmt->descriptor[i]->data = NULL;
                pstmt->descriptor[i]->isNull = 0;
                pstmt->descriptor[i]->precision = 0;
                pstmt->descriptor[i]->orInd = 0;
                if (pstmt->outputColumnIndex && (i+1) == 
                    pstmt->outputColumnIndex)
                    segment_size = pstmt->outputSegmentSize;
                else if (pstmt->outputColumnIndex && (i+1) != 
                    pstmt->outputColumnIndex)
                    segment_size = SEGMENT_SIZE;
                else if (pstmt->outputSegmentSize)
                    segment_size = pstmt->outputSegmentSize;
                else
                    segment_size = SEGMENT_SIZE;
                segment = malloc( segment_size + 1);
                ptr = segment; 
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_CHAR, 
                    ptr, segment_size, &orind);
                if (orind == SQL_NULL_DATA)
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    pstmt->descriptor[i]->isNull = 1;
                    rc = SQL_SUCCESS;
                }
                else if (rc == SQL_SUCCESS)
                {
                    pstmt->descriptor[i]->data = segment;
                    pstmt->descriptor[i]->precision = orind;
                    pstmt->descriptor[i]->orInd = orind;
                }
                else
                {
                    while(SQL_SUCCEEDED(rc))
                    {
                        /*
                        ** Orind contains length of data left in the driver.
                        */
                        if (orind >= segment_size)
                        {
                            /*
                            ** We have segment_size in the buffer
                            */
                            count += segment_size - 1;                            
                            /*
                            ** Add another segment to the buffer
                            ** include adjustment for null terminator
                            ** for long varchar.
                            */
                            newalloc = count + segment_size;
                            segment = realloc( segment, newalloc );
                            ptr = segment + count;
                            rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_CHAR, 
                                ptr, segment_size, &orind);
                        }
                        else
                        {
                            count += orind;
                            break;
                        }
                    }
                    if (SQL_SUCCEEDED(rc))
                    {
                        pstmt->descriptor[i]->data = segment;
                        pstmt->descriptor[i]->precision = count;
                        pstmt->descriptor[i]->orInd = count;
                    }
                }
                if (!SQL_SUCCEEDED(rc))
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                }
                break;

            case SQL_LONGVARBINARY:
                /*
                ** Initialization of descriptor and temporaries
                */
                count = 0;
                pstmt->descriptor[i]->data = NULL;
                pstmt->descriptor[i]->isNull = 0;
                pstmt->descriptor[i]->precision = 0;
                pstmt->descriptor[i]->orInd = 0;
                if (pstmt->outputColumnIndex && (i+1) == 
                    pstmt->outputColumnIndex)
                    segment_size = pstmt->outputSegmentSize;
                else if (pstmt->outputColumnIndex && (i+1) != 
                    pstmt->outputColumnIndex)
                    segment_size = SEGMENT_SIZE;
                else if (pstmt->outputSegmentSize)
                    segment_size = pstmt->outputSegmentSize;
                else
                    segment_size = SEGMENT_SIZE;
                segment = malloc( segment_size + 1);
                ptr = segment; 
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_BINARY, 
                    ptr, segment_size, &orind);
                if (orind == SQL_NULL_DATA)
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    pstmt->descriptor[i]->isNull = 1;
                    rc = SQL_SUCCESS;
                }
                else
                {
                    while(SQL_SUCCEEDED(rc))
                    {
                        /*
                        ** orind contains length of data left in the driver
                        ** Not sure that this is correct.
                        */
                        if (orind > segment_size)
                        {
                            /*
                            ** We have segment_size in the buffer
                            */
                            count += segment_size;                            
                            /*
                            ** Add another segment to the buffer
                            ** include adjustment for null terminator
                            ** for long varchar.
                            */
                            newalloc = count + segment_size + 1;
                            segment = realloc( segment, newalloc );
                            ptr = segment + count;
                            rc = SQLGetData(pstmt->hdr.handle, i+1,
                                SQL_C_BINARY, ptr, segment_size, &orind);
                        }
                        else
                        {
                            count += orind;
                            break;
                        }
                    }
                    if (SQL_SUCCEEDED(rc))
                    {
                        pstmt->descriptor[i]->data = segment;
                        pstmt->descriptor[i]->precision = count;
                        pstmt->descriptor[i]->orInd = count;
                    }
                }
                if (!SQL_SUCCEEDED(rc))
                {
                    if (segment != NULL)
                    {
                        free( segment );
                    }
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                }
                break;

            case SQL_FLOAT:
            case SQL_REAL:
            case SQL_DOUBLE:
                nType = pstmt->descriptor[i]->type;
           
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_DOUBLE,
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                        DBPRINTF(DBI_TRC_STAT)
                        ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                            rc, __LINE__, pstmt->hdr.err.sqlState, 
                            pstmt->hdr.err.messageText, 
                            pstmt->hdr.err.native);
                    if (segment)
                    {
                        free(segment);
                        segment = 0;
                    }
                    return return_code;
                }
                break;

            case SQL_BIGINT:
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_SBIGINT, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                            pstmt->hdr.err.native);
                    return return_code;
                }
                break;


            case SQL_INTEGER:
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_LONG, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                            pstmt->hdr.err.native);
                    return return_code;
                }
                break;

            case SQL_TINYINT:
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_TINYINT, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
                    return return_code;
                }
                break;

            case SQL_BIT:
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_BIT, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
                    return return_code;
                }
                break;

            case SQL_SMALLINT:
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_SHORT, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
                    return return_code;
                }
                break;

            case SQL_TYPE_DATE:            
            case SQL_TYPE_TIME:            
            case SQL_TYPE_TIMESTAMP:            
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_TIMESTAMP, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
                    return return_code;
                }
                break;

            case SQL_DECIMAL: 
            case SQL_WVARCHAR:
            case SQL_WCHAR:
            case SQL_CHAR:
            case SQL_VARCHAR:
                /* Start of generic get code */
                rc = SQLGetData(pstmt->hdr.handle, i+1, 
                                     pstmt->descriptor[i]->cType,
                                     pstmt->descriptor[i]->data, 
                                     pstmt->descriptor[i]->internalSize,
                                     &orind); 
                if (orind == SQL_NULL_DATA) 
                    pstmt->descriptor[i]->isNull = 1; 
                else 
                    pstmt->descriptor[i]->isNull = 0; 
                pstmt->descriptor[i]->orInd = orind; 
                if (rc == SQL_NO_DATA) 
                    pstmt->fetchDone = TRUE; 
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA) 
                { 
                    rc = DBI_SQL_SUCCESS; 
                    break; 
                } 
                else 
                { 
                    return_code = IIDBI_ERROR( rc, NULL, NULL,  
                        hstmt, &pstmt->hdr.err ); 
                    DBPRINTF(DBI_TRC_STAT) 
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n", 
                        rc, __LINE__, pstmt->hdr.err.sqlState,  
                        pstmt->hdr.err.messageText,  
                        pstmt->hdr.err.native); 
                    return return_code; 
                } 
                break; 
 

            default:            
                rc = SQLGetData(pstmt->hdr.handle, i+1, SQL_C_CHAR, 
                                     pstmt->descriptor[i]->data,
                                     pstmt->descriptor[i]->precision+1,
                                     &orind);
                if (orind == SQL_NULL_DATA)
                    pstmt->descriptor[i]->isNull = 1;
                else
                    pstmt->descriptor[i]->isNull = 0;
                if (rc == SQL_NO_DATA)
                    pstmt->fetchDone = TRUE;
                if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
                {
                    rc = DBI_SQL_SUCCESS;
                    break;
                }
                else
                {
                    return_code = IIDBI_ERROR( rc, NULL, NULL, 
                        hstmt, &pstmt->hdr.err );
                    DBPRINTF(DBI_TRC_STAT)
                    ( "%d = dbi_cursorFetchone (%d) %s %s %x\n\n",
                        rc, __LINE__, pstmt->hdr.err.sqlState, 
                        pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
                    return return_code;
                }
                break;
            }
        }
    }
    else
    {
        return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
        DBPRINTF(DBI_TRC_STAT) ( "%p: %d = dbi_cursorFetchone (%d) %s %s %x\n\n", pstmt,
            rc, __LINE__, pstmt->hdr.err.sqlState, pstmt->hdr.err.messageText, 
            pstmt->hdr.err.native);
        return return_code;
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_cursorFetchone }}}1\n", pstmt);
    return(DBI_SQL_SUCCESS);
}

/*{
** Name: dbi_getDescriptor - Get column descriptor info
**
** Description:
**     Get descriptor information from the tuple descriptor array.
**
** Inputs:
**     None.
**
** Outputs:
**     Returns:
** 	   Pointer to IIDBI_DESCRIPTOR.
**
**     Exceptions:
**         An error is raised if the previous call to executeXXX() did
**         not produce any result set or no call was issued yet.
**
** Side Effects:
**
** History:
**     13-Jul-2004 (loera01)
**         Created.
}*/

IIDBI_DESCRIPTOR *dbi_getDescriptor ( IIDBI_STMT *pstmt, int colNbr, 
    unsigned char isParam )
{
    DBPRINTF(DBI_TRC_STAT)("Getting the descriptor from %p\n",pstmt->descriptor);
    if (!pstmt)
        return 0;

    if (isParam)
    {
        if (!pstmt->parameter )
            return 0;
        return pstmt->parameter[colNbr];
    }
     
    if (!pstmt->descriptor )
        return 0;
    return pstmt->descriptor[colNbr];
}

/*{
** Name: dbi_freeDescriptor - Free a tuple descriptor
**
** Description:
**     Frees all memory associated with a tuple descriptor.
**
** Inputs:
**     pstmt - pointer to DBI statement structure.
**
** Outputs:
**     Returns:
** 	   All memory for tuple descriptor structure array is freed.
**
**     Exceptions:
**         An error is raised if memory problems.
**
** Side Effects:
**
** History:
**     15-Jul-2004 (loera01@ca.com)
**         Created.
**     17-Jul-2004 (komve01@ca.com)
**         Fixed dbi_freeDescriptor. Clean parameter if isParam is one.
**         else clean descriptor block.
}*/

RETCODE dbi_freeDescriptor(IIDBI_STMT *pstmt, unsigned char isParam)
{
    int i;

    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_freeDescriptor isParam[%u]{{{1\n", pstmt, isParam);

    if (!pstmt)
        return DBI_INTERNAL_ERROR;

    if (isParam)
    {
        if (!pstmt->parmCount)
            return DBI_SQL_SUCCESS;
    
        if (!pstmt->parameter)
        {
            pstmt->parmCount = 0;
            pstmt->rowCount = -1;
            return DBI_INTERNAL_ERROR;
        }
        for (i = 0; i < pstmt->parmCount; i++) 
        {
            /*
            ** Since the data for long data types was retrieved from 
            ** PyBufferFromObject(), we rely on Python to free the
            ** memory for the data.  Same for ASCII and Unicode.
            */
            if (pstmt->parameter[i]->data)
            {
                switch (pstmt->parameter[i]->type)
                {
                    case SQL_LONGVARCHAR:
                    case SQL_LONGVARBINARY:
                    case SQL_WLONGVARCHAR:
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_WVARCHAR:
                    case SQL_WCHAR:
                        break;

                    default:
                        free (pstmt->parameter[i]->data);  
                        break;
                }
            }
            free (pstmt->parameter[i]);             
        }
        free(pstmt->parameter);                        
        pstmt->parameter = 0;
        pstmt->parmCount = 0;
        pstmt->rowCount = -1;
    }
    else
    {
        dbi_freeData(pstmt);
        for (i = 0; i < pstmt->descCount; i++) 
        {
			DBPRINTF(DBI_TRC_STAT)("Freeing descriptor[%i]\n", i);
	    free(pstmt->descriptor[i]->columnName);
            free (pstmt->descriptor[i]);
	}
        free(pstmt->descriptor);                        
        pstmt->descriptor = 0;
        pstmt->descCount = 0;
        pstmt->rowCount = -1;
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_freeDescriptor }}}1\n", pstmt);
    return DBI_SQL_SUCCESS;
}

/*{
** Name: dbi_allocDescriptor - Allocate tuple descriptor
**
** Description:
**     Allocate tuple descriptor.
**
** Inputs:
**     pstmt - pointer to DBI statement structure.
**
** Outputs:
**     Returns:
** 	   pstmt has allocated data for the tuple descriptor.
**
**     Exceptions:
**         An error is raised if memory problems.
**
** Side Effects:
**
** History:
**     15-Jul-2004 (loera01@ca.com)
**         Created.
}*/

RETCODE 
dbi_allocDescriptor(IIDBI_STMT *pstmt, int numCols, unsigned char isParam )
{
     int i;
     int return_code = DBI_SQL_SUCCESS;

     DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_allocDescriptor numCols[%i] isParam[%u] {{{1\n", pstmt, numCols, isParam);

     if (!pstmt)
         return DBI_INTERNAL_ERROR;

     if (!numCols)
         return DBI_SQL_SUCCESS;

     if (isParam)
     {
         pstmt->parmCount = numCols;
    
         pstmt->parameter = calloc(1,sizeof(IIDBI_DESCRIPTOR *) * numCols);
         if (!pstmt->parameter)
         {
             pstmt->parameter = 0;
             return DBI_INTERNAL_ERROR;
         }
         for (i = 0; i < pstmt->parmCount; i++)
         {
             pstmt->parameter[i] = calloc(1,sizeof(IIDBI_DESCRIPTOR));
             if (!pstmt->parameter[i])
             {
                 int k;
                 for (k=0; k <= i; k++)
                 {
                     if (pstmt->parameter[k])
                         free (pstmt->parameter[k]);  
                 }
                 free(pstmt->parameter);                        
                 return_code = DBI_INTERNAL_ERROR;
                 break;
             }
         }
     } 
     else
     {
        pstmt->descCount = numCols;
        pstmt->rowCount = -1;
   
        pstmt->descriptor = calloc(1,sizeof(IIDBI_DESCRIPTOR *) * numCols);
        if (!pstmt->descriptor)
        {
            pstmt->descCount = 0;
            return DBI_INTERNAL_ERROR;
        }
        for (i = 0; i < pstmt->descCount; i++)
        {
            pstmt->descriptor[i] = calloc(1,sizeof(IIDBI_DESCRIPTOR));
            if (!pstmt->descriptor[i])
            {
                int k;
                for (k=0; k <= i; k++)
                {
                    if (pstmt->descriptor[k])
                        free (pstmt->descriptor[k]);  
                }
                free(pstmt->descriptor);                        
                return_code = DBI_INTERNAL_ERROR;
                break;
            }
        }
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_allocDescriptor }}}1\n", pstmt);
    return return_code;
}

/*{
** Name: dbi_allocData - Allocate data in tuple descriptor
**
** Description:
**     Allocate data in tuple descriptor.
**
** Inputs:
**     pstmt - pointer to DBI statement structure.
**
** Outputs:
**     Returns:
** 	   pstmt has allocated data for non-blob columns.
**
**     Exceptions:
**         An error is raised if memory problems.
**
** Side Effects:
**
** History:
**     15-Jul-2004 (loera01@ca.com)
**         Created.
**      16-Jul-2004 (raymond.fan@ca.com)
**          Use precision value for sizeof data.
}*/
RETCODE
dbi_allocData ( IIDBI_STMT *pstmt)
{  
     int i;
     int return_code = DBI_SQL_SUCCESS;
     int dataLen;

     DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_allocData {{{1\n", pstmt);

     if (!pstmt)
         return DBI_INTERNAL_ERROR;

         if (!pstmt->descCount)
             return DBI_SQL_SUCCESS;
    
         for (i = 0; i < pstmt->descCount; i++)
         {
	     dataLen = pstmt->descriptor[i]->internalSize;
             switch (pstmt->descriptor[i]->type)
             {
             case SQL_LONGVARCHAR:
             case SQL_LONGVARBINARY:
             case SQL_WLONGVARCHAR:
                 pstmt->descriptor[i]->data = NULL;
                 break;
    
             default:
                 pstmt->descriptor[i]->data = 
                     calloc(1, dataLen);
                 if (!pstmt->descriptor[i]->data)
                 {
                     int k=i;
                     return_code = DBI_INTERNAL_ERROR;
                     for (i=0; i <= k; i++)
                     {
                         if (pstmt->descriptor[i]->data)
                             free (pstmt->descriptor[i]->data);  
                     }
                 }
                 break;
             }
         }
     DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_allocData }}}1\n", pstmt);
     return return_code;
}

/*{
** Name: dbi_freeData - Free data in tuple descriptor
**
** Description:
**     Free data in tuple descriptor.
**
** Inputs:
**     pstmt - pointer to DBI statement structure.
**
** Outputs:
**     Returns:
** 	   Pstmt has freed data, but not the descriptor count or
**         descriptor.
**
**     Exceptions:
**         An error is raised if memory problems.
**
** Side Effects:
**
** History:
**     07-Jan-2005 (loera01@ca.com)
**         Created.
}*/
RETCODE
dbi_freeData ( IIDBI_STMT *pstmt)
{  
     int i;

     DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_freeData {{{1\n", pstmt);

     if (!pstmt)
         return DBI_SQL_SUCCESS;

     if (!pstmt->descriptor)
         return DBI_SQL_SUCCESS;

     if (!pstmt->descCount)
         return DBI_SQL_SUCCESS;

     for (i = 0; i < pstmt->descCount; i++)
     {
         if (pstmt->descriptor[i]->data)
         {
             if (pstmt->descriptor[i]->data)
             {
                 free(pstmt->descriptor[i]->data);  
                 pstmt->descriptor[i]->data = 0;
	     }
         }
     }
     DBPRINTF(DBI_TRC_ENTRY)("%p: dbi_freeData }}}1\n", pstmt);
     return DBI_SQL_SUCCESS;
}


/*{
** Name: BindParameters - Associates parameter descriptors with a statement.
**
** Description:
**      Associates parameters with a statement by calling SQLBindParameter.
**      Parameters are either input or output depending on the statement.
**
** Inputs:
**     pstmt - pointer to DBI statement structure.
**
** Outputs:
**      None.
**
** Returns:
** 	    DBI_SQL_SUCCESS
**      DBI_SQL_ERROR
**      DBI_INTERNAL_ERROR
**
** Exceptions:
**      None.
**
** Side Effects:
**
** History:
**      16-Jul-2004 (raymond.fan@ca.com)
**          History.
**          Use precision value for sizeof data.
**      17-Jul-2004 (komve01@ca.com)
**          Added support for float,real and double type parameters
**      19-Jul-2004 (komve01@ca.com)
**          Added support for long type parameters
**      22-Jul-2004 (komve01@ca.com)
**          Fixed BIGINT conversion.
}*/
RETCODE
BindParameters(IIDBI_STMT *pstmt, unsigned char isProc)
{
    RETCODE rc;
    int return_code;
    int i, nType,type;
    IIDBI_DESCRIPTOR **pdesc;
    SQLINTEGER *orind=NULL;
    void *data;
    SQLUINTEGER precision;
    SQLSMALLINT scale;
    SQLSMALLINT cType;
    SQLLEN internalSize;
    unsigned char isNull;

    DBPRINTF(DBI_TRC_ENTRY)("%p: Begin BindParameters\n", pstmt);
    if (!pstmt)
        return DBI_INTERNAL_ERROR;

    if (!pstmt->parmCount)
        return DBI_SQL_SUCCESS;

    if (!pstmt->parameter)
        return DBI_INTERNAL_ERROR;

    pdesc = pstmt->parameter;

    for (i = 0; i < pstmt->parmCount; i++)
    {
        type = pstmt->parameter[i]->type;
        data = pstmt->parameter[i]->data;
        precision = pstmt->parameter[i]->precision;
        scale = pstmt->parameter[i]->scale;
        isNull = pstmt->parameter[i]->isNull;
        cType = pstmt->parameter[i]->cType;
        internalSize = pstmt->parameter[i]->internalSize;
        /*
        ** make orind persistent beyond this call by using the field
        ** in the parameter descriptor structure.
        */
        orind = (SQLINTEGER *)&pstmt->parameter[i]->orInd;
        if (isNull)
            *orind = SQL_NULL_DATA;

        DBPRINTF(DBI_TRC_STAT)("DATATYPE IS %d for parameter %d\n",
            pstmt->parameter[i]->type, i+1);
        switch (type)
        {
        case SQL_LONGVARBINARY:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else if (precision > 0)
                *orind = SQL_LEN_DATA_AT_EXEC((SQLINTEGER)precision);
            else 
                *orind = 0;
            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                SQL_C_BINARY, SQL_LONGVARBINARY, precision, scale, 
                    (SQLPOINTER)(i+1), 0, orind);
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );

                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break;
        case SQL_INTEGER:
        case SQL_SMALLINT:
        case SQL_TINYINT:
        case SQL_BIT:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;
            precision = 0;
            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                SQL_C_LONG, type, precision, scale, data, 0, orind);
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_BIGINT:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;
            precision = 0;
            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                SQL_C_SBIGINT, type, precision, scale, data, 0, orind);
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_DECIMAL:
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_TYPE_NULL:  /* special case typeless null ("None" data) */
            /* Start of generic BindParameters code */
/* TODO add trace code for data? */
            DBPRINTF(DBI_TRC_STAT)("BindParameter %d SQL_PARAM_INPUT cType %d, type %d, precision %d, scale %d, internalSize %d, orind %d\n", i+1, cType, type, precision, scale, internalSize, *orind);
            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                cType, type, precision, scale, data, internalSize, orind);

            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );

                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_FLOAT:
        case SQL_REAL:
        case SQL_DOUBLE:

            DBPRINTF(DBI_TRC_STAT)("float double or real ...\n");

            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;

            nType = type;
            switch(nType)
            {
               case SQL_DOUBLE:
                    nType = SQL_C_DOUBLE;
                    break;
               case SQL_REAL:
                    nType = SQL_C_FLOAT;
                    break;
               default:
                    nType = SQL_C_FLOAT;
                    break;
            }
            DBPRINTF(DBI_TRC_STAT)("Precision is %d with data %g and C type %d\n", 
                precision, *(double *)data, nType);

            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                nType, SQL_DOUBLE, precision, 0,data, 0, orind);

            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );

                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_TYPE_TIME:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;

            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                 SQL_C_TYPE_TIME, SQL_TYPE_TIME, precision, 0, data, 
                 0, orind);
      
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_TYPE_DATE:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;

            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                 SQL_C_TYPE_DATE, SQL_TYPE_DATE, precision, 0, data, 
                 0, orind);
      
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        case SQL_TYPE_TIMESTAMP:
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = 0;

            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                 SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, precision, 0, data, 
                 0, orind);
      
            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 

        default:
            DBPRINTF(DBI_TRC_STAT)("default (coerce to SQL_CHAR)\n");
            if (isNull)
                *orind = SQL_NULL_DATA;
            else
                *orind = SQL_NTS; /* FIXME consider using string size (i.e. PyString_Size), see SQL_VARCHAR above */

            DBPRINTF(DBI_TRC_STAT)("Precision is %d with data %s\n",precision, data);

            rc = SQLBindParameter(pstmt->hdr.handle, i+1, SQL_PARAM_INPUT, 
                type, SQL_C_CHAR, precision, scale, (char *)data, precision+1, orind);

            if (rc != SQL_SUCCESS) 
            {
                return_code = IIDBI_ERROR( rc, NULL, NULL, pstmt->hdr.handle, 
                    &pstmt->hdr.err );

                DBPRINTF(DBI_TRC_STAT)
                    ( "%d = SQLBindParameter (%d) %s %s %x\n    %s\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, pstmt->hdr.err.native, 0 );
                return return_code;
            }
            break; 
        }
    }
    DBPRINTF(DBI_TRC_ENTRY)("%p: End BindParameters\n", pstmt);
    return DBI_SQL_SUCCESS;
}

RETCODE dbi_describeColumns (IIDBI_STMT *pstmt)
{
    RETCODE rc = DBI_SQL_SUCCESS;
    int return_code;
    int i, numCols;
    HSTMT hstmt = pstmt->hdr.handle;
    SQLCHAR colName[257];
    SQLSMALLINT type;
    SQLSMALLINT scale, nullable, cbColName;
    SQLINTEGER internalSize;
    SQLINTEGER displaySize; 
    SQLUINTEGER precision;  
    int cType;

    numCols = pstmt->descCount;

    for (i = 0; i < numCols; i++)
    {
        cType=SQL_C_CHAR; /* Default */
        rc = SQLDescribeCol(hstmt, i+1, colName, 256, &cbColName, &type, 
            &precision, &scale, &nullable);
        if (!SQL_SUCCEEDED(rc))
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                ( "%d = dbi_describeColumns (%d) %s %s %x\n\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
            return return_code;
        }
        pstmt->descriptor[i]->type = type;
        pstmt->descriptor[i]->precision = precision;
        /*
        ** Precision overrides for special data types.
        */
        switch (pstmt->descriptor[i]->type)
        {
        case SQL_TIMESTAMP:
        case SQL_DATE:
        case SQL_TIME:
            pstmt->descriptor[i]->precision = 26;
            cType=SQL_C_TIMESTAMP;
            break;

        case SQL_LONGVARCHAR:
        case SQL_LONGVARBINARY:
        case SQL_WLONGVARCHAR:
            precision = 0;
            pstmt->descriptor[i]->precision = precision;
            break;
        case SQL_WVARCHAR:
        case SQL_WCHAR:
            cType=SQL_C_WCHAR;
            break;
        }
        rc = SQLColAttribute(hstmt, i+1, SQL_DESC_DISPLAY_SIZE, 0, 0, NULL, 
            &displaySize);
        if (!SQL_SUCCEEDED(rc))
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                ( "%d = dbi_describeColumns (%d) %s %s %x\n\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
            return return_code;
        }
        if (displaySize == MAX_DISPLAY_SIZE) /* if set at max, default to -1 */
            displaySize = -1;
        switch (pstmt->descriptor[i]->type)
        {
            case SQL_NUMERIC:
            case SQL_DECIMAL:
                internalSize = MAX_STRING_LEN_FOR_DECIMAL_PRECISION(precision);
                cType = SQL_C_CHAR;
                break;

            default:
        rc = SQLColAttribute(hstmt, i+1, SQL_DESC_OCTET_LENGTH, 0, 0, NULL, 
            &internalSize);
        if (!SQL_SUCCEEDED(rc))
        {
            return_code = IIDBI_ERROR( rc, NULL, NULL, hstmt, &pstmt->hdr.err );
                DBPRINTF(DBI_TRC_STAT)
                ( "%d = dbi_describeColumns (%d) %s %s %x\n\n",
                    rc, __LINE__, pstmt->hdr.err.sqlState, 
                    pstmt->hdr.err.messageText, 
                        pstmt->hdr.err.native);
            return return_code;
        }
                switch (pstmt->descriptor[i]->type)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                        /* precision + 1 */
                        internalSize+=1; /* SQLGetData null terminates strings, so need space for it */
                        break;
                    case SQL_WVARCHAR:
                    case SQL_WCHAR:
                        /* (precision + 1) * sizeof(SQLWCHAR) */
                        internalSize+=sizeof(SQLWCHAR); /* SQLGetData null terminates strings, so need space for it */
                        break;
                }
                
                break;
        }
        DBPRINTF(DBI_TRC_STAT)("For Col %d, type is %d (cType is %d), name is %s, precision is %d, scale is %d display size is %d internal size is %d and nullable is %d\n", i, type, cType, colName, precision, scale, displaySize, internalSize, nullable); 
         pstmt->descriptor[i]->scale = scale;
         pstmt->descriptor[i]->nullable = nullable;
         pstmt->descriptor[i]->columnName = calloc(1,cbColName + 1);
         strcpy(pstmt->descriptor[i]->columnName, (char *)colName);
         pstmt->descriptor[i]->displaySize = displaySize;
         pstmt->descriptor[i]->internalSize = internalSize;
         pstmt->descriptor[i]->cType = cType;
     }
     return DBI_SQL_SUCCESS;
}
