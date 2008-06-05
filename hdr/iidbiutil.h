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
/**
** Name: iidbiutil.h - Ingres/Python connection definitions
**
** Description:
**      Module that contains the interface specific definitions between
**      for interfacing between Python and C and vice versa.
**
**      Implements DBAPI 2.0.
**
** History:
**      10-Jul-2004 (raymond.fan@ca.com)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add global trace flag and trace file descriptor.
**      10-Jul-2004 (jeremy.peel@ca.com)
**          Only include stdlib.h if _MSC_VER defined
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add prototype for print_err.
**      11-Jul-2004 (raymond.fan@ca.com)
**          Add internal trace print function.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Add function to initialize error structure.
**/

#ifndef __IIDBI_UTIL_H_INCLUDED
#define __IIDBI_UTIL_H_INCLUDED

# define DBPRINTF(X)    if (dbi_trclevel & X) (void)dbi_format
# define DBI_TRC_OFF    (int)0x0000
# define DBI_TRC_ENTRY  (int)0x0001
# define DBI_TRC_RET    (int)0x0002
# define DBI_TRC_STAT   (int)0x0004

/*
** Globals
*/
 int dbi_trclevel;
 FILE *dbi_dbgfd;

IIDBI_DBC *dbi_newdbc( void );

void
print_err(RETCODE rc, HENV henv, HDBC hdbc, SQLHSTMT hstmt);

short int
dbi_format( char* fmt, ... );

RETCODE
dbi_error_withtext( RETCODE status, HENV henv, HDBC hdbc, SQLHSTMT hstmt, IIDBI_ERROR* err, char *err_str );
#define IIDBI_ERROR( status, henv, hdbc, hstmt, err) dbi_error_withtext( status, henv, hdbc, hstmt, err, NULL )

short int
dbi_trace( int dbglevel, char* trcfile );

# endif     /* __IIDBI_UTIL_H_INCLUDED */
