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
** Name: iidbiccurs.h - DBI C interface for cursor methods.
**
** Description:
**
** History:
**      09-Jul-2004 (raymond.fan@ca.com)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Add prototype for dbi_cursorExecute and dbi_cursorClose.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Change prototypes to take a pointer (self) from python.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Made functions consistently return integers.
**      14-Jul-2004 (raymond.fan@ca.com)
**          Modified prototype to include pstmt.
**      13-dec-2004 (Ralph.Loen@ca.com)
**          Dbi_mapType() now returns an integer rather than a built-in
**          Python type.
**      07-Jan-2004 (Ralph.Loen@ca.com)
**          Added dbi_freeData().
**      05-Aug-2009 (Chris.Clark@ingres.com)
**          Removed unused dbi_cursorFetchall()
**/

#ifndef __IIDBI_CURS_H_INCLUDED
#define __IIDBI_CURS_H_INCLUDED

/*
** DBI C prototypes
*/
extern RETCODE
dbi_cursorClose( IIDBI_STMT* pstmt );

extern RETCODE
dbi_cursorExecute( IIDBI_DBC* pdbc, IIDBI_STMT* pstmt, char *stmnt, 
    unsigned char isProc );

extern RETCODE
dbi_allocDescriptor ( IIDBI_STMT *pstmt, int nbrCols, unsigned char isParam );

extern RETCODE
dbi_allocData ( IIDBI_STMT *pstmt );

extern RETCODE
dbi_freeData ( IIDBI_STMT *pstmt );

extern RETCODE 
dbi_freeDescriptor(IIDBI_STMT *pstmt, unsigned char isParam);

extern RETCODE
dbi_cursorFetchone( IIDBI_STMT *pstmt );

extern RETCODE 
dbi_describeColumns (IIDBI_STMT *pstmt);

extern int  
dbi_mapType(int type);

# endif     /* __IIDBI_CURS_H_INCLUDED */
