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
** Name: iidbicconn.h - DBI C interface for connect methods.
**
** Description:
**
** History:
**      09-Jul-2004 (raymond.fan@ca.com)
**          Created.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Modified the prototype for dbi_connect.
**      10-Jul-2004 (raymond.fan@ca.com)
**          Change prototypes to take a pointer (self) from python.
**      12-Jul-2004 (raymond.fan@ca.com)
**          Made functions consistently return integers.
**      21-Dec-2004 (ralph.Loen@ca.com)
**          Added autocommit argument to dbi_connect.
**/

#ifndef __IIDBI_CONN_H_INCLUDED
#define __IIDBI_CONN_H_INCLUDED
/*
** DBI C interface prototypes
*/
extern RETCODE
dbi_alloc_env(IIDBI_ENV *penv, IIDBI_DBC *pdbc);

extern RETCODE
dbi_connect( IIDBI_DBC *pdbc);

extern RETCODE
dbi_connectionClose( IIDBI_DBC *pdbc );

extern RETCODE
dbi_connectionCommit( IIDBI_DBC *pdbc );

extern RETCODE
dbi_connectionCursor( IIDBI_DBC *pdbc );

extern RETCODE
dbi_connectionRollback( IIDBI_DBC *pdbc );

# endif     /* __IIDBI_CONN_H_INCLUDED */
