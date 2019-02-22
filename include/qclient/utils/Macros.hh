// ----------------------------------------------------------------------
// File: Macros.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * qclient - A simple redis C++ client with support for redirects       *
 * Copyright (C) 2016 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef QCLIENT_UTILS_MACROS_HH
#define QCLIENT_UTILS_MACROS_HH

#define qclient_assert(condition) if(!((condition))) std::cerr << "assertion violation, condition is not true: " << #condition << ". Location: " << __FILE__ << ":" << __LINE__

#ifdef QCLIENT_IS_UNDER_TEST
#define PUBLIC_FOR_TESTS_ONLY public
#else
#define PUBLIC_FOR_TESTS_ONLY private
#endif


#endif
