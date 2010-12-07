#!/usr/bin/env python
"""
 Copyright (c) 2010 Ingres Corporation. All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 as
 published by the Free Software Foundation.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
try:
    import pysvn
except ImportError:
    print "missing Python SVN module, pysvn"
import shlex, subprocess
import getopt
import codecs
import sys
import os.path


"""
 History

    3-Dec-2010 - Initial creation (grant.croker@ingres.com)
    7-Dec-2010 - Add getopt support to allow for parameter
                 passing

"""

def usage():
    sys.stderr.write(\
"""Usage:
%s [OPTIONS]
For a given repository create a CHANGELOG for each tag contained within the tag
directory

Options:
  --svn2log-dir=DIR            Location of the svn2log scripts. svn2log is
                               available from http://www.core.com.pl/svn2log/
                               defaults to '/usr/local/bin'
  --repository=SVN_REPOSITORY  URL of the SVN repositorya with the trailing '/'.
                               Defaults to 'http://code.ingres.com/ingres/'
  --trunk=TRUNK                Location relative to the repository where the
                               main/trunk code is kept. Defaults to 'main/'
  --tags=TAGS                  Location relative to the repository where the
                               tags are kept. Defaults to 'tags/'
  --include-head               Include changes from the last tag and 
                               HEAD revision
  --output=CHANGELOG           Set the output file name. Defaults to CHANGELOG.
                               Use '-' to sent the output to STDOUT
  --quiet                      Don't output messages about the current tag being
                               processed
""" % (sys.argv[0]))

def process_args():
    include_head = False
    repo_parent = "http://code.ingres.com/ingres/"
    trunk = "main/"
    tags = "tags/"
    output_file = "CHANGELOG"
    quiet = False
    svn2log_dir='/usr/local/bin'

    try:
        opts,args = getopt.gnu_getopt(sys.argv[1:],"",
            ["svn2log-dir=","output=","repository=","trunk=","tags=","include-head","quiet"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--repository"):
            repo_parent = arg
        elif opt in ("--trunk"):
            trunk = arg
        elif opt in ("--tags"):
            tags = arg
        elif opt in ("--include-head"):
            include_head = True
        elif opt in ("--svn2log-dir"):
            svn2log_dir=arg
        elif opt in ("--output"):
            if arg == "-":
                output_file = sys.stdout
            else:
                output_file = codecs.open(arg, "w+", encoding="utf-8", errors="replace")
        elif opt in ("--quiet"):
            quiet = True
        else:
            usage()
            sys.exit(2)
    if os.path.isdir(svn2log_dir) == False:
        print "%s is not a valid directory" % ( svn2log_dir )
        sys.exit(3)
    else:
        if os.path.isfile(svn2log_dir+'/svn2log.py') == False:
            print 'Error : %s/svn2log.py does not exist' % ( svn2log_dir )
            sys.exit(3)

    generate_changelog(repo_parent, trunk, tags, include_head, output_file, quiet, svn2log_dir)


"""
Fetch a list of tags
"""
def get_tags(repository,include_head=False):
    svn_client = pysvn.Client()
    dirlist = svn_client.list(repository,recurse=False)
    tags=[]
    for tuple in dirlist:
        for dirent in tuple:
            if dirent:
                if dirent['kind'] == pysvn.node_kind.dir:
                    path=dirent["repos_path"].split("/")
                    tag=path[len(path)-1]
                    rev=dirent["created_rev"].number
                    tags.append([tag,rev])
    #Remove the parent directory from the list of tags
    tags.pop(0)
    #Order by reverse revision number
    tags = sorted(tags, key=lambda tag: tag[1], reverse=True)
    if include_head:
        tags.insert(0,['SVN','HEAD'])
    return tags

"""
Iterate over each tag generating the CHANGELOG for each tag
"""
# Remove the first entry since it contains the parent directory

def generate_tag_changelist(tag,start_rev,end_rev,output_file,repository,svn2log_dir):
    output_file.write(tag + "\n")
    if end_rev == 'HEAD':
        svn_log_cmd = 'svn log -r %d:%s -v --xml %s' % (start_rev, end_rev, repository) 
    else:
        svn_log_cmd = 'svn log -r %d:%d -v --xml %s' % (start_rev, end_rev, repository)

    svn2log_cmd='python %s/svn2log.py --first-line-only  --no-files  -o - ' % (svn2log_dir)
    grep_date_cmd='grep -v "^20[0-1]"'
    grep_empty_lines='grep -v "^$"'
    sed_leading_spaces="sed 's/^\t//'"

    svnlog=subprocess.Popen(shlex.split(svn_log_cmd),stdout=subprocess.PIPE)
    svn2log=subprocess.Popen(shlex.split(svn2log_cmd),stdin=svnlog.stdout,stdout=subprocess.PIPE)
    grep1=subprocess.Popen(shlex.split(grep_date_cmd),stdin=svn2log.stdout,stdout=subprocess.PIPE)
    grep2=subprocess.Popen(shlex.split(grep_empty_lines),stdin=grep1.stdout,stdout=subprocess.PIPE)
    sed=subprocess.Popen(shlex.split(sed_leading_spaces),stdin=grep2.stdout,stdout=subprocess.PIPE)
    changelog=sed.communicate()[0]
    output_file.write(changelog)
    output_file.write("\n")

def generate_changelog(repo_parent, trunk, tags, include_head, output_file, quiet, svn2log_dir):
    repo_tags = repo_parent + tags
    repo_trunk = repo_parent + trunk

    tags=get_tags(repo_tags,include_head)
    tag_count=len(tags)

    tag_no = 0
    for tag,rev in tags:
        tag_no = tag_no + 1
        if tag_no == tag_count:
            start_rev=0
        else:
            start_rev= tags[tag_no][1]
        end_rev=rev
        if quiet == False:
            if end_rev == 'HEAD':
                print "Generating the CHANGELOG for %s (revisions %d:%s)" % (tag, start_rev, end_rev)
            else:
                print "Generating the CHANGELOG for %s (revisions %d:%d)" % (tag, start_rev, end_rev)
        generate_tag_changelist(tag,start_rev,end_rev,output_file,repo_trunk,svn2log_dir)
    output_file.close()

if __name__ == "__main__":
    process_args()
