import os
import time
import mercurial.ui
import mercurial.hg
import mercurial.commands
import difflib
import shutil

from Chandra.Time import DateTime
import tempfile

TM = 'iFOT_time_machine'


def time_machine_get_file(date, file='load_segment.rdb'):
    """
    Get the time machine file `file` at `date`.

    """
    # use the arc "time machine"
    repo_dir = '/proj/sot/ska/data/arc/{}/'.format(TM)
    hg_time_str = "<{}".format(
        time.strftime("%c", time.gmtime(DateTime(date).unix)))
    hg_ui = mercurial.ui.ui()
    repo = mercurial.hg.repository(hg_ui, repo_dir)
    temp = tempfile.NamedTemporaryFile(delete=False)
    mercurial.commands.cat(hg_ui, repo,
                           os.path.join(repo_dir, file),
                           output=temp.name,
                           rev="date('{}')".format(hg_time_str))
    return temp.name


def find_changes(start=None, stop=None):
    """
    Generate an html diff for the load_segment iFOT files
    from `start` and `stop`.  Defaults to returning
    diff from file 7 days ago to most recent file.

    :start: input Chandra.Time compatible start date
    :stop: input Chandra.Time compatible stop date
    :returns: dictionary that includes difflib.HtmlDiff
              table (string) of diffs in 'htmldiff' key
    """

    if start == None:
        start = DateTime() - 7
    if stop == None:
        stop = DateTime()
    file1 = time_machine_get_file(start)
    file2 = time_machine_get_file(stop)
    html_diff = difflib.HtmlDiff()
    out = html_diff.make_table(open(file1).readlines()[2:],
                               open(file2).readlines()[2:],
                               context=True)
    os.remove(file1)
    os.remove(file2)
    return {'start': start,
            'stop': stop,
            'htmldiff': out}


def main(start=None, stop=None):
    out = find_changes(start=start, stop=stop)
    print out['htmldiff']


if __name__ == '__main__':
    main()


